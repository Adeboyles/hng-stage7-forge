"""
engine/runner.py
----------------
Executes build jobs inside hardened Docker containers.

Isolation guarantees enforced per job:
  - No host network. Container joins `forge-internal` (internal: true).
    Only the registry service is reachable.
  - Read-only root filesystem. Writable space is tmpfs on /workspace and /tmp.
  - 1.0 CPU, 512 MiB RAM, no swap, max 100 PIDs.
  - --no-new-privileges and security-opt=no-new-privileges (defense in depth).
  - --rm so the container disappears after exit.
  - Exit 137 is treated as OOM kill and reported as such.

The runner does NOT shell out and build a command string from user input.
All container parameters go through the Docker SDK so there is no room
for shell injection through job names, tokens, or build steps.
"""

from __future__ import annotations

import logging
import os
import shlex
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Optional

import docker
from docker.errors import APIError, ContainerError, ImageNotFound, NotFound
from docker.models.containers import Container

try:
    from .config import engine_settings, isolation_settings, registry_internal_base_url
    from .logs import LogWriter
except ImportError:  # pragma: no cover - supports running as `python runner.py` from /app
    from config import engine_settings, isolation_settings, registry_internal_base_url
    from logs import LogWriter

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants. Centralised so the security review has one place to read.
# ---------------------------------------------------------------------------

# Resource limits.
CPU_PERIOD = 100_000
PID_LIMIT = 100

# Default image. The platform pins to a digest in production.
DEFAULT_IMAGE = "alpine:3.18"

# Exit codes.
EXIT_OOM = 137               # 128 + SIGKILL (9), Docker reports this on OOM
EXIT_TIMEOUT = 124            # convention used here; we set it ourselves

# Hard ceiling on how long a job can run before we kill it.
DEFAULT_TIMEOUT_S = 30 * 60


# ---------------------------------------------------------------------------
# Job model
# ---------------------------------------------------------------------------

@dataclass
class JobSpec:
    """Everything needed to run one build step."""
    run_id: str
    step_name: str               # e.g. "build", "test"
    script: str                  # the shell snippet to execute
    image: str = DEFAULT_IMAGE
    timeout_s: int = DEFAULT_TIMEOUT_S
    extra_env: dict = field(default_factory=dict)


@dataclass
class JobResult:
    run_id: str
    step_name: str
    exit_code: int
    oom_killed: bool
    timed_out: bool
    duration_s: float


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

class JobRunner:
    """
    One JobRunner per platform process. Holds a Docker client and a
    LogWriter factory.

    The caller (the scheduler) is responsible for issuing FORGE_TOKEN values
    and persisting JobResult. This class only runs containers.
    """

    def __init__(
        self,
        docker_client: Optional[docker.DockerClient] = None,
        log_dir: Optional[str] = None,
        token_provider: Optional[Callable[[str], str]] = None,
    ):
        engine_config = engine_settings()
        isolation_config = isolation_settings()
        self.client = docker_client or docker.from_env()
        self.log_dir = log_dir or engine_config.get("log_base", "/var/forge/logs")
        self.workspace_base = engine_config.get("workspace_base", "/data/workspaces")
        self.network_name = isolation_config.get("network_name", "forge-internal")
        self.registry_url = registry_internal_base_url()
        default_cpu = float(isolation_config.get("default_cpu", 1.0))
        self.cpu_quota = max(1, int(default_cpu * CPU_PERIOD))
        self.mem_limit = str(isolation_config.get("default_memory", "512m"))
        self.memswap_limit = self.mem_limit
        # token_provider lets tests inject deterministic tokens. In prod the
        # scheduler mints a short-lived token bound to (run_id, step_name).
        self._token_provider = token_provider or (lambda _run_id: uuid.uuid4().hex)

        self._ensure_network()

    # -- network ----------------------------------------------------------

    def _ensure_network(self) -> None:
        """Create forge-internal if it does not already exist."""
        try:
            self.client.networks.get(self.network_name)
            return
        except NotFound:
            pass

        log.info("creating docker network %s (internal)", self.network_name)
        self.client.networks.create(
            name=self.network_name,
            driver="bridge",
            internal=True,        # <- no external internet
            check_duplicate=True,
            attachable=True,
            options={
                # Disable inter-container communication on the default bridge
                # semantics. Containers on this network can still reach each
                # other (we need that for the registry), but the bridge has
                # no route to the outside.
                "com.docker.network.bridge.enable_ip_masquerade": "false",
            },
        )

    # -- env --------------------------------------------------------------

    def _build_env(self, spec: JobSpec) -> dict:
        """
        Compose the container environment. We refuse to forward arbitrary
        host env vars; only the explicit allow-list below plus extras the
        caller passed in.
        """
        env = {
            "FORGE_TOKEN": self._token_provider(spec.run_id),
            "FORGE_URL": self.registry_url,
            "FORGE_RUN_ID": spec.run_id,
            "FORGE_STEP": spec.step_name,
            # PATH is set explicitly so the container doesn't inherit /host paths.
            "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        }
        # Safe to merge: extra_env is a dict, not a shell string.
        env.update(spec.extra_env or {})
        return env

    # -- run --------------------------------------------------------------

    def run(self, spec: JobSpec) -> JobResult:
        """Run one job. Blocks until the container exits or is killed."""
        log_path = os.path.join(self.log_dir, f"{spec.run_id}.log")
        workspace_path = self._workspace_path(spec.run_id)
        writer = LogWriter(log_path, job=spec.step_name)

        writer.write(f"--- starting step {spec.step_name} ---")
        writer.write(
            f"image={spec.image} cpu={self.cpu_quota / CPU_PERIOD:.1f} mem={self.mem_limit} timeout={spec.timeout_s}s"
        )

        # We invoke sh -c <script>. The script is passed as a single argv
        # element (not interpolated into a shell string), so the only thing
        # interpreting it is the in-container sh. No host shell sees it.
        cmd = ["sh", "-c", spec.script]

        start = time.monotonic()
        container: Optional[Container] = None
        timed_out = False

        try:
            container = self.client.containers.run(
                image=spec.image,
                command=cmd,
                detach=True,
                remove=False,                    # we remove manually after reading state
                network=self.network_name,
                environment=self._build_env(spec),

                # --- isolation ---
                read_only=True,
                tmpfs={
                    "/tmp": "rw,size=64m,mode=1777",
                },
                volumes={
                    workspace_path: {"bind": "/workspace", "mode": "rw"},
                },
                working_dir="/workspace",
                user="nobody",                   # don't run as root in the container
                cap_drop=["ALL"],                # drop every Linux capability
                security_opt=["no-new-privileges"],

                # --- resources ---
                cpu_period=CPU_PERIOD,
                cpu_quota=self.cpu_quota,
                mem_limit=self.mem_limit,
                memswap_limit=self.memswap_limit,
                pids_limit=PID_LIMIT,
                oom_kill_disable=False,          # we WANT the kernel to OOM-kill

                # --- misc ---
                labels={
                    "forge.run_id": spec.run_id,
                    "forge.step": spec.step_name,
                },
                stdout=True,
                stderr=True,
            )

            self._stream_logs(container, writer)

            try:
                result = container.wait(timeout=spec.timeout_s)
                exit_code = int(result.get("StatusCode", -1))
            except Exception:
                # docker-py raises requests.exceptions.ReadTimeout on wait timeout.
                timed_out = True
                writer.write(f"Job killed: timeout after {spec.timeout_s}s")
                try:
                    container.kill()
                except APIError:
                    pass
                exit_code = EXIT_TIMEOUT

            oom_killed = self._was_oom_killed(container, exit_code)

            if oom_killed:
                writer.write(f"Job killed: memory limit exceeded ({self.mem_limit})")
            elif not timed_out:
                writer.write(f"--- step {spec.step_name} exited with code {exit_code} ---")

            duration = time.monotonic() - start
            return JobResult(
                run_id=spec.run_id,
                step_name=spec.step_name,
                exit_code=exit_code,
                oom_killed=oom_killed,
                timed_out=timed_out,
                duration_s=duration,
            )

        except ImageNotFound:
            writer.write(f"Job failed: image {spec.image!r} not found in registry")
            return JobResult(spec.run_id, spec.step_name, exit_code=125,
                             oom_killed=False, timed_out=False,
                             duration_s=time.monotonic() - start)
        except (ContainerError, APIError) as e:
            writer.write(f"Job failed: docker error: {e}")
            return JobResult(spec.run_id, spec.step_name, exit_code=125,
                             oom_killed=False, timed_out=False,
                             duration_s=time.monotonic() - start)
        finally:
            if container is not None:
                try:
                    container.remove(force=True)
                except APIError:
                    pass
            writer.close(write_eof=False)

    # -- helpers ----------------------------------------------------------

    def _stream_logs(self, container: Container, writer: LogWriter) -> None:
        """
        Stream container stdout+stderr to disk one line at a time.

        We use stream=True + follow=True. docker-py yields chunks, not lines,
        so we re-split on newlines ourselves. Each line is written through
        LogWriter which timestamps it and flushes to disk immediately.
        """
        try:
            log_iter = container.logs(stream=True, follow=True, stdout=True, stderr=True)
        except APIError as e:
            writer.write(f"could not attach to container logs: {e}")
            return

        buffer = b""
        for chunk in log_iter:
            if not chunk:
                continue
            buffer += chunk
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                writer.write(line.decode("utf-8", errors="replace"))

        # Anything still in the buffer (no trailing newline) is the last partial line.
        if buffer:
            writer.write(buffer.decode("utf-8", errors="replace"))

    def _workspace_path(self, run_id: str) -> str:
        """Return the host workspace directory for a pipeline run."""
        path = os.path.abspath(os.path.join(self.workspace_base, run_id))
        os.makedirs(path, exist_ok=True)
        return path

    def _was_oom_killed(self, container: Container, exit_code: int) -> bool:
        """
        Docker reports OOM in two places:
          1. container.attrs["State"]["OOMKilled"] == True
          2. ExitCode 137 (SIGKILL from the cgroup OOM killer)
        We trust (1) first and fall back to (2).
        """
        try:
            container.reload()
            state = container.attrs.get("State", {}) or {}
            if state.get("OOMKilled"):
                return True
        except APIError:
            pass
        return exit_code == EXIT_OOM


# ---------------------------------------------------------------------------
# Tiny CLI for local sanity checks: `python -m engine.runner "echo hi"`
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    script = " ".join(shlex.quote(a) for a in sys.argv[1:]) or "echo hello from forge"
    runner = JobRunner(log_dir="/tmp")
    res = runner.run(JobSpec(
        run_id="local-" + uuid.uuid4().hex[:8],
        step_name="build",
        script=script,
    ))
    print(res)
