"""
engine/runner.py
----------------
Executes build jobs inside hardened Docker containers.

Isolation guarantees enforced per job:
  - No host network. Each job gets its own internal Docker network.
    Only the registry service is attached to that network.
  - Read-only root filesystem. Writable space is the shared /workspace bind mount
    for the current run plus tmpfs on /tmp.
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
import socket
import shlex
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Optional

import docker
from docker.errors import APIError, ContainerError, ImageNotFound, NotFound
from docker.models.containers import Container

try:
    from .config import engine_settings, isolation_settings
    from .logs import LogWriter
except (
    ImportError
):  # pragma: no cover - supports running as `python runner.py` from /app
    from config import engine_settings, isolation_settings
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
EXIT_OOM = 137  # 128 + SIGKILL (9), Docker reports this on OOM
EXIT_TIMEOUT = 124  # convention used here; we set it ourselves

# Hard ceiling on how long a job can run before we kill it.
DEFAULT_TIMEOUT_S = 30 * 60


# ---------------------------------------------------------------------------
# Job model
# ---------------------------------------------------------------------------


@dataclass
class JobSpec:
    """Everything needed to run one build step."""

    run_id: str
    step_name: str  # e.g. "build", "test"
    script: str  # the shell snippet to execute
    image: str = DEFAULT_IMAGE
    timeout_s: int = DEFAULT_TIMEOUT_S
    cpu_limit: Optional[float] = None
    memory_limit: Optional[str] = None
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
        self.registry_host, self.registry_port = self._registry_target(isolation_config)
        self.registry_url = f"http://{self.registry_host}:{self.registry_port}"
        self.default_cpu = float(isolation_config.get("default_cpu", 1.0))
        self.default_memory = str(isolation_config.get("default_memory", "512m"))
        self._host_workspace_base: Optional[str] = None
        # token_provider lets tests inject deterministic tokens. In prod the
        # scheduler mints a short-lived token bound to (run_id, step_name).
        self._token_provider = token_provider or (lambda _run_id: uuid.uuid4().hex)

    # -- network ----------------------------------------------------------

    def _create_job_network(self, run_id: str):
        """Create an internal network for one job run and attach only the registry container."""
        self._prune_stale_job_networks()
        network_name = f"{self.network_name}-{run_id[:12]}"
        log.info("creating docker network %s (internal)", network_name)
        network = self.client.networks.create(
            name=network_name,
            driver="bridge",
            internal=True,  # <- no external internet
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
        registry_container = self._registry_container()
        network.connect(registry_container, aliases=[self.registry_host])
        return network

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
        workspace_bind_source = self._host_workspace_path(spec.run_id)
        writer = LogWriter(log_path, job=spec.step_name)
        cpu_limit = spec.cpu_limit if spec.cpu_limit is not None else self.default_cpu
        cpu_quota = self._cpu_quota(cpu_limit)
        mem_limit = self._docker_memory_limit(
            spec.memory_limit if spec.memory_limit is not None else self.default_memory
        )

        writer.write(f"--- starting step {spec.step_name} ---")
        writer.write(
            f"image={spec.image} cpu={cpu_limit:.1f} mem={mem_limit} timeout={spec.timeout_s}s"
        )

        # We invoke sh -c <script>. The script is passed as a single argv
        # element (not interpolated into a shell string), so the only thing
        # interpreting it is the in-container sh. No host shell sees it.
        cmd = ["sh", "-c", spec.script]

        start = time.monotonic()
        container: Optional[Container] = None
        network = None
        registry_container = None
        registry_attached = False
        timed_out = False

        try:
            network = self._create_job_network(spec.run_id)
            registry_container = self._registry_container()
            registry_attached = True
            container = self.client.containers.run(
                image=spec.image,
                command=cmd,
                detach=True,
                remove=False,  # we remove manually after reading state
                network=network.name,
                environment=self._build_env(spec),
                # --- isolation ---
                read_only=True,
                tmpfs={
                    "/tmp": "rw,size=64m,mode=1777",
                },
                volumes={
                    workspace_bind_source: {"bind": "/workspace", "mode": "rw"},
                },
                working_dir="/workspace",
                cap_drop=["ALL"],  # drop every Linux capability
                security_opt=["no-new-privileges"],
                # --- resources ---
                cpu_period=CPU_PERIOD,
                cpu_quota=cpu_quota,
                mem_limit=mem_limit,
                memswap_limit=mem_limit,
                pids_limit=PID_LIMIT,
                oom_kill_disable=False,  # we WANT the kernel to OOM-kill
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
                writer.write(f"Job killed: memory limit exceeded ({mem_limit})")
            elif not timed_out:
                writer.write(
                    f"--- step {spec.step_name} exited with code {exit_code} ---"
                )

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
            return JobResult(
                spec.run_id,
                spec.step_name,
                exit_code=125,
                oom_killed=False,
                timed_out=False,
                duration_s=time.monotonic() - start,
            )
        except (ContainerError, APIError) as e:
            writer.write(f"Job failed: docker error: {e}")
            return JobResult(
                spec.run_id,
                spec.step_name,
                exit_code=125,
                oom_killed=False,
                timed_out=False,
                duration_s=time.monotonic() - start,
            )
        finally:
            if container is not None:
                try:
                    container.remove(force=True)
                except APIError as exc:
                    log.warning(
                        "failed to remove job container for run %s step %s: %s",
                        spec.run_id,
                        spec.step_name,
                        exc,
                    )
            if network is not None and registry_attached and registry_container is not None:
                try:
                    network.disconnect(registry_container, force=True)
                except APIError as exc:
                    log.warning(
                        "failed to disconnect registry from job network %s for run %s: %s",
                        network.name,
                        spec.run_id,
                        exc,
                    )
            if network is not None:
                try:
                    network.remove()
                except APIError as exc:
                    log.warning(
                        "failed to remove job network %s for run %s: %s",
                        network.name,
                        spec.run_id,
                        exc,
                    )
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
            log_iter = container.logs(
                stream=True, follow=True, stdout=True, stderr=True
            )
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
        """Return the engine-visible workspace directory for a pipeline run."""
        path = os.path.normpath(
            os.path.abspath(os.path.join(self.workspace_base, run_id))
        )
        os.makedirs(path, exist_ok=True)
        return path

    def _host_workspace_path(self, run_id: str) -> str:
        """
        Return the Docker-daemon-visible workspace source path for a pipeline run.

        The engine process runs inside a container and talks to the host Docker
        daemon over `/var/run/docker.sock`. That means bind-mount source paths are
        interpreted by the daemon host, not by the engine container. We inspect
        the engine container's own bind mounts to translate `/data/workspaces`
        into the corresponding host source path before launching sibling job
        containers.
        """
        base = self._resolve_host_workspace_base()
        return os.path.normpath(os.path.join(base, run_id))

    def _resolve_host_workspace_base(self) -> str:
        """Find the host bind source that backs the engine's workspace mount."""
        if self._host_workspace_base is not None:
            return self._host_workspace_base

        fallback_base = os.path.normpath(self.workspace_base)
        container_name = os.environ.get("HOSTNAME") or socket.gethostname()
        try:
            container = self.client.containers.get(container_name)
        except (APIError, AttributeError, NotFound):
            self._host_workspace_base = fallback_base
            return self._host_workspace_base

        for mount in getattr(container, "attrs", {}).get("Mounts", []):
            destination = mount.get("Destination")
            source = mount.get("Source")
            if isinstance(source, str) and self._same_path(
                destination, self.workspace_base
            ):
                self._host_workspace_base = os.path.normpath(source)
                return self._host_workspace_base

        self._host_workspace_base = fallback_base
        return self._host_workspace_base

    def _same_path(self, left: str | None, right: str | None) -> bool:
        """Compare two paths using the current platform's normalization rules."""
        if not left or not right:
            return False
        return os.path.normcase(os.path.normpath(left)) == os.path.normcase(
            os.path.normpath(right)
        )

    def _registry_container(self):
        """Find the registry service container managed by Docker Compose."""
        containers = self.client.containers.list(
            filters={"label": "com.docker.compose.service=registry"}
        )
        if len(containers) != 1:
            raise APIError("expected exactly one registry service container")
        return containers[0]

    def _registry_target(self, isolation_config: dict) -> tuple[str, int]:
        """Return the configured registry hostname and port allowed for egress."""
        allowed = isolation_config.get("allowed_egress") or []
        if len(allowed) != 1:
            raise ValueError(
                "isolation.allowed_egress must contain exactly one registry endpoint"
            )
        target = str(allowed[0])
        if ":" not in target:
            raise ValueError(
                "isolation.allowed_egress entries must be in host:port form"
            )
        host, port_text = target.rsplit(":", 1)
        return host, int(port_text)

    def _prune_stale_job_networks(self) -> None:
        """Best-effort cleanup of leaked empty Forge job networks from prior runs."""
        try:
            networks = self.client.networks.list(names=[self.network_name])
        except (APIError, AttributeError) as exc:
            log.warning("failed to list stale job networks: %s", exc)
            return

        prefix = f"{self.network_name}-"
        for network in networks:
            name = getattr(network, "name", "")
            if not name.startswith(prefix):
                continue
            containers = getattr(network, "attrs", {}).get("Containers") or {}
            if containers:
                continue
            try:
                network.remove()
                log.info("removed stale job network %s", name)
            except APIError as exc:
                log.warning("failed to remove stale job network %s: %s", name, exc)

    def _cpu_quota(self, cpu_limit: float) -> int:
        """Convert a CPU count into a Docker CFS quota."""
        return max(1, int(cpu_limit * CPU_PERIOD))

    def _docker_memory_limit(self, value: str) -> str | int:
        """Normalize memory strings so YAML values like `512Mi` become Docker-safe byte counts."""
        text = str(value).strip()
        units = {
            "ki": 1024,
            "mi": 1024**2,
            "gi": 1024**3,
            "ti": 1024**4,
        }
        lowered = text.lower()
        for suffix, multiplier in units.items():
            if lowered.endswith(suffix):
                amount = float(text[: -len(suffix)])
                return int(amount * multiplier)
        return text

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
    res = runner.run(
        JobSpec(
            run_id="local-" + uuid.uuid4().hex[:8],
            step_name="build",
            script=script,
        )
    )
    print(res)
