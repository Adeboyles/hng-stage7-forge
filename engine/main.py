import uuid
import asyncio
import hashlib
import io
import json
import shutil
import tarfile
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiosqlite
from fastapi import FastAPI, UploadFile, File, Header, HTTPException
from fastapi.responses import StreamingResponse
import httpx

try:
    from .config import engine_settings, registry_internal_base_url
    from .logs import append_eof, append_log_line, tail_log
    from .parser import parse_pipeline_text as parse_pipeline, PipelineValidationError
    from .scheduler import DAGScheduler, SchedulerError
    from . import slack
except ImportError:  # pragma: no cover - supports running as `uvicorn main:app` from /app
    from config import engine_settings, registry_internal_base_url
    from logs import append_eof, append_log_line, tail_log
    from parser import parse_pipeline_text as parse_pipeline, PipelineValidationError
    from scheduler import DAGScheduler, SchedulerError
    import slack


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Initialize persistent engine resources before serving requests."""
    await init_db()
    yield


ENGINE_CONFIG = engine_settings()
app = FastAPI(title="Forge CI Engine", lifespan=lifespan)

# ── NEW ROOT ROUTE ───────────────────────────────────

@app.get("/")
async def root():
    return {
        "status": "healthy",
        "service": "Forge CI Engine",
        "message": "Welcome! Use /docs to view available API endpoints."
    }

# ── DB ───────────────────────────────────────────────

DB_PATH = Path("/tmp/engine.db")
LOG_DIR = Path(ENGINE_CONFIG.get("log_base", "/var/forge/logs"))
WORKSPACE_BASE = Path(ENGINE_CONFIG.get("workspace_base", "/data/workspaces"))
DEFAULT_CONCURRENCY_LIMIT = int(ENGINE_CONFIG.get("max_concurrency", 4))


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY,
                pipeline_name TEXT,
                status TEXT,
                lockfile TEXT,
                jobs TEXT,
                started_at TEXT,
                finished_at TEXT,
                created_at TEXT
            )
        """)
        await db.commit()

# ── Auth ──────────────────────────────────────────────

async def verify_token(authorization: Optional[str]) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")

    token = authorization.replace("Bearer ", "").strip()

    import httpx
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{registry_internal_base_url()}/auth/verify",
            headers={"Authorization": f"Bearer {token}"},
            timeout=5.0
        )

        if resp.status_code != 200:
            raise HTTPException(status_code=401, detail="Invalid token")

    return token


# ── CREATE RUN ───────────────────────────────────────

@app.post("/runs")
async def create_run(
    pipeline: UploadFile = File(...),
    authorization: Optional[str] = Header(None)
):
    token = await verify_token(authorization)

    content = await pipeline.read()

    try:
        pipeline_def = parse_pipeline(content.decode("utf-8"))
    except PipelineValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))

    run_id = str(uuid.uuid4())

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO runs (id, pipeline_name, status, jobs, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                run_id,
                pipeline_def.name,
                "queued",
                json.dumps({}),
                datetime.now(timezone.utc).isoformat()
            )
        )
        await db.commit()

    asyncio.create_task(execute_pipeline(run_id, pipeline_def, token))

    return {"run_id": run_id}


@app.get("/runs/{run_id}")
async def get_run(run_id: str, authorization: Optional[str] = Header(None)):
    """Return the persisted run status, per-job status, and lockfile URL."""
    await verify_token(authorization)
    row = await _fetch_run_row(run_id)
    return {
        "status": row["status"],
        "jobs": json.loads(row["jobs"] or "{}"),
        "lockfile_url": f"/runs/{run_id}/lockfile",
    }


@app.get("/runs/{run_id}/lockfile")
async def get_lockfile(run_id: str, authorization: Optional[str] = Header(None)):
    """Return the exact lockfile JSON stored for a run."""
    await verify_token(authorization)
    row = await _fetch_run_row(run_id)
    return json.loads(row["lockfile"] or "{}")


@app.get("/runs/{run_id}/logs")
async def stream_run_logs(
    run_id: str,
    follow: bool = False,
    authorization: Optional[str] = Header(None),
):
    """Stream persisted run logs over Server-Sent Events."""
    await verify_token(authorization)
    await _fetch_run_row(run_id)
    log_path = str(LOG_DIR / f"{run_id}.log")
    return StreamingResponse(
        tail_log(log_path, follow=follow),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache"},
    )


# ── PIPELINE EXECUTION ───────────────────────────────

async def execute_pipeline(run_id: str, pipeline_def, token: str):
    pipeline_name = pipeline_def.name
    started_at = datetime.now(timezone.utc).isoformat()

    await update_run_status(run_id, "running", started_at=started_at)
    await slack.notify_pipeline_started(pipeline_name, run_id)

    # -----------------------------
    # Dependencies / Lockfile
    # -----------------------------
    deps = list(pipeline_def.dependencies)

    lockfile = {}
    if deps:
        try:
            lockfile = await resolve_dependencies(deps, token)
        except Exception as e:
            await update_run_status(run_id, "conflict_failure")
            _log_system(run_id, f"dependency resolution failed: {e}")
            _finish_run_log(run_id)
            await slack.notify_resolution_failure(pipeline_name, run_id, str(e))
            return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE runs SET lockfile=? WHERE id=?",
            (json.dumps(lockfile), run_id)
        )
        await db.commit()

    try:
        await materialize_dependencies(run_id, lockfile, token)
    except IntegrityFailureError as e:
        finished_at = datetime.now(timezone.utc).isoformat()
        _log_system(
            run_id,
            f"integrity failure for {e.artifact}: expected sha256 {e.expected_sha}, actual sha256 {e.actual_sha}",
        )
        await update_run_status(run_id, "integrity_failure", finished_at=finished_at, jobs={})
        _finish_run_log(run_id)
        await slack.notify_integrity_failure(e.artifact, e.expected_sha, e.actual_sha, run_id)
        return

    # -----------------------------
    # DAG execution
    # -----------------------------
    try:
        scheduler = DAGScheduler(
            pipeline_def,
            concurrency_limit=DEFAULT_CONCURRENCY_LIMIT,
        )
    except SchedulerError as e:
        await update_run_status(run_id, "cycle_failure")
        _log_system(run_id, f"job graph cycle detected: {e}")
        _finish_run_log(run_id)
        await slack.notify_resolution_failure(pipeline_name, run_id, str(e))
        return

    try:
        JobRunner, JobSpec = _load_runner_types()
        runner = JobRunner(
            log_dir=str(LOG_DIR),
            token_provider=lambda _run_id: token,
        )
    except Exception as e:
        finished_at = datetime.now(timezone.utc).isoformat()
        _log_system(run_id, f"runner initialization failed: {e}")
        await update_run_status(run_id, "failed", finished_at=finished_at, jobs={})
        _finish_run_log(run_id)
        await slack.notify_pipeline_failed(pipeline_name, run_id, "0s", "runner_init")
        await slack.notify_resolution_failure(pipeline_name, run_id, str(e))
        return
    failing_job = None

    def executor(job_name, job_def):
        spec = JobSpec(
            run_id=run_id,
            step_name=job_name,
            script=job_def.to_shell_script(),
            image=job_def.runtime,
            cpu_limit=job_def.resources.cpu,
            memory_limit=job_def.resources.memory,
            extra_env={},
        )
        return runner.run(spec)

    try:
        run_result = scheduler.run(executor)
    except SchedulerError as e:
        await update_run_status(run_id, "failed")
        _log_system(run_id, f"scheduler execution failed: {e}")
        _finish_run_log(run_id)
        await slack.notify_pipeline_failed(pipeline_name, run_id, "0s", "scheduler")
        await slack.notify_resolution_failure(pipeline_name, run_id, str(e))
        return

    job_results = {}
    final_status = "succeeded"

    for job_name in sorted(pipeline_def.jobs):
        status = run_result.job_statuses[job_name]
        job_result = {"status": status}
        executor_result = run_result.executor_results.get(job_name)

        if executor_result is not None:
            if hasattr(executor_result, "exit_code"):
                job_result["exit_code"] = executor_result.exit_code
            if hasattr(executor_result, "duration_s"):
                job_result["duration_s"] = executor_result.duration_s

        job_results[job_name] = job_result

        if status == "failed" and failing_job is None:
            failing_job = job_name
            final_status = "failed"

    # -----------------------------
    # Finalize
    # -----------------------------
    if final_status == "succeeded":
        try:
            await publish_pipeline_artifacts(
                run_id,
                pipeline_def,
                token,
            )
        except Exception as e:
            final_status = "failed"
            finished_at = datetime.now(timezone.utc).isoformat()
            duration = (
                datetime.fromisoformat(finished_at) -
                datetime.fromisoformat(started_at)
            ).seconds
            await update_run_status(
                run_id,
                final_status,
                finished_at=finished_at,
                jobs=job_results,
            )
            _log_system(run_id, f"artifact publish failed: {e}")
            _finish_run_log(run_id)
            await slack.notify_pipeline_failed(
                pipeline_name, run_id, f"{duration}s", "artifact_publish"
            )
            return
        finished_at = datetime.now(timezone.utc).isoformat()
        duration = (
            datetime.fromisoformat(finished_at) -
            datetime.fromisoformat(started_at)
        ).seconds
        await update_run_status(
            run_id,
            final_status,
            finished_at=finished_at,
            jobs=job_results,
        )
        _finish_run_log(run_id)
        await slack.notify_pipeline_succeeded(pipeline_name, run_id, f"{duration}s")
    else:
        finished_at = datetime.now(timezone.utc).isoformat()
        duration = (
            datetime.fromisoformat(finished_at) -
            datetime.fromisoformat(started_at)
        ).seconds
        await update_run_status(
            run_id,
            final_status,
            finished_at=finished_at,
            jobs=job_results,
        )
        _finish_run_log(run_id)
        await slack.notify_pipeline_failed(
            pipeline_name, run_id, f"{duration}s", failing_job or "unknown"
        )


# ── DB HELPERS ───────────────────────────────────────

async def update_run_status(run_id, status, started_at=None, finished_at=None, jobs=None):
    async with aiosqlite.connect(DB_PATH) as db:
        updates = ["status=?"]
        values = [status]

        if started_at:
            updates.append("started_at=?")
            values.append(started_at)

        if finished_at:
            updates.append("finished_at=?")
            values.append(finished_at)

        if jobs is not None:
            updates.append("jobs=?")
            values.append(json.dumps(jobs))

        values.append(run_id)

        await db.execute(
            f"UPDATE runs SET {', '.join(updates)} WHERE id=?",
            values
        )
        await db.commit()


async def _fetch_run_row(run_id: str) -> dict:
    """Load a run row or raise 404 if the run does not exist."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, status, lockfile, jobs FROM runs WHERE id=?",
            (run_id,),
        )
        row = await cursor.fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="Run not found")

    return dict(row)


class IntegrityFailureError(Exception):
    """Raised when a downloaded dependency does not match the lockfile SHA-256."""

    def __init__(self, artifact: str, expected_sha: str, actual_sha: str):
        self.artifact = artifact
        self.expected_sha = expected_sha
        self.actual_sha = actual_sha
        super().__init__(
            f"integrity failure for {artifact}: expected {expected_sha}, got {actual_sha}"
        )


async def materialize_dependencies(run_id: str, lockfile: dict, token: str) -> None:
    """Download, verify, and unpack lockfile dependencies into the shared workspace."""
    resolved = lockfile.get("resolved", {})
    if not resolved:
        return

    deps_root = _deps_root(run_id)
    deps_root.mkdir(parents=True, exist_ok=True)

    for name in sorted(resolved):
        entry = resolved[name]
        version = entry["version"]
        expected_sha = entry["sha256"]
        artifact = f"{name}@{version}"
        _log_system(run_id, f"pulling dependency {artifact}")
        payload = await download_artifact(name, version, token)
        actual_sha = hashlib.sha256(payload).hexdigest()
        if actual_sha != expected_sha:
            raise IntegrityFailureError(artifact, expected_sha, actual_sha)
        package_dir = deps_root / name
        if package_dir.exists():
            shutil.rmtree(package_dir)
        package_dir.mkdir(parents=True, exist_ok=True)
        _unpack_dependency_bytes(payload, package_dir)
        _log_system(run_id, f"verified dependency {artifact} sha256 {actual_sha}")


async def download_artifact(name: str, version: str, token: str) -> bytes:
    """Fetch one resolved artifact blob from the registry."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{registry_internal_base_url()}/artifacts/{name}/{version}")
        resp.raise_for_status()
        return resp.content


async def publish_pipeline_artifacts(run_id: str, pipeline_def, token: str) -> None:
    """Publish all declared top-level artifacts from the run workspace."""
    deps_payload = [
        {"name": dep.name, "version": dep.version}
        for dep in pipeline_def.dependencies
    ]
    workspace = _workspace_path(run_id)
    for artifact in pipeline_def.artifacts:
        artifact_path = (workspace / artifact.path).resolve()
        await publish_artifact(
            artifact_path,
            artifact.name,
            artifact.version,
            token,
            deps=deps_payload,
        )
        _log_system(run_id, f"published artifact {artifact.name}@{artifact.version}")


async def publish_artifact(
    path: Path,
    name: str,
    version: str,
    token: str,
    *,
    deps: list[dict[str, str]] | None = None,
) -> None:
    """Upload one built artifact to the registry with server-side checksum verification."""
    payload = path.read_bytes()
    checksum = "sha256:" + hashlib.sha256(payload).hexdigest()
    data = {"checksum": checksum}
    if deps:
        data["deps"] = json.dumps(deps, sort_keys=True, separators=(",", ":"))

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{registry_internal_base_url()}/artifacts/{name}/{version}",
            headers={"Authorization": f"Bearer {token}"},
            files={"file": (path.name, payload, "application/octet-stream")},
            data=data,
            timeout=120.0,
        )
        resp.raise_for_status()


def _unpack_dependency_bytes(payload: bytes, target_dir: Path) -> None:
    """Extract an archive into the dependency directory, or store raw bytes if not archived."""
    try:
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r:*") as archive:
            _safe_extract_tar(archive, target_dir)
            return
    except tarfile.TarError:
        pass

    (target_dir / "artifact.bin").write_bytes(payload)


def _safe_extract_tar(archive: tarfile.TarFile, target_dir: Path) -> None:
    """Extract tar members while preventing path traversal outside the target directory."""
    base = target_dir.resolve()
    for member in archive.getmembers():
        destination = (target_dir / member.name).resolve()
        if not str(destination).startswith(str(base)):
            raise IntegrityFailureError(member.name, "safe-path", "path-traversal")
    archive.extractall(target_dir)


def _workspace_path(run_id: str) -> Path:
    """Return the shared workspace directory for one pipeline run."""
    path = WORKSPACE_BASE / run_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def _deps_root(run_id: str) -> Path:
    """Return the dependency directory inside the run workspace."""
    return _workspace_path(run_id) / "deps"


def _run_log_path(run_id: str) -> str:
    return str(LOG_DIR / f"{run_id}.log")


def _log_system(run_id: str, line: str) -> None:
    append_log_line(_run_log_path(run_id), "system", line)


def _finish_run_log(run_id: str) -> None:
    append_eof(_run_log_path(run_id), "system")


# ── DEPENDENCY RESOLUTION ───────────────────────────

async def resolve_dependencies(deps, token):
    import httpx

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{registry_internal_base_url()}/resolve",
            json={"dependencies": [d.__dict__ for d in deps]},
            headers={"Authorization": f"Bearer {token}"}
        )

        if resp.status_code != 200:
            raise Exception(resp.text)

        return resp.json()


def _load_runner_types():
    """Import runner types lazily so engine.main can load without Docker SDK."""
    try:
        from .runner import JobRunner, JobSpec
    except ImportError:  # pragma: no cover - supports running as `uvicorn main:app` from /app
        from runner import JobRunner, JobSpec
    return JobRunner, JobSpec
