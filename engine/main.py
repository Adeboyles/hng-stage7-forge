import uuid
import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiosqlite
from fastapi import FastAPI, UploadFile, File, Header, HTTPException
from fastapi.responses import StreamingResponse

try:
    from .config import engine_settings, registry_internal_base_url
    from .parser import parse_pipeline_text as parse_pipeline, PipelineValidationError
    from .scheduler import DAGScheduler, SchedulerError
    from .logs import tail_log
    from . import slack
except ImportError:  # pragma: no cover - supports running as `uvicorn main:app` from /app
    from config import engine_settings, registry_internal_base_url
    from parser import parse_pipeline_text as parse_pipeline, PipelineValidationError
    from scheduler import DAGScheduler, SchedulerError
    from logs import tail_log
    import slack


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Initialize persistent engine resources before serving requests."""
    await init_db()
    yield


ENGINE_CONFIG = engine_settings()
app = FastAPI(title="Forge CI Engine", lifespan=lifespan)

# ── DB ───────────────────────────────────────────────

DB_PATH = Path("/tmp/engine.db")
LOG_DIR = Path(ENGINE_CONFIG.get("log_base", "/var/forge/logs"))
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
            await slack.notify_resolution_failure(pipeline_name, run_id, str(e))
            return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE runs SET lockfile=? WHERE id=?",
            (json.dumps(lockfile), run_id)
        )
        await db.commit()

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
        await slack.notify_resolution_failure(pipeline_name, run_id, str(e))
        return

    try:
        JobRunner, JobSpec = _load_runner_types()
        runner = JobRunner(log_dir=str(LOG_DIR))
    except Exception as e:
        finished_at = datetime.now(timezone.utc).isoformat()
        await update_run_status(run_id, "failed", finished_at=finished_at, jobs={})
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
            extra_env={},
        )
        return runner.run(spec)

    try:
        run_result = scheduler.run(executor)
    except SchedulerError as e:
        await update_run_status(run_id, "failed")
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
    finished_at = datetime.now(timezone.utc).isoformat()

    await update_run_status(
        run_id,
        final_status,
        finished_at=finished_at,
        jobs=job_results
    )

    duration = (
        datetime.fromisoformat(finished_at) -
        datetime.fromisoformat(started_at)
    ).seconds

    if final_status == "succeeded":
        await slack.notify_pipeline_succeeded(pipeline_name, run_id, f"{duration}s")
    else:
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
