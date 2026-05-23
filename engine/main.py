import uuid
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml
import aiosqlite
from fastapi import FastAPI, UploadFile, File, Header, HTTPException, Request
from fastapi.responses import StreamingResponse, JSONResponse

from engine.parser import parse_pipeline, PipelineValidationError
from engine.scheduler import DAGScheduler
from engine.runner import JobRunner, Job, JobStep, JobResources, JobArtifact
from engine.logs import stream_logs, write_log_line
from engine import slack

app = FastAPI(title="Forge CI Engine")

# ── Database setup ─────────────────────────────────────────────────

DB_PATH = Path("/tmp/engine.db")


async def get_db():
    return await aiosqlite.connect(DB_PATH)


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY,
                pipeline_name TEXT NOT NULL,
                status TEXT NOT NULL,
                lockfile TEXT,
                jobs TEXT,
                started_at TEXT,
                finished_at TEXT,
                created_at TEXT NOT NULL
            )
        """)
        await db.commit()


@app.on_event("startup")
async def startup():
    await init_db()


# ── Auth helper ────────────────────────────────────────────────────

async def verify_token(authorization: Optional[str]) -> str:
    """Verify Bearer token against registry auth."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = authorization.replace("Bearer ", "").strip()
    # Delegate token verification to registry
    import httpx
    registry_url = "http://registry:8001"
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(
                f"{registry_url}/auth/verify",
                headers={"Authorization": f"Bearer {token}"},
                timeout=5.0
            )
            if resp.status_code != 200:
                raise HTTPException(status_code=401, detail="Invalid token")
        except httpx.RequestError:
            raise HTTPException(
                status_code=503,
                detail="Registry unavailable for auth"
            )
    return token


# ── Run a pipeline ─────────────────────────────────────────────────

@app.post("/runs")
async def create_run(
    pipeline: UploadFile = File(...),
    authorization: Optional[str] = Header(None)
):
    token = await verify_token(authorization)

    # Read and parse pipeline YAML
    content = await pipeline.read()
    try:
        pipeline_def = parse_pipeline(content.decode("utf-8"))
    except PipelineValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))

    run_id = str(uuid.uuid4())

    # Save run to DB with queued status
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO runs
            (id, pipeline_name, status, jobs, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                run_id,
                pipeline_def["name"],
                "queued",
                json.dumps({}),
                datetime.now(timezone.utc).isoformat()
            )
        )
        await db.commit()

    # Run pipeline in background
    asyncio.create_task(
        execute_pipeline(run_id, pipeline_def, token)
    )

    return {"run_id": run_id}


async def execute_pipeline(run_id: str, pipeline_def: dict, token: str):
    """
    Full pipeline execution:
    1. Resolve dependencies
    2. Build DAG
    3. Execute jobs
    4. Send Slack notifications
    """
    pipeline_name = pipeline_def["name"]
    started_at = datetime.now(timezone.utc).isoformat()

    await update_run_status(run_id, "running", started_at=started_at)
    await slack.notify_pipeline_started(pipeline_name, run_id)

    # Resolve dependencies first
    lockfile = {}
    deps = pipeline_def.get("dependencies", [])

    if deps:
        try:
            lockfile = await resolve_dependencies(deps, run_id, token)
        except Exception as e:
            details = str(e)
            await update_run_status(run_id, "conflict_failure")
            await slack.notify_resolution_failure(
                pipeline_name, run_id, details
            )
            write_log_line(run_id, "system", f"Resolution failed: {details}")
            return

    # Save lockfile to DB
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE runs SET lockfile=? WHERE id=?",
            (json.dumps(lockfile), run_id)
        )
        await db.commit()

    # Build and execute DAG
    jobs = pipeline_def.get("jobs", {})
    scheduler = DAGScheduler()

    try:
        execution_order = scheduler.build_and_sort(jobs)
    except Exception as e:
        details = str(e)
        await update_run_status(run_id, "cycle_failure")
        await slack.notify_resolution_failure(
            pipeline_name, run_id, details
        )
        return

    runner = JobRunner(
        registry_url="http://registry:8001",
        forge_token=token,
        internal_network="forge-internal",
    )

    job_results = {}
    final_status = "succeeded"
    failing_job = None

    for job_name in execution_order:
        job_def = jobs[job_name]

        # Check if any dependency failed — skip if so
        needs = job_def.get("needs", [])
        should_skip = any(
            job_results.get(n, {}).get("status") in ("failed", "skipped")
            for n in needs
        )

        if should_skip:
            job_results[job_name] = {"status": "skipped"}
            write_log_line(run_id, job_name, f"Job skipped — dependency failed")
            continue

        # Build job object
        job = Job(
            name=job_name,
            runtime=job_def["runtime"],
            resources=JobResources(
                cpu=job_def.get("resources", {}).get("cpu", 1.0),
                memory=job_def.get("resources", {}).get("memory", "512Mi"),
            ),
            steps=[
                JobStep(name=s["name"], run=s["run"])
                for s in job_def.get("steps", [])
            ],
            artifacts=[
                JobArtifact(
                    name=a["name"],
                    version=a["version"],
                    path=a["path"]
                )
                for a in job_def.get("artifacts", [])
            ],
            needs=needs,
        )

        # Run job
        result = await runner.run(
            run_id=run_id,
            job=job,
            dependencies=[
                type("Dep", (), {"name": d["name"], "version": d["version"]})()
                for d in deps
            ],
            lockfile=lockfile,
        )

        job_results[job_name] = {
            "status": result.status,
            "started_at": result.started_at,
            "finished_at": result.finished_at,
            "exit_code": result.exit_code,
        }

        # Handle integrity failure
        if result.status == "integrity_failure":
            final_status = "integrity_failure"
            failing_job = job_name
            await slack.notify_integrity_failure(
                artifact=job_name,
                expected_sha="unknown",
                actual_sha="unknown",
                run_id=run_id
            )
            break

        if result.status == "failed":
            final_status = "failed"
            failing_job = job_name

    # Update final run status
    finished_at = datetime.now(timezone.utc).isoformat()
    await update_run_status(
        run_id,
        final_status,
        finished_at=finished_at,
        jobs=job_results
    )

    # Calculate duration
    start = datetime.fromisoformat(started_at)
    end = datetime.fromisoformat(finished_at)
    duration = f"{(end - start).seconds}s"

    # Send final Slack notification
    if final_status == "succeeded":
        await slack.notify_pipeline_succeeded(
            pipeline_name, run_id, duration
        )
    else:
        await slack.notify_pipeline_failed(
            pipeline_name, run_id, duration,
            failing_job or "unknown"
        )


async def resolve_dependencies(
    deps: list,
    run_id: str,
    token: str
) -> dict:
    """Call registry resolver to get lockfile."""
    import httpx
    registry_url = "http://registry:8001"
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{registry_url}/resolve",
            json={"dependencies": deps},
            headers={"Authorization": f"Bearer {token}"},
            timeout=30.0
        )
        if resp.status_code != 200:
            raise Exception(resp.json().get("detail", "Resolution failed"))
        return resp.json()


async def update_run_status(
    run_id: str,
    status: str,
    started_at: str = None,
    finished_at: str = None,
    jobs: dict = None
):
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


# ── Get run status ─────────────────────────────────────────────────

@app.get("/runs/{run_id}")
async def get_run(run_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM runs WHERE id=?", (run_id,)
        ) as cursor:
            row = await cursor.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Run not found")

    return {
        "run_id": row["id"],
        "status": row["status"],
        "pipeline_name": row["pipeline_name"],
        "jobs": json.loads(row["jobs"] or "{}"),
        "lockfile_url": f"/runs/{run_id}/lockfile",
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
    }


# ── Get lockfile ───────────────────────────────────────────────────

@app.get("/runs/{run_id}/lockfile")
async def get_lockfile(run_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT lockfile FROM runs WHERE id=?", (run_id,)
        ) as cursor:
            row = await cursor.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Run not found")

    if not row["lockfile"]:
        return {}

    return json.loads(row["lockfile"])


# ── Stream logs ────────────────────────────────────────────────────

@app.get("/runs/{run_id}/logs")
async def get_logs(run_id: str, follow: bool = False):
    """
    Stream logs as Server-Sent Events.
    follow=true → keep streaming as new lines are written
    follow=false → stream existing lines and stop
    """
    return StreamingResponse(
        stream_logs(run_id, follow=follow),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # disable nginx buffering
        }
    )