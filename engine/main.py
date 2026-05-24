import uuid
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiosqlite
from fastapi import FastAPI, UploadFile, File, Header, HTTPException

from parser import parse_pipeline_text as parse_pipeline, PipelineValidationError
from scheduler import DAGScheduler
from runner import JobRunner, JobSpec
import slack

app = FastAPI(title="Forge CI Engine")

# ── DB ───────────────────────────────────────────────

DB_PATH = Path("/tmp/engine.db")


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


@app.on_event("startup")
async def startup():
    await init_db()


# ── Auth ──────────────────────────────────────────────

async def verify_token(authorization: Optional[str]) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")

    token = authorization.replace("Bearer ", "").strip()

    import httpx
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "http://registry:8001/auth/verify",
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
    scheduler = DAGScheduler()
    jobs = pipeline_def.jobs

    try:
        execution_order = scheduler.build_and_sort(jobs)
    except Exception as e:
        await update_run_status(run_id, "cycle_failure")
        await slack.notify_resolution_failure(pipeline_name, run_id, str(e))
        return

    runner = JobRunner()

    job_results = {}
    final_status = "succeeded"
    failing_job = None

    for job_name in execution_order:
        job_def = jobs[job_name]

        needs = job_def.needs

        # Skip if dependency failed
        if any(job_results.get(n, {}).get("status") != "succeeded" for n in needs):
            job_results[job_name] = {"status": "skipped"}
            continue

        # Build JobSpec (THIS is what your runner expects)
        script = job_def.to_shell_script()

        spec = JobSpec(
            run_id=run_id,
            step_name=job_name,
            script=script,
            image=job_def.runtime,
            extra_env={}
        )

        result = runner.run(spec)

        job_results[job_name] = {
            "status": "succeeded" if result.exit_code == 0 else "failed",
            "exit_code": result.exit_code,
            "duration_s": result.duration_s
        }

        if result.exit_code != 0:
            final_status = "failed"
            failing_job = job_name
            break

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


# ── DEPENDENCY RESOLUTION ───────────────────────────

async def resolve_dependencies(deps, token):
    import httpx

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "http://registry:8001/resolve",
            json={"dependencies": [d.__dict__ for d in deps]},
            headers={"Authorization": f"Bearer {token}"}
        )

        if resp.status_code != 200:
            raise Exception(resp.text)

        return resp.json()