import asyncio
import importlib
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from engine.parser import parse_pipeline_text


def test_engine_main_imports_as_package():
    module = importlib.import_module("engine.main")
    assert module.app.title == "Forge CI Engine"


def test_create_run_accepts_valid_pipeline(monkeypatch):
    engine_main = importlib.import_module("engine.main")

    tmp_dir = tempfile.TemporaryDirectory()
    db_path = Path(tmp_dir.name) / "engine.db"
    monkeypatch.setattr(engine_main, "DB_PATH", db_path)
    asyncio.run(engine_main.init_db())

    async def fake_verify_token(_authorization):
        return "good-token"

    async def fake_execute_pipeline(_run_id, _pipeline_def, _token):
        return None

    def fake_create_task(coro):
        coro.close()
        return SimpleNamespace()

    monkeypatch.setattr(engine_main, "verify_token", fake_verify_token)
    monkeypatch.setattr(engine_main, "execute_pipeline", fake_execute_pipeline)
    monkeypatch.setattr(engine_main.asyncio, "create_task", fake_create_task)

    with TestClient(engine_main.app) as client:
        response = client.post(
            "/runs",
            headers={"Authorization": "Bearer good-token"},
            files={
                "pipeline": (
                    "pipeline.yaml",
                    (
                        "name: demo\n"
                        "version: 1.0.0\n"
                        "jobs:\n"
                        "  build:\n"
                        "    runtime: alpine:3.18\n"
                        "    resources:\n"
                        "      cpu: 1.0\n"
                        "      memory: 128Mi\n"
                        "    steps:\n"
                        "      - name: build\n"
                        "        run: echo hi\n"
                    ).encode("utf-8"),
                    "application/yaml",
                )
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert "run_id" in body

    tmp_dir.cleanup()


def test_execute_pipeline_uses_current_scheduler_contract(monkeypatch):
    engine_main = importlib.import_module("engine.main")
    tmp_dir = tempfile.TemporaryDirectory()
    db_path = Path(tmp_dir.name) / "engine.db"
    monkeypatch.setattr(engine_main, "DB_PATH", db_path)
    asyncio.run(engine_main.init_db())
    pipeline = parse_pipeline_text(
        """
name: demo
version: 1.0.0
jobs:
  build:
    runtime: alpine:3.18
    resources:
      cpu: 1.0
      memory: 128Mi
    steps:
      - name: build
        run: echo hi
"""
    )

    updates = []

    async def fake_update_run_status(run_id, status, started_at=None, finished_at=None, jobs=None):
        updates.append(
            {
                "run_id": run_id,
                "status": status,
                "started_at": started_at,
                "finished_at": finished_at,
                "jobs": jobs,
            }
        )

    async def fake_resolve_dependencies(_deps, _token):
        return {}

    async def fake_notify(*_args, **_kwargs):
        return None

    class FakeRunner:
        def run(self, spec):
            assert spec.image == "alpine:3.18"
            assert "echo hi" in spec.script
            return SimpleNamespace(exit_code=0, duration_s=0.1, timed_out=False, oom_killed=False)

    class FakeJobSpec:
        def __init__(self, run_id, step_name, script, image, extra_env):
            self.run_id = run_id
            self.step_name = step_name
            self.script = script
            self.image = image
            self.extra_env = extra_env

    monkeypatch.setattr(engine_main, "update_run_status", fake_update_run_status)
    monkeypatch.setattr(engine_main, "resolve_dependencies", fake_resolve_dependencies)
    monkeypatch.setattr(engine_main.slack, "notify_pipeline_started", fake_notify)
    monkeypatch.setattr(engine_main.slack, "notify_pipeline_succeeded", fake_notify)
    monkeypatch.setattr(engine_main.slack, "notify_pipeline_failed", fake_notify)
    monkeypatch.setattr(engine_main.slack, "notify_resolution_failure", fake_notify)
    monkeypatch.setattr(engine_main, "_load_runner_types", lambda: (FakeRunner, FakeJobSpec))

    asyncio.run(engine_main.execute_pipeline("run-1", pipeline, "good-token"))

    assert updates[0]["status"] == "running"
    assert updates[-1]["status"] == "succeeded"
    assert updates[-1]["jobs"]["build"]["status"] == "succeeded"
    tmp_dir.cleanup()
