import asyncio
import hashlib
import importlib
import io
import json
import tarfile
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
        def __init__(self, *args, **kwargs):
            pass

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


def test_execute_pipeline_marks_run_failed_when_runner_init_crashes(monkeypatch):
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

    class BrokenRunner:
        def __init__(self, *args, **kwargs):
            raise NameError("FORGE_NETWORK is not defined")

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
    monkeypatch.setattr(engine_main, "_load_runner_types", lambda: (BrokenRunner, FakeJobSpec))

    asyncio.run(engine_main.execute_pipeline("run-broken", pipeline, "good-token"))

    assert updates[0]["status"] == "running"
    assert updates[-1]["status"] == "failed"
    assert updates[-1]["finished_at"] is not None
    tmp_dir.cleanup()


def test_execute_pipeline_materializes_dependencies_before_running_jobs(monkeypatch):
    engine_main = importlib.import_module("engine.main")
    tmp_dir = tempfile.TemporaryDirectory()
    base = Path(tmp_dir.name)
    db_path = base / "engine.db"
    log_dir = base / "logs"
    workspace_base = base / "workspaces"
    monkeypatch.setattr(engine_main, "DB_PATH", db_path)
    monkeypatch.setattr(engine_main, "LOG_DIR", log_dir)
    monkeypatch.setattr(engine_main, "WORKSPACE_BASE", workspace_base)
    asyncio.run(engine_main.init_db())
    pipeline = parse_pipeline_text(
        """
name: demo
version: 1.0.0
dependencies:
  - name: lib-core
    version: ^1.0.0
jobs:
  build:
    runtime: alpine:3.18
    resources:
      cpu: 1.0
      memory: 128Mi
    steps:
      - name: build
        run: ls deps/lib-core
"""
    )

    archive_bytes = _make_tar_bytes({"src/core.txt": "core"})
    archive_sha = hashlib.sha256(archive_bytes).hexdigest()
    updates = []

    async def fake_update_run_status(run_id, status, started_at=None, finished_at=None, jobs=None):
        updates.append({"status": status, "finished_at": finished_at, "jobs": jobs})

    async def fake_resolve_dependencies(_deps, _token):
        return {"resolved": {"lib-core": {"version": "1.0.0", "sha256": archive_sha}}}

    async def fake_download_artifact(name, version, token):
        assert name == "lib-core"
        assert version == "1.0.0"
        return archive_bytes

    async def fake_notify(*_args, **_kwargs):
        return None

    class FakeRunner:
        def __init__(self, *args, **kwargs):
            pass

        def run(self, spec):
            dep_file = workspace_base / spec.run_id / "deps" / "lib-core" / "src" / "core.txt"
            assert dep_file.exists()
            assert dep_file.read_text(encoding="utf-8") == "core"
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
    monkeypatch.setattr(engine_main, "download_artifact", fake_download_artifact)
    monkeypatch.setattr(engine_main.slack, "notify_pipeline_started", fake_notify)
    monkeypatch.setattr(engine_main.slack, "notify_pipeline_succeeded", fake_notify)
    monkeypatch.setattr(engine_main.slack, "notify_pipeline_failed", fake_notify)
    monkeypatch.setattr(engine_main.slack, "notify_resolution_failure", fake_notify)
    monkeypatch.setattr(engine_main.slack, "notify_integrity_failure", fake_notify)
    monkeypatch.setattr(engine_main, "_load_runner_types", lambda: (FakeRunner, FakeJobSpec))

    asyncio.run(engine_main.execute_pipeline("run-deps", pipeline, "good-token"))

    assert updates[-1]["status"] == "succeeded"
    tmp_dir.cleanup()


def test_execute_pipeline_marks_integrity_failure_on_checksum_mismatch(monkeypatch):
    engine_main = importlib.import_module("engine.main")
    tmp_dir = tempfile.TemporaryDirectory()
    base = Path(tmp_dir.name)
    db_path = base / "engine.db"
    log_dir = base / "logs"
    workspace_base = base / "workspaces"
    monkeypatch.setattr(engine_main, "DB_PATH", db_path)
    monkeypatch.setattr(engine_main, "LOG_DIR", log_dir)
    monkeypatch.setattr(engine_main, "WORKSPACE_BASE", workspace_base)
    asyncio.run(engine_main.init_db())
    pipeline = parse_pipeline_text(
        """
name: demo
version: 1.0.0
dependencies:
  - name: lib-core
    version: ^1.0.0
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

    payload = b"corrupted"
    actual_sha = hashlib.sha256(payload).hexdigest()
    expected_sha = "0" * 64
    updates = []
    integrity_calls = []

    async def fake_update_run_status(run_id, status, started_at=None, finished_at=None, jobs=None):
        updates.append({"status": status, "finished_at": finished_at, "jobs": jobs})

    async def fake_resolve_dependencies(_deps, _token):
        return {"resolved": {"lib-core": {"version": "1.0.0", "sha256": expected_sha}}}

    async def fake_download_artifact(name, version, token):
        return payload

    async def fake_notify(*_args, **_kwargs):
        return None

    async def fake_integrity_notify(artifact, expected, actual, run_id):
        integrity_calls.append((artifact, expected, actual, run_id))

    monkeypatch.setattr(engine_main, "update_run_status", fake_update_run_status)
    monkeypatch.setattr(engine_main, "resolve_dependencies", fake_resolve_dependencies)
    monkeypatch.setattr(engine_main, "download_artifact", fake_download_artifact)
    monkeypatch.setattr(engine_main.slack, "notify_pipeline_started", fake_notify)
    monkeypatch.setattr(engine_main.slack, "notify_pipeline_succeeded", fake_notify)
    monkeypatch.setattr(engine_main.slack, "notify_pipeline_failed", fake_notify)
    monkeypatch.setattr(engine_main.slack, "notify_resolution_failure", fake_notify)
    monkeypatch.setattr(engine_main.slack, "notify_integrity_failure", fake_integrity_notify)
    monkeypatch.setattr(engine_main, "_load_runner_types", lambda: (_raise_runner_should_not_init, object))

    asyncio.run(engine_main.execute_pipeline("run-integrity", pipeline, "good-token"))

    assert updates[-1]["status"] == "integrity_failure"
    assert updates[-1]["finished_at"] is not None
    assert integrity_calls == [("lib-core@1.0.0", expected_sha, actual_sha, "run-integrity")]
    tmp_dir.cleanup()


def test_execute_pipeline_publishes_declared_artifacts_after_success(monkeypatch):
    engine_main = importlib.import_module("engine.main")
    tmp_dir = tempfile.TemporaryDirectory()
    base = Path(tmp_dir.name)
    db_path = base / "engine.db"
    log_dir = base / "logs"
    workspace_base = base / "workspaces"
    monkeypatch.setattr(engine_main, "DB_PATH", db_path)
    monkeypatch.setattr(engine_main, "LOG_DIR", log_dir)
    monkeypatch.setattr(engine_main, "WORKSPACE_BASE", workspace_base)
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
      - name: package
        run: echo package
artifacts:
  - name: demo-lib
    version: 1.0.0
    path: ./out.tar.gz
"""
    )

    updates = []
    published = []

    async def fake_update_run_status(run_id, status, started_at=None, finished_at=None, jobs=None):
        updates.append({"status": status, "finished_at": finished_at, "jobs": jobs})

    async def fake_resolve_dependencies(_deps, _token):
        return {}

    async def fake_publish_artifact(path, name, version, token, **kwargs):
        published.append((path, name, version, token, kwargs.get("deps")))

    async def fake_notify(*_args, **_kwargs):
        return None

    class FakeRunner:
        def __init__(self, *args, **kwargs):
            pass

        def run(self, spec):
            artifact_path = workspace_base / spec.run_id / "out.tar.gz"
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path.write_bytes(b"artifact-bytes")
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
    monkeypatch.setattr(engine_main, "publish_artifact", fake_publish_artifact)
    monkeypatch.setattr(engine_main.slack, "notify_pipeline_started", fake_notify)
    monkeypatch.setattr(engine_main.slack, "notify_pipeline_succeeded", fake_notify)
    monkeypatch.setattr(engine_main.slack, "notify_pipeline_failed", fake_notify)
    monkeypatch.setattr(engine_main.slack, "notify_resolution_failure", fake_notify)
    monkeypatch.setattr(engine_main.slack, "notify_integrity_failure", fake_notify)
    monkeypatch.setattr(engine_main, "_load_runner_types", lambda: (FakeRunner, FakeJobSpec))

    asyncio.run(engine_main.execute_pipeline("run-publish", pipeline, "good-token"))

    assert updates[-1]["status"] == "succeeded"
    assert published == [
        (
            workspace_base / "run-publish" / "out.tar.gz",
            "demo-lib",
            "1.0.0",
            "good-token",
            [],
        )
    ]
    tmp_dir.cleanup()


def test_get_run_returns_status_jobs_and_lockfile_url(monkeypatch):
    engine_main = importlib.import_module("engine.main")

    tmp_dir = tempfile.TemporaryDirectory()
    db_path = Path(tmp_dir.name) / "engine.db"
    monkeypatch.setattr(engine_main, "DB_PATH", db_path)
    asyncio.run(engine_main.init_db())

    async def fake_verify_token(_authorization):
        return "good-token"

    monkeypatch.setattr(engine_main, "verify_token", fake_verify_token)

    asyncio.run(
        _seed_run(
            engine_main,
            run_id="run-123",
            pipeline_name="demo",
            status="running",
            lockfile={"packages": {"lib-core": {"version": "1.0.0"}}},
            jobs={"build": {"status": "running"}},
        )
    )

    with TestClient(engine_main.app) as client:
        response = client.get("/runs/run-123", headers={"Authorization": "Bearer good-token"})

    assert response.status_code == 200
    assert response.json() == {
        "status": "running",
        "jobs": {"build": {"status": "running"}},
        "lockfile_url": "/runs/run-123/lockfile",
    }
    tmp_dir.cleanup()


def test_get_run_returns_404_for_missing_run(monkeypatch):
    engine_main = importlib.import_module("engine.main")

    tmp_dir = tempfile.TemporaryDirectory()
    db_path = Path(tmp_dir.name) / "engine.db"
    monkeypatch.setattr(engine_main, "DB_PATH", db_path)

    async def fake_verify_token(_authorization):
        return "good-token"

    monkeypatch.setattr(engine_main, "verify_token", fake_verify_token)

    with TestClient(engine_main.app) as client:
        response = client.get("/runs/missing-run", headers={"Authorization": "Bearer good-token"})

    assert response.status_code == 404
    assert response.json() == {"detail": "Run not found"}
    tmp_dir.cleanup()


def test_get_lockfile_returns_stored_json(monkeypatch):
    engine_main = importlib.import_module("engine.main")

    tmp_dir = tempfile.TemporaryDirectory()
    db_path = Path(tmp_dir.name) / "engine.db"
    monkeypatch.setattr(engine_main, "DB_PATH", db_path)
    asyncio.run(engine_main.init_db())

    async def fake_verify_token(_authorization):
        return "good-token"

    monkeypatch.setattr(engine_main, "verify_token", fake_verify_token)

    lockfile = {"dependencies": [{"name": "lib-core", "version": "1.0.0"}]}
    asyncio.run(
        _seed_run(
            engine_main,
            run_id="run-456",
            pipeline_name="demo",
            status="succeeded",
            lockfile=lockfile,
            jobs={"build": {"status": "succeeded"}},
        )
    )

    with TestClient(engine_main.app) as client:
        response = client.get("/runs/run-456/lockfile", headers={"Authorization": "Bearer good-token"})

    assert response.status_code == 200
    assert response.json() == lockfile
    tmp_dir.cleanup()


def test_get_lockfile_returns_404_for_missing_run(monkeypatch):
    engine_main = importlib.import_module("engine.main")

    tmp_dir = tempfile.TemporaryDirectory()
    db_path = Path(tmp_dir.name) / "engine.db"
    monkeypatch.setattr(engine_main, "DB_PATH", db_path)

    async def fake_verify_token(_authorization):
        return "good-token"

    monkeypatch.setattr(engine_main, "verify_token", fake_verify_token)

    with TestClient(engine_main.app) as client:
        response = client.get("/runs/missing-run/lockfile", headers={"Authorization": "Bearer good-token"})

    assert response.status_code == 404
    assert response.json() == {"detail": "Run not found"}
    tmp_dir.cleanup()


def test_logs_endpoint_streams_backlog(monkeypatch):
    engine_main = importlib.import_module("engine.main")

    tmp_dir = tempfile.TemporaryDirectory()
    db_path = Path(tmp_dir.name) / "engine.db"
    log_dir = Path(tmp_dir.name) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(engine_main, "DB_PATH", db_path)
    monkeypatch.setattr(engine_main, "LOG_DIR", log_dir)
    asyncio.run(engine_main.init_db())

    async def fake_verify_token(_authorization):
        return "good-token"

    monkeypatch.setattr(engine_main, "verify_token", fake_verify_token)

    asyncio.run(
        _seed_run(
            engine_main,
            run_id="run-789",
            pipeline_name="demo",
            status="running",
            lockfile={},
            jobs={"build": {"status": "running"}},
        )
    )

    (log_dir / "run-789.log").write_text(
        json.dumps({"ts": "2026-05-24T12:00:00.000Z", "job": "build", "line": "hello"}) + "\n",
        encoding="utf-8",
    )

    with TestClient(engine_main.app) as client:
        response = client.get("/runs/run-789/logs?follow=false", headers={"Authorization": "Bearer good-token"})

    assert response.status_code == 200
    assert "text/event-stream" in response.headers["content-type"]
    assert 'data: {"ts": "2026-05-24T12:00:00.000Z", "job": "build", "line": "hello"}' in response.text
    tmp_dir.cleanup()


def test_logs_endpoint_returns_404_for_missing_run(monkeypatch):
    engine_main = importlib.import_module("engine.main")

    tmp_dir = tempfile.TemporaryDirectory()
    db_path = Path(tmp_dir.name) / "engine.db"
    monkeypatch.setattr(engine_main, "DB_PATH", db_path)

    async def fake_verify_token(_authorization):
        return "good-token"

    monkeypatch.setattr(engine_main, "verify_token", fake_verify_token)

    with TestClient(engine_main.app) as client:
        response = client.get("/runs/missing-run/logs?follow=false", headers={"Authorization": "Bearer good-token"})

    assert response.status_code == 404
    assert response.json() == {"detail": "Run not found"}
    tmp_dir.cleanup()


async def _seed_run(engine_main, run_id, pipeline_name, status, lockfile, jobs):
    async with engine_main.aiosqlite.connect(engine_main.DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO runs (id, pipeline_name, status, lockfile, jobs, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                pipeline_name,
                status,
                json.dumps(lockfile),
                json.dumps(jobs),
                "2026-05-24T12:00:00+00:00",
            ),
        )
        await db.commit()


def _make_tar_bytes(files: dict[str, str]) -> bytes:
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w:gz") as archive:
        for path, content in files.items():
            data = content.encode("utf-8")
            info = tarfile.TarInfo(name=path)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return payload.getvalue()


class _raise_runner_should_not_init:
    def __init__(self, *args, **kwargs):
        raise AssertionError("runner should not be initialized on integrity failure")
