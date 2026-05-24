import hashlib
import tempfile
from pathlib import Path

from fastapi.testclient import TestClient

import registry.main as registry_main


def make_client(monkeypatch):
    tmp_dir = tempfile.TemporaryDirectory()
    db_path = Path(tmp_dir.name) / "registry.db"
    storage_path = Path(tmp_dir.name) / "blobs"

    monkeypatch.setattr(registry_main, "DB_PATH", db_path)
    monkeypatch.setattr(registry_main, "STORAGE_PATH", storage_path)
    monkeypatch.setattr(registry_main, "storage", registry_main.ArtifactStorage(storage_path))
    monkeypatch.setattr(
        registry_main,
        "verify_token",
        lambda token: "tester" if token == "good-token" else None,
    )
    registry_main.app.dependency_overrides[registry_main.require_auth] = lambda: "tester"

    client = TestClient(registry_main.app)
    return tmp_dir, client


def auth_headers():
    return {"Authorization": "Bearer good-token"}


def test_upload_download_and_metadata_roundtrip(monkeypatch):
    tmp_dir, client = make_client(monkeypatch)
    with tmp_dir:
        payload = b"artifact-bytes"
        checksum = "sha256:" + hashlib.sha256(payload).hexdigest()

        upload = client.post(
            "/artifacts/lib-core/1.0.0",
            headers=auth_headers(),
            files={"file": ("lib-core.tgz", payload, "application/octet-stream")},
            data={"checksum": checksum},
        )
        assert upload.status_code == 201
        assert upload.json()["sha256"] == checksum.removeprefix("sha256:")

        meta = client.get("/artifacts/lib-core/1.0.0/meta")
        assert meta.status_code == 200
        assert meta.json()["name"] == "lib-core"
        assert meta.json()["version"] == "1.0.0"

        download = client.get("/artifacts/lib-core/1.0.0")
        assert download.status_code == 200
        assert download.content == payload
        assert download.headers["X-Artifact-SHA256"] == checksum.removeprefix("sha256:")


def test_upload_rejects_checksum_mismatch_with_400(monkeypatch):
    tmp_dir, client = make_client(monkeypatch)
    with tmp_dir:
        upload = client.post(
            "/artifacts/lib-core/1.0.0",
            headers=auth_headers(),
            files={"file": ("lib-core.tgz", b"artifact-bytes", "application/octet-stream")},
            data={"checksum": "sha256:" + "0" * 64},
        )

        assert upload.status_code == 400
        body = upload.json()
        assert body["detail"]["error"] == "checksum_mismatch"


def test_upload_rejects_duplicate_coordinate_with_409(monkeypatch):
    tmp_dir, client = make_client(monkeypatch)
    with tmp_dir:
        payload = b"artifact-bytes"
        checksum = "sha256:" + hashlib.sha256(payload).hexdigest()

        first = client.post(
            "/artifacts/lib-core/1.0.0",
            headers=auth_headers(),
            files={"file": ("lib-core.tgz", payload, "application/octet-stream")},
            data={"checksum": checksum},
        )
        second = client.post(
            "/artifacts/lib-core/1.0.0",
            headers=auth_headers(),
            files={"file": ("lib-core.tgz", payload, "application/octet-stream")},
            data={"checksum": checksum},
        )

        assert first.status_code == 201
        assert second.status_code == 409


def test_resolve_returns_lockfile_dict(monkeypatch):
    tmp_dir, client = make_client(monkeypatch)
    with tmp_dir:
        def upload(name, version, deps=None):
            payload = f"{name}-{version}".encode("utf-8")
            checksum = "sha256:" + hashlib.sha256(payload).hexdigest()
            response = client.post(
                f"/artifacts/{name}/{version}",
                headers=auth_headers(),
                files={"file": (f"{name}.tgz", payload, "application/octet-stream")},
                data={"checksum": checksum, "deps": "[]" if deps is None else deps},
            )
            return response

        assert upload("lib-core", "1.0.0").status_code == 201
        assert upload("lib-http", "1.0.0").status_code == 201

        response = client.post(
            "/resolve",
            headers=auth_headers(),
            json={"dependencies": [{"name": "lib-core", "version": "1.0.0"}]},
        )

        assert response.status_code == 200
        body = response.json()
        assert body["resolved"]["lib-core"]["version"] == "1.0.0"
