import hashlib
import json
from typing import Optional
from pathlib import Path

import aiosqlite
from fastapi import (
    FastAPI, UploadFile, File, Form,
    Header, HTTPException, Depends
)
from fastapi.responses import Response, JSONResponse

from storage import ArtifactStorage
from metadata import ArtifactMetadataStore as MetadataStore
from resolver import DependencyResolver
from auth import create_token, verify_token, require_auth, get_db

app = FastAPI(title="Forge Artifact Registry")

DB_PATH = Path("/tmp/artifacts/registry.db")
STORAGE_PATH = Path("/tmp/artifacts/blobs")

storage = ArtifactStorage(STORAGE_PATH)
# Auth uses standalone functions from auth.py


async def get_metadata():
    return MetadataStore(DB_PATH)


@app.on_event("startup")
async def startup():
    STORAGE_PATH.mkdir(parents=True, exist_ok=True)
    MetadataStore(DB_PATH).init_schema()
    get_db()  # ensure tokens table exists


# ── Auth ───────────────────────────────────────────────────────────

async def require_auth(
    authorization: Optional[str] = Header(None)
) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = authorization.replace("Bearer ", "").strip()
    identity = verify_token(token)
    if not identity:
        raise HTTPException(status_code=401, detail="Invalid token")
    return identity


@app.get("/auth/verify")
async def verify_token_endpoint(authorization: Optional[str] = Header(None)):
    """Used by engine to verify tokens."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid token")
    token = authorization.replace("Bearer ", "").strip()
    identity = verify_token(token)
    if not identity:
        raise HTTPException(status_code=401, detail="Invalid token")
    return {"identity": identity}


# ── Upload artifact ────────────────────────────────────────────────

@app.post("/artifacts/{name}/{version}", status_code=201)
async def upload_artifact(
    name: str,
    version: str,
    file: UploadFile = File(...),
    checksum: str = Form(...),
    identity: str = Depends(require_auth),
):
    """
    Upload an artifact.
    - Validates semver version
    - Computes SHA-256 server-side
    - Rejects if client checksum mismatches (400)
    - Rejects if version already exists (409)
    """
    metadata_store = MetadataStore(DB_PATH)

    # Validate semver
    if not is_valid_semver(version):
        raise HTTPException(
            status_code=400,
            detail=f"Version '{version}' is not valid semver (e.g. 1.0.0)"
        )

    # Check immutability — reject duplicate
    existing = await metadata_store.get(name, version)
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"{name}@{version} already exists — versions are immutable"
        )

    # Read file bytes
    file_bytes = await file.read()

    # Compute SHA-256 server-side
    actual_sha256 = hashlib.sha256(file_bytes).hexdigest()

    # Parse client-declared checksum
    # Expected format: "sha256:hexvalue"
    if not checksum.startswith("sha256:"):
        raise HTTPException(
            status_code=400,
            detail="Checksum must be in format sha256:<hex>"
        )
    declared_sha256 = checksum.replace("sha256:", "").strip()

    # Reject if checksums don't match
    if actual_sha256 != declared_sha256:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "checksum_mismatch",
                "declared": declared_sha256,
                "actual": actual_sha256,
            }
        )

    # Store the blob
    storage.store(actual_sha256, file_bytes)

    # Store metadata
    await metadata_store.create(
        name=name,
        version=version,
        sha256=actual_sha256,
        size=len(file_bytes),
        publisher=identity,
        deps=[]
    )

    return {
        "name": name,
        "version": version,
        "sha256": actual_sha256,
        "size": len(file_bytes),
    }


# ── Download artifact ──────────────────────────────────────────────

@app.get("/artifacts/{name}/{version}")
async def download_artifact(name: str, version: str):
    """Download artifact blob."""
    metadata_store = MetadataStore(DB_PATH)
    meta = await metadata_store.get(name, version)

    if not meta:
        raise HTTPException(
            status_code=404,
            detail=f"{name}@{version} not found"
        )

    blob = storage.retrieve(meta["sha256"])
    if not blob:
        raise HTTPException(
            status_code=404,
            detail="Blob not found in storage"
        )

    return Response(
        content=blob,
        media_type="application/octet-stream",
        headers={
            "X-Artifact-SHA256": meta["sha256"],
            "Content-Disposition": f'attachment; filename="{name}-{version}.tar.gz"'
        }
    )


# ── Get artifact metadata ──────────────────────────────────────────

@app.get("/artifacts/{name}/{version}/meta")
async def get_artifact_meta(name: str, version: str):
    metadata_store = MetadataStore(DB_PATH)
    meta = await metadata_store.get(name, version)

    if not meta:
        raise HTTPException(
            status_code=404,
            detail=f"{name}@{version} not found"
        )

    return meta


# ── List versions ──────────────────────────────────────────────────

@app.get("/artifacts/{name}")
async def list_versions(name: str):
    metadata_store = MetadataStore(DB_PATH)
    versions = await metadata_store.list_versions(name)
    return {"name": name, "versions": versions}


# ── Resolve dependencies ───────────────────────────────────────────

@app.post("/resolve")
async def resolve_deps(
    body: dict,
    identity: str = Depends(require_auth)
):
    """
    Resolve dependency constraints to exact versions.
    Returns a lockfile.
    """
    deps = body.get("dependencies", [])
    metadata_store = MetadataStore(DB_PATH)
    resolver = DependencyResolver(metadata_store)

    try:
        lockfile = await resolver.resolve(deps)
        return lockfile
    except Exception as e:
        raise HTTPException(status_code=409, detail=str(e))


# ── Semver validator ───────────────────────────────────────────────

def is_valid_semver(version: str) -> bool:
    """
    Check if version is valid semver: MAJOR.MINOR.PATCH
    Optional pre-release: 1.0.0-alpha.1
    """
    import re
    pattern = r"^\d+\.\d+\.\d+(-[a-zA-Z0-9.]+)?(\+[a-zA-Z0-9.]+)?$"
    return bool(re.match(pattern, version))