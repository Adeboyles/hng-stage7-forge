from __future__ import annotations

import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import Response

try:
    from .auth import get_db, load_config, require_auth, verify_token
    from .metadata import (
        ArtifactMetadataStore as MetadataStore,
        ArtifactNotFoundError,
        DuplicateArtifactError,
        InvalidVersionError,
        MetadataError,
    )
    from .resolver import (
        DependencyCycleError,
        DependencyResolver,
        InvalidConstraintError,
        PackageNotFoundError,
        ResolutionConflictError,
    )
    from .storage import (
        ArtifactStorage,
        BlobNotFoundError,
        ChecksumMismatchError,
        InvalidDigestError,
    )
except (
    ImportError
):  # pragma: no cover - supports running as `uvicorn main:app` from /app
    from auth import get_db, load_config, require_auth, verify_token
    from metadata import (
        ArtifactMetadataStore as MetadataStore,
        ArtifactNotFoundError,
        DuplicateArtifactError,
        InvalidVersionError,
        MetadataError,
    )
    from resolver import (
        DependencyCycleError,
        DependencyResolver,
        InvalidConstraintError,
        PackageNotFoundError,
        ResolutionConflictError,
    )
    from storage import (
        ArtifactStorage,
        BlobNotFoundError,
        ChecksumMismatchError,
        InvalidDigestError,
    )

REGISTRY_CONFIG = load_config().get("registry", {})
DB_PATH = Path(REGISTRY_CONFIG.get("db_path", "/data/registry.db"))
STORAGE_PATH = Path(REGISTRY_CONFIG.get("storage_path", "/data/artifacts"))

storage = ArtifactStorage(STORAGE_PATH)


def get_metadata_store() -> MetadataStore:
    """Create a metadata store bound to the configured database path."""
    return MetadataStore(DB_PATH)


def get_resolver() -> DependencyResolver:
    """Create a dependency resolver over the configured metadata store."""
    return DependencyResolver(get_metadata_store())


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Initialize registry storage and auth/metadata tables on app startup."""
    STORAGE_PATH.mkdir(parents=True, exist_ok=True)
    get_metadata_store().init_schema()
    get_db().close()
    yield


app = FastAPI(title="Forge Artifact Registry", lifespan=lifespan)


@app.get("/auth/verify")
async def verify_token_endpoint(
    authorization: Optional[str] = Header(None),
) -> dict[str, str]:
    """ "Used by engine to verify tokens."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid token")

    token = authorization.removeprefix("Bearer ").strip()
    identity = verify_token(token)
    if identity is None:
        raise HTTPException(status_code=401, detail="Invalid token")
    return {"identity": identity}


@app.post("/artifacts/{name}/{version}", status_code=201)
async def upload_artifact(
    name: str,
    version: str,
    file: UploadFile = File(...),
    checksum: str = Form(...),
    deps: Optional[str] = Form(None),
    identity: str = Depends(require_auth),
) -> dict[str, object]:
    """Upload an immutable artifact and persist its metadata."""
    try:
        file.file.seek(0)
        stored = storage.store_verified(file.file, checksum)
        record = get_metadata_store().publish(
            name=name,
            version=version,
            sha256=stored.sha256,
            size=stored.size,
            publisher=identity,
            deps=_parse_deps_form(deps),
        )
    except DuplicateArtifactError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ChecksumMismatchError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "checksum_mismatch",
                "declared": exc.expected,
                "actual": exc.actual,
            },
        ) from exc
    except (InvalidDigestError, InvalidVersionError, MetadataError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return record.to_dict()


@app.get("/artifacts/{name}/{version}")
async def download_artifact(name: str, version: str) -> Response:
    """Download an artifact blob and expose its checksum header."""
    try:
        meta = get_metadata_store().get(name, version)
        blob = storage.retrieve(meta.sha256)
    except ArtifactNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except BlobNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (InvalidVersionError, MetadataError, InvalidDigestError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return Response(
        content=blob,
        media_type="application/octet-stream",
        headers={"X-Artifact-SHA256": meta.sha256},
    )


@app.get("/artifacts/{name}/{version}/meta")
async def get_artifact_meta(name: str, version: str) -> dict[str, object]:
    """Return metadata for one published artifact coordinate."""
    try:
        return get_metadata_store().get(name, version).to_dict()
    except ArtifactNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (InvalidVersionError, MetadataError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/artifacts/{name}")
async def list_versions(name: str) -> dict[str, object]:
    """List all published versions for one package name."""
    try:
        versions = get_metadata_store().list_versions(name)
    except MetadataError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"versions": versions}


@app.post("/resolve")
async def resolve_deps(
    body: dict,
    identity: str = Depends(require_auth),
) -> dict[str, object]:
    """Resolve dependency constraints to an exact lockfile."""
    del identity  # auth is required, but the identity is not yet persisted here
    deps = body.get("dependencies", [])

    try:
        lockfile = get_resolver().resolve(deps)
    except (InvalidConstraintError, MetadataError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (PackageNotFoundError, ResolutionConflictError, DependencyCycleError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    return lockfile.to_dict()


def _parse_deps_form(value: Optional[str]) -> list[dict[str, str]]:
    """Parse optional JSON dependency metadata from multipart form data."""
    if value in (None, ""):
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError("deps must be valid JSON") from exc
    if not isinstance(parsed, list):
        raise ValueError("deps must decode to a JSON array")
    return parsed
