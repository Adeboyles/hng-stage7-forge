import json
import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


SEMVER_RE = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")
NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")
SHA256_RE = re.compile(r"^[a-f0-9]{64}$")


class MetadataError(Exception):
    """Base error for artifact metadata operations."""


class DuplicateArtifactError(MetadataError):
    """Raised when a published artifact version already exists."""


class ArtifactNotFoundError(MetadataError, KeyError):
    """Raised when artifact metadata cannot be found."""


class InvalidArtifactNameError(MetadataError, ValueError):
    """Raised when an artifact name is invalid."""


class InvalidVersionError(MetadataError, ValueError):
    """Raised when a version is not strict MAJOR.MINOR.PATCH semver."""


@dataclass(frozen=True)
class ArtifactRecord:
    name: str
    version: str
    sha256: str
    size: int
    publisher: str
    published_at: str
    deps: list[dict[str, str]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "sha256": self.sha256,
            "size": self.size,
            "publisher": self.publisher,
            "published_at": self.published_at,
            "deps": self.deps,
        }


class ArtifactMetadataStore:
    """SQLite-backed artifact metadata store.

    Immutability is enforced by the database-level UNIQUE(name, version)
    constraint, which also handles two concurrent publishers racing for the same
    coordinate.
    """

    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.init_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        return conn

    @contextmanager
    def transaction(self):
        conn = self.connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init_schema(self) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS artifacts (
                  id INTEGER PRIMARY KEY,
                  name TEXT NOT NULL,
                  version TEXT NOT NULL,
                  sha256 TEXT NOT NULL,
                  size INTEGER NOT NULL,
                  publisher TEXT NOT NULL,
                  published_at TEXT NOT NULL,
                  deps TEXT NOT NULL,
                  UNIQUE(name, version)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_artifacts_name ON artifacts(name)"
            )

    def publish(
        self,
        *,
        name: str,
        version: str,
        sha256: str,
        size: int,
        publisher: str,
        deps: Iterable[dict[str, str]] | None = None,
        published_at: str | None = None,
    ) -> ArtifactRecord:
        validate_name(name)
        validate_version(version)
        validate_sha256(sha256)
        validate_size(size)
        validate_publisher(publisher)
        deps_list = normalize_deps(deps or [])
        published_at = published_at or datetime.now(timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )

        try:
            with self.transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO artifacts
                      (name, version, sha256, size, publisher, published_at, deps)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        name,
                        version,
                        sha256,
                        size,
                        publisher,
                        published_at,
                        json.dumps(deps_list, sort_keys=True, separators=(",", ":")),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise DuplicateArtifactError(f"{name}@{version} already exists") from exc

        return ArtifactRecord(
            name=name,
            version=version,
            sha256=sha256,
            size=size,
            publisher=publisher,
            published_at=published_at,
            deps=deps_list,
        )

    def get(self, name: str, version: str) -> ArtifactRecord:
        validate_name(name)
        validate_version(version)
        with self.transaction() as conn:
            row = conn.execute(
                """
                SELECT name, version, sha256, size, publisher, published_at, deps
                FROM artifacts
                WHERE name = ? AND version = ?
                """,
                (name, version),
            ).fetchone()
        if row is None:
            raise ArtifactNotFoundError(f"{name}@{version} not found")
        return row_to_record(row)

    def list_versions(self, name: str) -> list[str]:
        validate_name(name)
        with self.transaction() as conn:
            rows = conn.execute(
                "SELECT version FROM artifacts WHERE name = ?",
                (name,),
            ).fetchall()
        return sorted((row["version"] for row in rows), key=version_sort_key)

    def list_records(self, name: str) -> list[ArtifactRecord]:
        validate_name(name)
        with self.transaction() as conn:
            rows = conn.execute(
                """
                SELECT name, version, sha256, size, publisher, published_at, deps
                FROM artifacts
                WHERE name = ?
                """,
                (name,),
            ).fetchall()
        return sorted((row_to_record(row) for row in rows), key=lambda r: version_sort_key(r.version))


def row_to_record(row: sqlite3.Row) -> ArtifactRecord:
    return ArtifactRecord(
        name=row["name"],
        version=row["version"],
        sha256=row["sha256"],
        size=int(row["size"]),
        publisher=row["publisher"],
        published_at=row["published_at"],
        deps=json.loads(row["deps"]),
    )


def validate_name(name: str) -> None:
    if not isinstance(name, str) or not NAME_RE.fullmatch(name):
        raise InvalidArtifactNameError(
            "artifact name must contain only letters, numbers, '.', '_' or '-'"
        )


def validate_version(version: str) -> None:
    if not isinstance(version, str) or not SEMVER_RE.fullmatch(version):
        raise InvalidVersionError("version must be strict semver MAJOR.MINOR.PATCH")


def validate_sha256(sha256: str) -> None:
    if not isinstance(sha256, str) or not SHA256_RE.fullmatch(sha256):
        raise MetadataError("sha256 must be a lowercase SHA-256 hex string")


def validate_size(size: int) -> None:
    if not isinstance(size, int) or size < 0:
        raise MetadataError("artifact size must be a non-negative integer")


def validate_publisher(publisher: str) -> None:
    if not isinstance(publisher, str) or not publisher.strip():
        raise MetadataError("publisher identity is required")


def normalize_deps(deps: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    normalized = []
    for dep in deps:
        if not isinstance(dep, dict):
            raise MetadataError("dependency entries must be objects")
        name = dep.get("name")
        version = dep.get("version")
        validate_name(name)
        if not isinstance(version, str) or not version.strip():
            raise MetadataError("dependency version constraint is required")
        normalized.append({"name": name, "version": version.strip()})
    return sorted(normalized, key=lambda item: (item["name"], item["version"]))


def version_sort_key(version: str) -> tuple[int, int, int]:
    validate_version(version)
    major, minor, patch = version.split(".")
    return int(major), int(minor), int(patch)
