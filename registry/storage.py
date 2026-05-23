import hashlib
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO


DEFAULT_ARTIFACT_ROOT = "/data/artifacts"
SHA256_RE = re.compile(r"^[a-f0-9]{64}$")


class StorageError(Exception):
    """Base error for artifact blob storage."""


class InvalidDigestError(StorageError, ValueError):
    """Raised when a digest is not a lowercase SHA-256 hex value."""


class ChecksumMismatchError(StorageError, ValueError):
    """Raised when uploaded bytes do not match the declared checksum."""

    def __init__(self, expected: str, actual: str):
        super().__init__(
            f"checksum mismatch: expected sha256:{expected}, actual sha256:{actual}"
        )
        self.expected = expected
        self.actual = actual


class BlobNotFoundError(StorageError, FileNotFoundError):
    """Raised when a blob digest does not exist in storage."""


@dataclass(frozen=True)
class StoredBlob:
    sha256: str
    size: int
    path: Path


class ArtifactStorage:
    """Content-addressable blob storage using SHA-256 paths."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def store(self, fileobj: BinaryIO) -> StoredBlob:
        """Store bytes from a binary file-like object and return its digest.

        The file is first copied to a temporary file while the hash is computed,
        then atomically moved into its content-addressed location. Existing blobs
        are reused, so identical package contents share one file on disk.
        """
        return self._store(fileobj)

    def store_verified(self, fileobj: BinaryIO, declared_checksum: str) -> StoredBlob:
        expected = parse_declared_checksum(declared_checksum)
        return self._store(fileobj, expected_sha256=expected)

    def _store(self, fileobj: BinaryIO, expected_sha256: str | None = None) -> StoredBlob:
        digest = hashlib.sha256()
        size = 0
        tmp_path = None

        with tempfile.NamedTemporaryFile(
            dir=self.root, prefix=".upload-", delete=False
        ) as tmp:
            tmp_path = Path(tmp.name)
            while True:
                chunk = fileobj.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
                size += len(chunk)
                tmp.write(chunk)
            tmp.flush()
            os.fsync(tmp.fileno())

        sha256 = digest.hexdigest()
        if expected_sha256 is not None and sha256 != expected_sha256:
            tmp_path.unlink()
            raise ChecksumMismatchError(expected=expected_sha256, actual=sha256)

        final_path = self.path_for(sha256)
        final_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            if final_path.exists():
                tmp_path.unlink()
            else:
                os.replace(tmp_path, final_path)
        finally:
            if tmp_path is not None and tmp_path.exists():
                tmp_path.unlink()

        return StoredBlob(sha256=sha256, size=size, path=final_path)

    def store_bytes(self, data: bytes) -> StoredBlob:
        from io import BytesIO

        return self.store(BytesIO(data))

    def retrieve(self, sha256: str) -> bytes:
        with self.open(sha256) as f:
            return f.read()

    def open(self, sha256: str) -> BinaryIO:
        path = self.retrieve_path(sha256)
        return path.open("rb")

    def retrieve_path(self, sha256: str) -> Path:
        path = self.path_for(sha256)
        if not path.exists():
            raise BlobNotFoundError(f"blob not found: {sha256}")
        return path

    def exists(self, sha256: str) -> bool:
        return self.path_for(sha256).exists()

    def path_for(self, sha256: str) -> Path:
        self._validate_sha256(sha256)
        return self.root / sha256[:2] / sha256

    @staticmethod
    def _validate_sha256(sha256: str) -> None:
        if not SHA256_RE.fullmatch(sha256):
            raise InvalidDigestError("digest must be a lowercase SHA-256 hex string")


def parse_declared_checksum(value: str) -> str:
    if not isinstance(value, str) or not value.startswith("sha256:"):
        raise InvalidDigestError("checksum must use format sha256:<hex>")
    sha256 = value.removeprefix("sha256:")
    ArtifactStorage._validate_sha256(sha256)
    return sha256


def store(file_bytes: bytes, root: str | Path = DEFAULT_ARTIFACT_ROOT) -> str:
    """Task-spec convenience helper: store bytes and return the SHA-256 digest."""
    return ArtifactStorage(root).store_bytes(file_bytes).sha256


def retrieve(sha256: str, root: str | Path = DEFAULT_ARTIFACT_ROOT) -> bytes:
    """Task-spec convenience helper: retrieve blob bytes by SHA-256 digest."""
    return ArtifactStorage(root).retrieve(sha256)
