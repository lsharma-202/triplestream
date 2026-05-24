"""Content hashing for idempotent staging and batch identity."""

from __future__ import annotations

import hashlib
from pathlib import Path

CHUNK_SIZE = 1 << 20


def file_sha256(path: Path) -> str:
    """Return the lowercase hex SHA-256 digest of a file's bytes."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def source_fingerprint(file_paths: list[str | Path]) -> tuple[str, dict[str, str]]:
    """
    Derive a stable batch id and per-file hashes from a source snapshot.

    The batch id is the first 16 hex chars of a composite digest over sorted
    ``file_name:sha256`` pairs, so unchanged raw inputs reuse the same staging dir.
    """
    file_hashes = {Path(path).name: file_sha256(Path(path)) for path in sorted(file_paths)}
    composite = "|".join(f"{name}:{digest}" for name, digest in sorted(file_hashes.items()))
    batch_id = hashlib.sha256(composite.encode()).hexdigest()[:16]
    return batch_id, file_hashes
