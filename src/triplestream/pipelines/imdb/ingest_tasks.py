"""Level 1 ingest tasks: discover, stage, profile, manifest."""

from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl
from prefect import get_run_logger, task
from rdflib import Literal, Namespace
from rdflib.namespace import PROV, RDFS

from triplestream.lineage.context import LineageContext
from triplestream.lineage.emit import record_batch_start, record_derivation
from triplestream.lineage.store import (
    INIT_BATCH_FRAGMENT,
    consolidate_provenance_fragments,
    land_fragment_slug,
    profile_fragment_slug,
    write_provenance_fragment,
)
from triplestream.lineage.uris import (
    data_source_uri,
    ingest_batch_uri,
    profile_report_uri,
    raw_file_uri,
    staged_file_uri,
)
from triplestream.sources.imdb import IMDB_SOURCE, IMDB_TSV_READ_KWARGS, SourceDefinition
from triplestream.staging.fingerprint import source_fingerprint

PIPELINE = Namespace("http://example.org/ontology/platform/pipeline#")


def _load_manifest(batch_dir: Path) -> dict[str, Any] | None:
    manifest_path = batch_dir / "manifest.json"
    if not manifest_path.is_file():
        return None
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _manifest_file_entry(
    manifest: dict[str, Any] | None,
    file_name: str,
) -> dict[str, Any] | None:
    if manifest is None:
        return None
    for entry in manifest.get("files", []):
        if entry.get("file") == file_name:
            return entry
    return None


@task(name="compute-source-fingerprint", tags=["imdb", "extract"])
def compute_source_fingerprint(source_paths: list[str]) -> dict[str, Any]:
    """Hash raw files and derive a stable batch id for this source snapshot."""
    logger = get_run_logger()
    batch_id, file_hashes = source_fingerprint(source_paths)
    logger.info("Source fingerprint batch_id=%s files=%d", batch_id, len(file_hashes))
    return {"batch_id": batch_id, "file_hashes": file_hashes}


@task(name="init-batch-provenance", tags=["imdb", "provenance"])
def init_batch_provenance(
    batch_dir: str,
    source_fingerprint: str,
    source_id: str = IMDB_SOURCE.source_id,
    display_name: str = IMDB_SOURCE.display_name,
) -> str:
    """Register pipeline:DataSource + pipeline:IngestBatch for this flow run."""
    logger = get_run_logger()
    ctx = LineageContext.from_batch_dir(source_id, batch_dir)

    triples = record_batch_start(
        batch_entity=ingest_batch_uri(source_id, ctx.batch_id),
        data_source=data_source_uri(source_id),
        agent=ctx.flow_agent,
        activity=ctx.task_activity,
        source_id=source_id,
        batch_id=ctx.batch_id,
        display_name=display_name,
        source_fingerprint=source_fingerprint,
    )
    fragment = write_provenance_fragment(
        ctx.batch_dir, source_id, ctx.batch_id, INIT_BATCH_FRAGMENT, triples
    )
    logger.info("Batch provenance initialized: %s", fragment)
    return str(fragment)


@task(name="list-source-files", retries=1, tags=["imdb", "extract"])
def list_source_files(source: SourceDefinition = IMDB_SOURCE) -> list[str]:
    """Discover raw files on disk."""
    logger = get_run_logger()
    raw_dir = source.raw_dir

    if not raw_dir.is_dir():
        raise FileNotFoundError(f"Raw directory missing: {raw_dir}")

    found = sorted(p.name for p in raw_dir.glob("*.tsv.gz"))
    missing = [name for name in source.expected_files if name not in found]
    if missing:
        raise FileNotFoundError(f"Missing expected IMDB files in {raw_dir}: {missing}")

    paths = [str(raw_dir / name) for name in found]
    logger.info("Discovered %d files in %s", len(paths), raw_dir)
    return paths


@task(name="land-raw-file", retries=2, tags=["imdb", "extract", "provenance"])
def land_raw_file(
    source_path: str,
    content_hash: str,
    batch_dir: str,
    source_id: str = IMDB_SOURCE.source_id,
) -> str:
    """Copy raw bytes into staging when content hash changed; record PROV-O."""
    logger = get_run_logger()
    ctx = LineageContext.from_batch_dir(source_id, batch_dir)
    slug = land_fragment_slug(Path(source_path).name)

    src = Path(source_path)
    dest = ctx.batch_dir / "raw" / src.name
    dest.parent.mkdir(parents=True, exist_ok=True)
    fragment_path = ctx.batch_dir / "provenance" / "fragments" / f"{slug}.trig"

    manifest = _load_manifest(ctx.batch_dir)
    prior = _manifest_file_entry(manifest, src.name)
    if (
        dest.is_file()
        and prior is not None
        and prior.get("content_hash") == content_hash
        and fragment_path.is_file()
    ):
        logger.info("Skipped land for unchanged file %s", src.name)
        return str(dest)

    started_at = datetime.now(UTC)
    if not dest.is_file() or prior is None or prior.get("content_hash") != content_hash:
        shutil.copy2(src, dest)
    ended_at = datetime.now(UTC)

    hash_literal = Literal(content_hash)
    triples = record_derivation(
        activity=ctx.task_activity,
        agent=ctx.flow_agent,
        input_entity=raw_file_uri(source_id, src.name),
        output_entity=staged_file_uri(source_id, ctx.batch_id, src.name),
        transform_spec="land-raw-file@v1",
        started_at=started_at,
        ended_at=ended_at,
        output_types=(PROV.Entity, PIPELINE.StagedFile),
        extra_input={
            PIPELINE.fileName: Literal(src.name),
            PIPELINE.contentHash: hash_literal,
        },
        extra_output={
            PIPELINE.fileName: Literal(src.name),
            PIPELINE.contentHash: hash_literal,
            RDFS.label: Literal(f"{source_id}/{ctx.batch_id}/raw/{src.name}"),
        },
    )
    write_provenance_fragment(
        ctx.batch_dir, source_id, ctx.batch_id, slug, triples, overwrite=True
    )
    logger.info("Landed %s -> %s (%d bytes)", src.name, dest, dest.stat().st_size)
    return str(dest)


@task(name="profile-tsv-file", retries=1, tags=["imdb", "profile", "provenance"])
def profile_tsv_file(
    staged_path: str,
    content_hash: str,
    batch_dir: str,
    source_id: str = IMDB_SOURCE.source_id,
) -> dict[str, Any]:
    """Scan a gzipped TSV for structural metadata + PROV-O."""
    logger = get_run_logger()
    ctx = LineageContext.from_batch_dir(source_id, batch_dir)
    path = Path(staged_path)
    slug = profile_fragment_slug(path.name)
    fragment_path = ctx.batch_dir / "provenance" / "fragments" / f"{slug}.trig"

    manifest = _load_manifest(ctx.batch_dir)
    prior = _manifest_file_entry(manifest, path.name)
    if (
        prior is not None
        and prior.get("content_hash") == content_hash
        and fragment_path.is_file()
        and prior.get("row_count") is not None
    ):
        logger.info("Skipped profile for unchanged file %s", path.name)
        return prior

    started_at = datetime.now(UTC)
    lazy = pl.scan_csv(
        path,
        **IMDB_TSV_READ_KWARGS,
    )
    row_count = lazy.select(pl.len()).collect().item()
    columns = lazy.collect_schema().names()
    ended_at = datetime.now(UTC)

    hash_literal = Literal(content_hash)
    triples = record_derivation(
        activity=ctx.task_activity,
        agent=ctx.flow_agent,
        input_entity=staged_file_uri(source_id, ctx.batch_id, path.name),
        output_entity=profile_report_uri(source_id, ctx.batch_id, path.name),
        transform_spec="profile-tsv-file@v1",
        started_at=started_at,
        ended_at=ended_at,
        record_count=row_count,
        output_types=(PROV.Entity, PIPELINE.ProfileReport),
        extra_input={
            PIPELINE.fileName: Literal(path.name),
            PIPELINE.contentHash: hash_literal,
        },
        extra_output={
            PIPELINE.fileName: Literal(path.name),
            RDFS.label: Literal(f"profile:{path.name}"),
        },
    )
    write_provenance_fragment(
        ctx.batch_dir, source_id, ctx.batch_id, slug, triples, overwrite=True
    )

    profile = {
        "file": path.name,
        "staged_path": str(path),
        "content_hash": content_hash,
        "row_count": row_count,
        "column_count": len(columns),
        "columns": columns,
        "size_bytes": path.stat().st_size,
    }
    logger.info("Profiled %s: %d rows, %d columns", path.name, row_count, len(columns))
    return profile


@task(name="write-ingest-manifest", tags=["imdb", "manifest"])
def write_ingest_manifest(
    batch_dir: str,
    profiles: list[dict[str, Any]],
    source_fingerprint: str,
    source_id: str = IMDB_SOURCE.source_id,
) -> str:
    """Write manifest.json (provenance consolidation happens after OTTR load)."""
    logger = get_run_logger()
    batch_path = Path(batch_dir)
    manifest_path = batch_path / "manifest.json"

    manifest = {
        "source_id": source_id,
        "batch_id": batch_path.name,
        "source_fingerprint": source_fingerprint,
        "ingested_at": datetime.now(UTC).isoformat(),
        "file_count": len(profiles),
        "total_rows": sum(p["row_count"] for p in profiles),
        "files": profiles,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    logger.info("Wrote manifest to %s", manifest_path)
    return str(manifest_path)
