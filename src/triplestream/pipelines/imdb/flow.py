"""
IMDB ingest flow — orchestrates five phase subflows for clear Prefect Gantt lanes.

Run locally:
    uv run neo-imdb ingest
    uv run neo-imdb materialize --only title-ratings
    uv run neo-imdb validate --batch-id b5787408c759d37a
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from prefect import flow, get_run_logger
from prefect.artifacts import create_link_artifact

from triplestream.pipelines.imdb.ingest_tasks import compute_source_fingerprint, list_source_files
from triplestream.pipelines.imdb.scope import load_imdb_scope
from triplestream.pipelines.imdb.subflows import (
    finalize_lineage_subflow,
    generate_triples_subflow,
    prepare_templates_subflow,
    resolve_task_results,
    stage_raw_subflow,
    validate_shacl_subflow,
)
from triplestream.sources.imdb import IMDB_SOURCE


def _resolve_batch(
    batch_id: str | None,
    source_paths: list[Path],
) -> tuple[str, dict[str, str]]:
    fingerprint = compute_source_fingerprint(source_paths)
    resolved_id = batch_id or fingerprint["batch_id"]
    return resolved_id, fingerprint["file_hashes"]


@flow(name="imdb-raw-ingest", log_prints=True)
def imdb_raw_ingest(
    batch_id: str | None = None,
    only_transforms: list[str] | None = None,
    *,
    skip_staging: bool = False,
    skip_templates: bool = False,
    skip_materialize: bool = False,
    skip_validate: bool = False,
) -> dict[str, Any]:
    """
    End-to-end ingest through RDF triple generation and SHACL validation.

    Phase subflows appear as separate rows in the Prefect Gantt chart:
    01-stage-raw → 02-prepare-templates → 03-generate-triples → 04-validate-shacl → 05-finalize-lineage
    """
    logger = get_run_logger()
    scope = load_imdb_scope()
    transforms = scope.resolve(only_transforms=only_transforms)
    source_paths = list_source_files()
    batch_id, file_hashes = _resolve_batch(batch_id, source_paths)

    batch_dir = IMDB_SOURCE.staging_dir / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)
    batch_s = str(batch_dir)

    logger.info(
        "Starting IMDB ingest | batch_id=%s | transforms=%s",
        batch_id,
        [t.id for t in transforms],
    )

    staging_meta: dict[str, Any] = {}
    if not skip_staging:
        _staged, staging_meta = stage_raw_subflow(
            batch_dir=batch_s,
            batch_id=batch_id,
            source_paths=source_paths,
            file_hashes=file_hashes,
        )

    if not skip_templates:
        prepare_templates_subflow(
            batch_dir=batch_s,
            batch_id=batch_id,
            only_transforms=only_transforms,
        )

    materialized: list[Any] = []
    if not skip_materialize:
        materialized = generate_triples_subflow(
            transforms=transforms,
            batch_dir=batch_s,
            batch_id=batch_id,
            file_hashes=file_hashes,
        )

    validated: list[Any] = []
    if not skip_validate and materialized:
        validated = validate_shacl_subflow(
            transforms=transforms,
            batch_dir=batch_s,
            batch_id=batch_id,
            materialized=materialized,
        )

    provenance_path = finalize_lineage_subflow(batch_dir=batch_s, batch_id=batch_id)

    total_triples = sum(r["total_triples"] for r in resolve_task_results(materialized)) if materialized else 0
    summary = {
        "source_id": IMDB_SOURCE.source_id,
        "batch_id": batch_id,
        "batch_dir": batch_s,
        "manifest_path": staging_meta.get("manifest_path"),
        "provenance_path": provenance_path,
        "transforms": resolve_task_results(validated) if validated else [],
        "total_triples": total_triples,
        "file_count": len(source_paths),
    }

    create_link_artifact(
        key=f"imdb-provenance-{batch_id}",
        link=Path(provenance_path).resolve().as_uri(),
        link_text="provenance.trig",
        description=f"PROV-O lineage for IMDB batch {batch_id}",
    )

    logger.info("Ingest complete: %s", summary)
    return summary


if __name__ == "__main__":
    result = imdb_raw_ingest()
    print(f"\nManifest: {result['manifest_path']}")
    print(f"Provenance: {result['provenance_path']}")
    print(f"Total triples: {result['total_triples']:,}")
