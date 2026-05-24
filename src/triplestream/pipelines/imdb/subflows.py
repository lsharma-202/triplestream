"""Prefect subflows — one swim lane per pipeline phase for clearer Gantt charts."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from prefect import flow

from triplestream.pipelines.imdb.graph_tasks import init_graph_layout
from triplestream.pipelines.imdb.ingest_tasks import (
    compute_source_fingerprint,
    init_batch_provenance,
    land_raw_file,
    list_source_files,
    profile_tsv_file,
    write_ingest_manifest,
)
from triplestream.pipelines.imdb.materialize_tasks import materialize_tsv
from triplestream.pipelines.imdb.ottr_tasks import load_ottr_templates
from triplestream.pipelines.imdb.scope import TransformScope
from triplestream.pipelines.imdb.validate_tasks import finalize_provenance, validate_tsv_graph
from triplestream.sources.imdb import IMDB_SOURCE


@flow(name="imdb-01-stage-raw", log_prints=True)
def stage_raw_subflow(
    batch_dir: str,
    batch_id: str,
    source_paths: list[Path],
    file_hashes: dict[str, str],
) -> tuple[list[str], dict[str, Any]]:
    """Level 1: fingerprint batch, land TSV copies, profile structure."""
    init_batch_provenance(
        batch_dir=batch_dir,
        source_id=IMDB_SOURCE.source_id,
        source_fingerprint=batch_id,
    )

    content_hashes = [file_hashes[Path(path).name] for path in source_paths]
    n = len(source_paths)
    batch_s = batch_dir
    source_s = IMDB_SOURCE.source_id

    staged_paths = land_raw_file.map(
        source_path=source_paths,
        content_hash=content_hashes,
        batch_dir=[batch_s] * n,
        source_id=[source_s] * n,
    )
    profiles = profile_tsv_file.map(
        staged_path=staged_paths,
        content_hash=content_hashes,
        batch_dir=[batch_s] * n,
        source_id=[source_s] * n,
    )
    manifest_path = write_ingest_manifest(
        batch_dir=batch_s,
        profiles=[p.result() for p in profiles],
        source_fingerprint=batch_id,
    )
    return [str(p.result()) for p in staged_paths], {
        "manifest_path": manifest_path,
        "staged_paths": [str(p.result()) for p in staged_paths],
    }


@flow(name="imdb-02-prepare-templates", log_prints=True)
def prepare_templates_subflow(
    batch_dir: str,
    batch_id: str,
    only_transforms: list[str] | None,
) -> dict[str, Any]:
    """Level 2a: load OTTR templates and initialize named-graph layout."""
    ottr = load_ottr_templates(batch_dir=batch_dir, only_transforms=only_transforms)
    layout = init_graph_layout(
        batch_dir=batch_dir,
        batch_id=batch_id,
        only_transforms=only_transforms,
    )
    return {"ottr": ottr, "layout": layout}


@flow(name="imdb-03-generate-triples", log_prints=True)
def generate_triples_subflow(
    transforms: tuple[TransformScope, ...],
    batch_dir: str,
    batch_id: str,
    file_hashes: dict[str, str],
) -> list[Any]:
    """Level 2b: Polars streaming → N-Quads under graphs/work/."""
    n = len(transforms)
    batch_s = batch_dir
    source_s = IMDB_SOURCE.source_id
    return materialize_tsv.map(
        transform_id=[t.id for t in transforms],
        batch_dir=[batch_s] * n,
        batch_id=[batch_id] * n,
        content_hash=[file_hashes[t.tsv] for t in transforms],
        source_id=[source_s] * n,
    )


def resolve_task_results(items: list[Any]) -> list[Any]:
    resolved: list[Any] = []
    for item in items:
        resolved.append(item.result() if hasattr(item, "result") else item)
    return resolved


@flow(name="imdb-04-validate-shacl", log_prints=True)
def validate_shacl_subflow(
    transforms: tuple[TransformScope, ...],
    batch_dir: str,
    batch_id: str,
    materialized: list[Any],
) -> list[Any]:
    """Level 2c: pyshacl gate — promote conformant parts to graphs/asserted/."""
    n = len(transforms)
    batch_s = batch_dir
    source_s = IMDB_SOURCE.source_id
    return validate_tsv_graph.map(
        transform_id=[t.id for t in transforms],
        batch_dir=[batch_s] * n,
        batch_id=[batch_id] * n,
        materialize_result=resolve_task_results(materialized),
        source_id=[source_s] * n,
    )


@flow(name="imdb-05-finalize-lineage", log_prints=True)
def finalize_lineage_subflow(batch_dir: str, batch_id: str) -> str:
    """Merge PROV-O fragments into provenance/provenance.trig."""
    return finalize_provenance(batch_dir=batch_dir, batch_id=batch_id)
