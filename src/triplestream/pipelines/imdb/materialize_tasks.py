"""Level 2b: TSV → N-Quads materialization tasks."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from prefect import get_run_logger, task
from rdflib import Literal, Namespace, URIRef
from rdflib.namespace import PROV, RDFS

from triplestream.graphs.layout import GraphZone, graph_iri
from triplestream.lineage.context import LineageContext
from triplestream.lineage.emit import record_derivation
from triplestream.lineage.store import materialize_fragment_slug, write_provenance_fragment
from triplestream.lineage.uris import graph_entity_uri, raw_file_uri
from triplestream.materialize.runner import materialize_transform
from triplestream.pipelines.imdb.scope import load_imdb_scope
from triplestream.sources.imdb import IMDB_SOURCE

PIPELINE = Namespace("http://example.org/ontology/platform/pipeline#")


def _graph_iri_for_transform(batch_dir: Path, batch_id: str, transform_id: str, source_id: str) -> str:
    layout_path = batch_dir / "graphs" / "layout.json"
    layout = json.loads(layout_path.read_text(encoding="utf-8"))
    for entry in layout["transforms"]:
        if entry["id"] == transform_id:
            return entry["graph_iri"]
    scope = load_imdb_scope()
    transform = scope.by_id(transform_id)
    if transform is None:
        msg = f"Unknown transform id: {transform_id}"
        raise ValueError(msg)
    return str(graph_iri(source_id, batch_id, GraphZone.ASSERTED, transform.tsv))


@task(name="materialize-tsv", retries=1, tags=["imdb", "materialize", "provenance"])
def materialize_tsv(
    transform_id: str,
    batch_dir: str,
    batch_id: str,
    content_hash: str,
    source_id: str = IMDB_SOURCE.source_id,
) -> dict[str, Any]:
    """Stream one staged TSV into N-Quads parts under graphs/work/{tsv_stem}/."""
    logger = get_run_logger()
    ctx = LineageContext.from_batch_dir(source_id, batch_dir)
    scope = load_imdb_scope()
    transform = scope.by_id(transform_id)
    if transform is None:
        msg = f"Unknown transform: {transform_id}"
        raise ValueError(msg)

    staged_path = ctx.batch_dir / "raw" / transform.tsv
    graph = _graph_iri_for_transform(ctx.batch_dir, batch_id, transform_id, source_id)
    slug = materialize_fragment_slug(transform_id)

    stats = materialize_transform(
        staged_path=staged_path,
        transform=transform,
        graph_iri=graph,
        content_hash=content_hash,
        batch_dir=ctx.batch_dir,
    )

    if stats.skipped:
        logger.info("Skipped materialize for unchanged transform %s", transform_id)
    else:
        started_at = datetime.now(UTC)
        ended_at = datetime.now(UTC)
        triples = record_derivation(
            activity=ctx.task_activity,
            agent=ctx.flow_agent,
            input_entity=raw_file_uri(source_id, transform.tsv),
            output_entity=graph_entity_uri(source_id, batch_id, transform.tsv, GraphZone.WORK),
            transform_spec=transform.transform_spec,
            started_at=started_at,
            ended_at=ended_at,
            record_count=stats.total_rows,
            output_types=(PROV.Entity, PIPELINE.NamedGraph),
            extra_output={
                RDFS.label: Literal(f"work:{transform.id}"),
                PIPELINE.fileName: Literal(transform.tsv),
            },
        )
        write_provenance_fragment(
            ctx.batch_dir,
            source_id,
            ctx.batch_id,
            slug,
            triples,
            overwrite=True,
        )

    logger.info(
        "Materialized %s: rows=%d triples=%d parts=%d skipped=%s",
        transform_id,
        stats.total_rows,
        stats.total_triples,
        len(stats.parts),
        stats.skipped,
    )
    return {
        "transform_id": transform_id,
        "graph_iri": graph,
        "total_rows": stats.total_rows,
        "total_triples": stats.total_triples,
        "parts": [asdict(part) for part in stats.parts],
        "skipped": stats.skipped,
    }
