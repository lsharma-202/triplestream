"""Prepare on-disk named-graph directories for one batch."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from prefect import get_run_logger, task

from triplestream.graphs.layout import GraphZone, ensure_graph_tree, graph_iri, tsv_stem
from triplestream.pipelines.imdb.scope import load_imdb_scope
from triplestream.sources.imdb import IMDB_SOURCE


@task(name="init-graph-layout", tags=["imdb", "graphs"])
def init_graph_layout(
    batch_dir: str,
    batch_id: str,
    only_transforms: list[str] | None = None,
    source_id: str = IMDB_SOURCE.source_id,
) -> dict[str, Any]:
    """
    Create asserted / quarantine / reports directories per enabled TSV transform.

    RDF parts (``part-00000.nq``) are written here in Level 2b; SHACL reports in Level 2c.
    """
    logger = get_run_logger()
    scope = load_imdb_scope()
    transforms = scope.resolve(only_transforms=only_transforms)
    batch_path = Path(batch_dir)

    layout = ensure_graph_tree(batch_path, [t.tsv for t in transforms])
    graph_index = {
        stem: {
            zone: str(graph_iri(source_id, batch_id, GraphZone(zone), f"{stem}.tsv"))
            for zone in GraphZone
        }
        for stem, _paths in layout.items()
    }

    manifest = {
        "source_id": source_id,
        "batch_id": batch_id,
        "initialized_at": datetime.now(UTC).isoformat(),
        "transforms": [
            {
                "id": t.id,
                "tsv": t.tsv,
                "stottr": t.stottr,
                "transform_spec": t.transform_spec,
                "chunk_rows": t.chunk_rows,
                "graph_iri": graph_index[tsv_stem(t.tsv)][GraphZone.ASSERTED],
            }
            for t in transforms
        ],
        "directories": layout,
        "graph_iris": graph_index,
    }

    out_path = batch_path / "graphs" / "layout.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    logger.info(
        "Graph layout ready for %d transforms -> %s",
        len(transforms),
        out_path,
    )
    return {"layout_path": str(out_path), "transform_count": len(transforms)}
