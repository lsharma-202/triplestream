"""Level 2a tasks: load OTTR template definitions."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from prefect import get_run_logger, task
from rdflib import Literal, Namespace
from rdflib.namespace import PROV, RDFS

from triplestream.lineage.context import LineageContext
from triplestream.lineage.emit import record_derivation
from triplestream.lineage.store import (
    LOAD_OTTR_FRAGMENT,
    consolidate_provenance_fragments,
    write_provenance_fragment,
)
from triplestream.lineage.uris import ingest_batch_uri, manifest_uri, ottr_catalog_uri
from triplestream.ottr.loader import catalog_to_dict, load_imdb_templates
from triplestream.ottr.registry import pending_tsv
from triplestream.pipelines.imdb.scope import load_imdb_scope
from triplestream.sources.imdb import IMDB_SOURCE

PIPELINE = Namespace("http://example.org/ontology/platform/pipeline#")


@task(name="load-ottr-templates", tags=["imdb", "ottr", "provenance"])
def load_ottr_templates(
    batch_dir: str,
    source_id: str = IMDB_SOURCE.source_id,
    only_transforms: list[str] | None = None,
) -> dict[str, Any]:
    """
    Parse project stOTTR files into pyOTTR and write a batch template catalog.

    Instance expansion (TSV rows → RDF triples) is the next pipeline stage.
    """
    logger = get_run_logger()
    ctx = LineageContext.from_batch_dir(source_id, batch_dir)
    started_at = datetime.now(UTC)

    scope = load_imdb_scope()
    _, catalog = load_imdb_templates(scope=scope, only_transforms=only_transforms)
    catalog_body = catalog_to_dict(catalog, scope=scope)
    catalog_body["loaded_at"] = datetime.now(UTC).isoformat()
    catalog_body["pending_tsv"] = list(pending_tsv(scope))

    catalog_dir = ctx.batch_dir / "templates"
    catalog_dir.mkdir(parents=True, exist_ok=True)
    catalog_path = catalog_dir / "catalog.json"
    catalog_path.write_text(json.dumps(catalog_body, indent=2), encoding="utf-8")

    ended_at = datetime.now(UTC)
    triples = record_derivation(
        activity=ctx.task_activity,
        agent=ctx.flow_agent,
        input_entity=manifest_uri(source_id, ctx.batch_id),
        output_entity=ottr_catalog_uri(source_id, ctx.batch_id),
        transform_spec="load-ottr-templates@v1",
        started_at=started_at,
        ended_at=ended_at,
        output_types=(PROV.Entity, PIPELINE.OttrTemplateCatalog),
        extra_output={
            RDFS.label: Literal(f"ottr-catalog:{ctx.batch_id}"),
            PIPELINE.fileName: Literal("catalog.json"),
        },
    )
    write_provenance_fragment(
        ctx.batch_dir,
        source_id,
        ctx.batch_id,
        LOAD_OTTR_FRAGMENT,
        triples,
        overwrite=True,
    )

    provenance_path = consolidate_provenance_fragments(ctx.batch_dir, source_id, ctx.batch_id)
    logger.info(
        "Loaded %d OTTR templates across %d files -> %s",
        catalog_body["template_count"],
        len(catalog_body["files"]),
        catalog_path,
    )
    logger.info("Consolidated provenance to %s", provenance_path)

    return {
        "catalog_path": str(catalog_path),
        "template_count": catalog_body["template_count"],
        "provenance_path": str(provenance_path),
        "batch_entity": str(ingest_batch_uri(source_id, ctx.batch_id)),
    }
