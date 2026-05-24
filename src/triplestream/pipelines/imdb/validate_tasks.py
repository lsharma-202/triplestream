"""Level 2c: SHACL validation gate — route parts to asserted/ or quarantine/."""

from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from prefect import get_run_logger, task
from rdflib import Literal, Namespace, URIRef
from rdflib.namespace import PROV, RDF, RDFS, XSD

from triplestream.graphs.layout import GraphZone, chunk_manifest_path, zone_dir, work_dir
from triplestream.lineage.context import LineageContext
from triplestream.lineage.emit import record_derivation
from triplestream.lineage.store import validate_fragment_slug, write_provenance_fragment
from triplestream.lineage.uris import graph_entity_uri, validation_report_uri
from triplestream.paths import PROJECT_ROOT
from triplestream.pipelines.imdb.scope import load_imdb_scope
from triplestream.sources.imdb import IMDB_SOURCE
from triplestream.validation.pyshacl_runner import validate_nq_part
from triplestream.validation.registry import resolve_validation_context

PIPELINE = Namespace("http://example.org/ontology/platform/pipeline#")


def _route_part(
    part: Path,
    *,
    transform_id: str,
    reports: Path,
    asserted: Path,
    quarantine: Path,
    source_id: str,
) -> dict[str, Any]:
    result = validate_nq_part(
        part,
        transform_id=transform_id,
        reports_dir=reports,
        source_id=source_id,
    )
    destination = asserted if result.conforms else quarantine
    shutil.move(str(part), str(destination / part.name))
    return {
        "part": part.name,
        "conforms": result.conforms,
        "violations": result.violation_count,
        "warnings": result.warning_count,
        "shacl_report": str(result.report_path) if result.report_path else None,
        "destination": destination.name,
        "shape_files": [str(p.relative_to(PROJECT_ROOT)) for p in result.shape_paths]
        if result.shape_paths
        else [],
    }


@task(name="validate-tsv-graph", retries=0, tags=["imdb", "validate", "shacl", "provenance"])
def validate_tsv_graph(
    transform_id: str,
    batch_dir: str,
    batch_id: str,
    materialize_result: dict[str, Any],
    source_id: str = IMDB_SOURCE.source_id,
) -> dict[str, Any]:
    """Run pyshacl on each work/ N-Quads part; promote conformant parts to asserted/."""
    logger = get_run_logger()
    ctx = LineageContext.from_batch_dir(source_id, batch_dir)
    scope = load_imdb_scope()
    transform = scope.by_id(transform_id)
    if transform is None:
        msg = f"Unknown transform: {transform_id}"
        raise ValueError(msg)

    work = work_dir(ctx.batch_dir, transform.tsv)
    asserted = zone_dir(ctx.batch_dir, GraphZone.ASSERTED, transform.tsv)
    reports = zone_dir(ctx.batch_dir, GraphZone.REPORTS, transform.tsv)
    quarantine = zone_dir(ctx.batch_dir, GraphZone.QUARANTINE, transform.tsv)
    for zone_path in (asserted, reports, quarantine):
        zone_path.mkdir(parents=True, exist_ok=True)

    graph_iri = materialize_result["graph_iri"]
    validation_ctx = resolve_validation_context(transform_id, source_id=source_id)
    started_at = datetime.now(UTC)

    part_results: list[dict[str, Any]] = []
    parts_to_validate = sorted(work.glob("part-*.nq"))
    if not parts_to_validate and materialize_result.get("skipped"):
        parts_to_validate = sorted(asserted.glob("part-*.nq"))

    if parts_to_validate and not materialize_result.get("skipped"):
        for old in asserted.glob("part-*.nq"):
            old.unlink()
        for old in quarantine.glob("part-*.nq"):
            old.unlink()

    for part in parts_to_validate:
        part_results.append(
            _route_part(
                part,
                transform_id=transform_id,
                reports=reports,
                asserted=asserted,
                quarantine=quarantine,
                source_id=source_id,
            )
        )

    if not materialize_result.get("skipped") and part_results and any(r["conforms"] for r in part_results):
        chunks_src = work / "chunks.json"
        if chunks_src.is_file():
            shutil.copy2(chunks_src, chunk_manifest_path(asserted))

    conformant = sum(1 for r in part_results if r["conforms"])
    failed = len(part_results) - conformant
    total_violations = sum(r["violations"] for r in part_results)
    total_warnings = sum(r["warnings"] for r in part_results)

    if not part_results:
        validation_status = "skipped-no-parts"
    elif failed == 0:
        validation_status = "passed" if validation_ctx.shape_paths else "passed-no-shapes"
    elif conformant == 0:
        validation_status = "failed"
    else:
        validation_status = "failed-partial"

    ended_at = datetime.now(UTC)
    report = {
        "transform_id": transform_id,
        "graph_iri": graph_iri,
        "status": validation_status,
        "engine": "pyshacl",
        "validated_at": ended_at.isoformat(),
        "shape_files": [str(p.relative_to(PROJECT_ROOT)) for p in validation_ctx.shape_paths],
        "ontology_files": [str(p.name) for p in validation_ctx.ontology_paths],
        "parts_total": len(part_results),
        "parts_conformant": conformant,
        "parts_quarantined": failed,
        "violations": total_violations,
        "warnings": total_warnings,
        "parts": part_results,
        "asserted_dir": str(asserted.relative_to(ctx.batch_dir)),
        "quarantine_dir": str(quarantine.relative_to(ctx.batch_dir)),
    }
    report_path = reports / "validation.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    input_graph = graph_entity_uri(source_id, batch_id, transform.tsv, GraphZone.WORK)
    output_graph = graph_entity_uri(
        source_id,
        batch_id,
        transform.tsv,
        GraphZone.QUARANTINE if validation_status == "failed" else GraphZone.ASSERTED,
    )
    report_entity = validation_report_uri(source_id, batch_id, transform.tsv)

    slug = validate_fragment_slug(transform_id)
    triples = record_derivation(
        activity=ctx.task_activity,
        agent=ctx.flow_agent,
        input_entity=input_graph,
        output_entity=output_graph,
        transform_spec="validate-tsv-graph@v1",
        started_at=started_at,
        ended_at=ended_at,
        record_count=materialize_result.get("total_triples"),
        input_types=(PROV.Entity, PIPELINE.NamedGraph),
        output_types=(PROV.Entity, PIPELINE.NamedGraph),
        extra_input={
            RDFS.label: Literal(f"work:{transform.id}"),
            PIPELINE.fileName: Literal(transform.tsv),
        },
        extra_output={
            RDFS.label: Literal(f"validated:{transform.id}"),
            PIPELINE.validationStatus: Literal(validation_status),
            PIPELINE.conforms: Literal(
                validation_status.startswith("passed"),
                datatype=XSD.boolean,
            ),
            PIPELINE.violationCount: Literal(total_violations, datatype=XSD.integer),
        },
    )
    triples.add((report_entity, RDF.type, PIPELINE.ValidationReport))
    triples.add((report_entity, RDF.type, PROV.Entity))
    triples.add((report_entity, PROV.wasDerivedFrom, output_graph))
    triples.add((report_entity, PIPELINE.validationStatus, Literal(validation_status)))
    triples.add((report_entity, PIPELINE.fileName, Literal("validation.json")))

    write_provenance_fragment(
        ctx.batch_dir,
        source_id,
        ctx.batch_id,
        slug,
        triples,
        overwrite=True,
    )

    logger.info(
        "Validated %s -> %s (%d/%d parts conformant, %d violations)",
        transform_id,
        validation_status,
        conformant,
        len(part_results),
        total_violations,
    )
    return {
        "transform_id": transform_id,
        "status": validation_status,
        "report_path": str(report_path),
        "asserted_dir": str(asserted.relative_to(ctx.batch_dir)),
        "quarantine_dir": str(quarantine.relative_to(ctx.batch_dir)),
        "parts_conformant": conformant,
        "parts_quarantined": failed,
        "violations": total_violations,
    }


@task(name="finalize-provenance", tags=["imdb", "provenance"])
def finalize_provenance(
    batch_dir: str,
    batch_id: str,
    source_id: str = IMDB_SOURCE.source_id,
) -> str:
    """Consolidate all provenance fragments after materialize + validate."""
    from triplestream.lineage.store import consolidate_provenance_fragments

    path = consolidate_provenance_fragments(Path(batch_dir), source_id, batch_id)
    get_run_logger().info("Final provenance: %s", path)
    return str(path)
