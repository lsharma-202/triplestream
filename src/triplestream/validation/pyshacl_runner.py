"""Run pyshacl on one N-Quads chunk."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyshacl import validate
from rdflib.exceptions import ParserError
from rdflib import Dataset, Graph

from triplestream.validation.registry import (
    ValidationContext,
    load_ontology_graph,
    load_shapes_graph,
    resolve_validation_context,
)


@dataclass(frozen=True, slots=True)
class PartValidationResult:
    part_path: Path
    conforms: bool
    violation_count: int
    warning_count: int
    report_text: str
    report_path: Path | None
    shape_paths: tuple[Path, ...]


def _count_severity(report_graph: Graph, severity_iri: str) -> int:
    sh = "http://www.w3.org/ns/shacl#"
    return sum(
        1
        for _s, _p, o in report_graph.triples((None, f"{sh}resultSeverity", None))
        if str(o) == severity_iri
    )


def validate_nq_part(
    nq_path: Path,
    *,
    transform_id: str,
    reports_dir: Path,
    source_id: str = "imdb",
    validation_context: ValidationContext | None = None,
) -> PartValidationResult:
    """
    Validate one materialized N-Quads part against transform-bound SHACL shapes.

    When no shapes are bound, the part passes (vacuous conformance) — useful for
    transforms like title-ratings that only attach properties without typed nodes.
    """
    ctx = validation_context or resolve_validation_context(transform_id, source_id=source_id)
    reports_dir.mkdir(parents=True, exist_ok=True)

    if not ctx.shape_paths:
        return PartValidationResult(
            part_path=nq_path,
            conforms=True,
            violation_count=0,
            warning_count=0,
            report_text="No SHACL shapes bound for this transform; skipped validation.",
            report_path=None,
            shape_paths=(),
        )

    data = Dataset()
    try:
        data.parse(nq_path, format="nquads")
    except ParserError as exc:
        return PartValidationResult(
            part_path=nq_path,
            conforms=False,
            violation_count=1,
            warning_count=0,
            report_text=f"N-Quads parse error: {exc}",
            report_path=None,
            shape_paths=ctx.shape_paths,
        )

    shapes_graph = load_shapes_graph(ctx.shape_paths)
    ontology_graph = load_ontology_graph(ctx.ontology_paths)

    conforms, report_graph, report_text = validate(
        data,
        shacl_graph=shapes_graph,
        ont_graph=ontology_graph,
        inference="none",
        advanced=True,
        abort_on_first=False,
    )

    sh_violation = "http://www.w3.org/ns/shacl#Violation"
    sh_warning = "http://www.w3.org/ns/shacl#Warning"
    violation_count = _count_severity(report_graph, sh_violation)
    warning_count = _count_severity(report_graph, sh_warning)

    report_path: Path | None = None
    if not conforms or warning_count:
        report_path = reports_dir / f"{nq_path.stem}.shacl.ttl"
        report_graph.serialize(destination=report_path, format="turtle")

    return PartValidationResult(
        part_path=nq_path,
        conforms=bool(conforms),
        violation_count=violation_count,
        warning_count=warning_count,
        report_text=report_text,
        report_path=report_path,
        shape_paths=ctx.shape_paths,
    )
