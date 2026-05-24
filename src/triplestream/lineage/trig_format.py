"""Compact TriG serialization with @base and readable instance paths."""

from __future__ import annotations

from pathlib import Path

from rdflib import Dataset

BASE = "http://example.org/"

HEADER = """\
@base <http://example.org/> .
@prefix activities: <activities/> .
@prefix agents: <agents/> .
@prefix data: <data/> .
@prefix graph: <graph/> .
@prefix pipeline: <ontology/platform/pipeline#> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

"""

# Longest match first — only instance paths under example.org/
_RELATIVE = (
    (f"<{BASE}activities/", "<activities/"),
    (f"<{BASE}agents/", "<agents/"),
    (f"<{BASE}graph/", "<graph/"),
    (f"<{BASE}data/", "<data/"),
)


def serialize_trig(dataset: Dataset) -> str:
    """Serialize a Dataset to TriG with @base and compact instance IRIs."""
    body = dataset.serialize(format="trig")
    for absolute, relative in _RELATIVE:
        body = body.replace(absolute, relative)
    lines = [line for line in body.splitlines() if not line.startswith("@prefix ")]
    body = "\n".join(line for line in lines if line.strip()).strip()
    return f"{HEADER}{body}\n"


def write_trig(dataset: Dataset, path: Path) -> None:
    path.write_text(serialize_trig(dataset), encoding="utf-8")
