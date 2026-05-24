"""Resolve SHACL shape graphs and ontology imports for a pipeline transform."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from rdflib import Graph, URIRef

from triplestream.paths import CONFIG_DIR, PROJECT_ROOT, SHAPES_DIR
PIPELINE = URIRef("http://example.org/ontology/platform/pipeline#")
APPLIES_TO_TRANSFORM = URIRef("http://example.org/ontology/platform/pipeline#appliesToTransform")

BINDINGS_PATH = CONFIG_DIR / "shapes" / "imdb.toml"


@dataclass(frozen=True, slots=True)
class ShapeBinding:
    transform_id: str
    shape_paths: tuple[Path, ...]


@dataclass(frozen=True, slots=True)
class ValidationContext:
    """Everything pyshacl needs for one transform."""

    transform_id: str
    shape_paths: tuple[Path, ...]
    ontology_paths: tuple[Path, ...]


def _repo_path(relative: str) -> Path:
    path = PROJECT_ROOT / relative
    if not path.is_file():
        msg = f"Validation resource not found: {path}"
        raise FileNotFoundError(msg)
    return path


@lru_cache(maxsize=1)
def _load_bindings_config(path: Path = BINDINGS_PATH) -> dict:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def default_ontology_paths(source_id: str = "imdb") -> tuple[Path, ...]:
    data = _load_bindings_config()
    if data.get("source_id") != source_id:
        msg = f"No shape bindings for source {source_id!r}"
        raise ValueError(msg)
    return tuple(_repo_path(item) for item in data["ontology"])


def _discover_shapes_for_transform(transform_id: str) -> tuple[Path, ...]:
    """Scan shapes/**/*.ttl for pipeline:appliesToTransform annotations."""
    discovered: list[Path] = []
    for shape_file in sorted(SHAPES_DIR.rglob("*.ttl")):
        graph = Graph()
        graph.parse(shape_file, format="turtle")
        for _subject, _predicate, literal in graph.triples((None, APPLIES_TO_TRANSFORM, None)):
            if str(literal) == transform_id:
                discovered.append(shape_file)
                break
    return tuple(discovered)


def resolve_validation_context(
    transform_id: str,
    *,
    source_id: str = "imdb",
) -> ValidationContext:
    """
    Merge explicit config bindings with optional annotation discovery.

    Explicit entries in config/shapes/imdb.toml win; discovered shapes are appended
    when ``discover_annotations = true``.
    """
    data = _load_bindings_config()
    explicit: list[Path] = []
    for binding in data.get("bindings", []):
        if binding["transform_id"] == transform_id:
            explicit = [_repo_path(item) for item in binding.get("shapes", [])]
            break

    shape_paths = tuple(dict.fromkeys(explicit))  # preserve order, dedupe
    if data.get("discover_annotations", False):
        merged = list(shape_paths) + [
            p for p in _discover_shapes_for_transform(transform_id) if p not in shape_paths
        ]
        shape_paths = tuple(merged)

    return ValidationContext(
        transform_id=transform_id,
        shape_paths=shape_paths,
        ontology_paths=default_ontology_paths(source_id),
    )


def load_shapes_graph(shape_paths: tuple[Path, ...]) -> Graph:
    graph = Graph()
    for path in shape_paths:
        graph.parse(path, format="turtle")
    return graph


def load_ontology_graph(ontology_paths: tuple[Path, ...]) -> Graph:
    graph = Graph()
    for path in ontology_paths:
        graph.parse(path, format="turtle")
    return graph


def list_configured_transforms(source_id: str = "imdb") -> tuple[str, ...]:
    data = _load_bindings_config()
    if data.get("source_id") != source_id:
        return ()
    return tuple(item["transform_id"] for item in data.get("bindings", []))
