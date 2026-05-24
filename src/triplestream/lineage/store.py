"""Persist provenance fragments to the data plane (parallel-safe)."""

from __future__ import annotations

from pathlib import Path

from rdflib import Dataset, Graph
from rdflib.namespace import PROV, RDFS, XSD

from triplestream.lineage.emit import PIPELINE
from triplestream.lineage.trig_format import write_trig
from triplestream.lineage.uris import provenance_graph_uri

FRAGMENTS_DIR = "provenance/fragments"
CONSOLIDATED_NAME = "provenance.trig"

INIT_BATCH_FRAGMENT = "00-init-batch"
LOAD_OTTR_FRAGMENT = "30-load-ottr-templates"


def materialize_fragment_slug(transform_id: str) -> str:
    return f"40-materialize-{transform_id}"


def validate_fragment_slug(transform_id: str) -> str:
    return f"50-validate-{transform_id}"


def land_fragment_slug(file_name: str) -> str:
    return f"10-land-{_slug(file_name)}"


def profile_fragment_slug(file_name: str) -> str:
    return f"20-profile-{_slug(file_name)}"


def _slug(value: str) -> str:
    safe = "".join(c if c.isalnum() or c in "._-" else "-" for c in value).strip("-")
    return safe[:80] or "artifact"


def _bind_context(context: Graph) -> None:
    context.bind("pipeline", PIPELINE, override=True)
    context.bind("prov", PROV, override=True)
    context.bind("rdfs", RDFS, override=True)
    context.bind("xsd", XSD, override=True)


def write_provenance_fragment(
    batch_dir: Path,
    source_id: str,
    batch_id: str,
    fragment_slug: str,
    triples: Graph,
    *,
    overwrite: bool = False,
) -> Path:
    """
    Write one task's PROV-O triples into an isolated TriG fragment.

    Parallel ``.map()`` tasks must not append to the same file — one fragment
    per logical step avoids write races. ``fragment_slug`` should be a stable,
    human-readable stem such as ``10-land-title.basics.tsv.gz``.

    When ``overwrite`` is false and the fragment already exists, the existing
    file is returned unchanged (supports idempotent re-runs).
    """
    fragment_dir = batch_dir / FRAGMENTS_DIR
    fragment_dir.mkdir(parents=True, exist_ok=True)
    out_path = fragment_dir / f"{fragment_slug}.trig"

    if out_path.exists() and not overwrite:
        return out_path

    dataset = Dataset()
    context = dataset.graph(provenance_graph_uri(source_id, batch_id))
    _bind_context(context)
    for triple in triples:
        context.add(triple)

    write_trig(dataset, out_path)
    return out_path


def consolidate_provenance_fragments(
    batch_dir: Path,
    source_id: str,
    batch_id: str,
) -> Path:
    """
    Merge step fragments into one ``provenance/provenance.trig`` for querying.

    Individual fragments exist only to keep parallel task writes safe during the
    flow; consolidation produces the single artifact downstream tools expect.
    """
    fragment_dir = batch_dir / FRAGMENTS_DIR
    provenance_dir = batch_dir / "provenance"
    provenance_dir.mkdir(parents=True, exist_ok=True)
    out_path = provenance_dir / CONSOLIDATED_NAME

    dataset = Dataset()
    context = dataset.graph(provenance_graph_uri(source_id, batch_id))
    _bind_context(context)

    if fragment_dir.is_dir():
        for fragment_path in sorted(fragment_dir.glob("*.trig")):
            fragment_ds = Dataset()
            fragment_ds.parse(fragment_path, format="trig")
            for graph in fragment_ds.graphs():
                if graph.identifier and str(graph.identifier) != "urn:x-rdflib:default":
                    for triple in graph:
                        context.add(triple)

    write_trig(dataset, out_path)
    return out_path
