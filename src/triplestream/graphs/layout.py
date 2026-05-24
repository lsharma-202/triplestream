"""Named-graph layout on disk and stable graph IRIs for one ingest batch."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path

from rdflib import URIRef

BASE = "http://example.org/"


class GraphZone(StrEnum):
    """Where chunk files land before / after SHACL (Level 2c)."""

    WORK = "work"
    ASSERTED = "asserted"
    QUARANTINE = "quarantine"
    REPORTS = "reports"


def tsv_stem(tsv_name: str) -> str:
    """``name.basics.tsv.gz`` → ``name.basics``."""
    return tsv_name.removesuffix(".gz").removesuffix(".tsv")


def graph_iri(
    source_id: str,
    batch_id: str,
    zone: GraphZone,
    tsv: str,
) -> URIRef:
    """
    One named graph per staged TSV file within a batch snapshot.

    All OTTR templates for the same TSV share this graph (e.g. Person + Profession
    from name.basics.stottr). Cross-file links use stable entity IRIs, not graph merge.
    """
    return URIRef(f"{BASE}graph/{zone}/{source_id}/{batch_id}/{tsv_stem(tsv)}")


def batch_graphs_dir(batch_dir: Path) -> Path:
    return batch_dir / "graphs"


def zone_dir(batch_dir: Path, zone: GraphZone, tsv: str) -> Path:
    return batch_graphs_dir(batch_dir) / zone / tsv_stem(tsv)


def chunk_path(zone_path: Path, part_index: int) -> Path:
    """Streaming N-Quads part file (append-friendly, pyoxigraph/triplestore ready)."""
    return zone_path / f"part-{part_index:05d}.nq"


def chunk_manifest_path(zone_path: Path) -> Path:
    """Per-TSV metadata: row ranges, triple counts, transform keys (for incremental skip)."""
    return zone_path / "chunks.json"


def work_dir(batch_dir: Path, tsv: str) -> Path:
    """Pre-validation N-Quads scratch space (promoted to asserted after SHACL gate)."""
    return batch_graphs_dir(batch_dir) / "work" / tsv_stem(tsv)


def ensure_graph_tree(batch_dir: Path, tsv_names: list[str]) -> dict[str, dict[str, str]]:
    """
    Create asserted / quarantine / reports directories for each TSV stem.

    Returns relative paths for logging and manifest embedding.
    """
    layout: dict[str, dict[str, str]] = {}
    for tsv in tsv_names:
        stem = tsv_stem(tsv)
        paths = {}
        for zone in GraphZone:
            path = zone_dir(batch_dir, zone, tsv)
            path.mkdir(parents=True, exist_ok=True)
            paths[zone] = str(path.relative_to(batch_dir))
        layout[stem] = paths
    return layout
