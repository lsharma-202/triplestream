"""Batch file materialization into N-Quads part files."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

import polars as pl

from triplestream.graphs.layout import chunk_path, tsv_stem, work_dir
from triplestream.materialize.imdb import get_mapper
from triplestream.materialize.nq import NQuadWriter
from triplestream.pipelines.imdb.scope import TransformScope
from triplestream.sources.imdb import IMDB_TSV_READ_KWARGS


@dataclass(frozen=True, slots=True)
class PartStats:
    part_index: int
    path: str
    rows: int
    triples: int


@dataclass(frozen=True, slots=True)
class MaterializeStats:
    transform_id: str
    graph_iri: str
    input_content_hash: str
    transform_spec: str
    total_rows: int
    total_triples: int
    parts: tuple[PartStats, ...]
    skipped: bool = False


def _chunks_manifest(work: Path) -> dict | None:
    path = work / "chunks.json"
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _should_skip(work: Path, content_hash: str, transform_spec: str) -> bool:
    manifest = _chunks_manifest(work)
    if manifest is None:
        return False
    if manifest.get("input_content_hash") != content_hash:
        return False
    if manifest.get("transform_spec") != transform_spec:
        return False
    return all(Path(part["path"]).is_file() for part in manifest.get("parts", []))


def materialize_transform(
    *,
    staged_path: Path,
    transform: TransformScope,
    graph_iri: str,
    content_hash: str,
    batch_dir: Path,
) -> MaterializeStats:
    """Stream staged TSV batches into N-Quads under graphs/work/{tsv_stem}/."""
    work = work_dir(batch_dir, transform.tsv)
    work.mkdir(parents=True, exist_ok=True)

    if _should_skip(work, content_hash, transform.transform_spec):
        manifest = _chunks_manifest(work)
        return MaterializeStats(
            transform_id=transform.id,
            graph_iri=graph_iri,
            input_content_hash=content_hash,
            transform_spec=transform.transform_spec,
            total_rows=int(manifest["total_rows"]),
            total_triples=int(manifest["total_triples"]),
            parts=tuple(PartStats(**p) for p in manifest["parts"]),
            skipped=True,
        )

    for old_part in work.glob("part-*.nq"):
        old_part.unlink()

    mapper = get_mapper(transform.id)
    reader = pl.read_csv_batched(
        staged_path,
        batch_size=transform.chunk_rows,
        **IMDB_TSV_READ_KWARGS,
    )

    parts: list[PartStats] = []
    total_rows = 0
    total_triples = 0
    part_index = 0

    while batches := reader.next_batches(1):
        df = batches[0]
        if df.is_empty():
            continue
        out_path = chunk_path(work, part_index)
        with out_path.open("w", encoding="utf-8") as handle:
            writer = NQuadWriter(graph_iri, handle)
            mapper(df, writer)
        row_count = df.height
        triple_count = writer.triple_count
        parts.append(
            PartStats(
                part_index=part_index,
                path=str(out_path.relative_to(batch_dir)),
                rows=row_count,
                triples=triple_count,
            )
        )
        total_rows += row_count
        total_triples += triple_count
        part_index += 1

    manifest = {
        "transform_id": transform.id,
        "tsv_stem": tsv_stem(transform.tsv),
        "input_content_hash": content_hash,
        "transform_spec": transform.transform_spec,
        "graph_iri": graph_iri,
        "total_rows": total_rows,
        "total_triples": total_triples,
        "parts": [asdict(part) for part in parts],
    }
    (work / "chunks.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return MaterializeStats(
        transform_id=transform.id,
        graph_iri=graph_iri,
        input_content_hash=content_hash,
        transform_spec=transform.transform_spec,
        total_rows=total_rows,
        total_triples=total_triples,
        parts=tuple(parts),
    )
