"""CLI entry points for IMDB pipeline phases."""

from __future__ import annotations

import argparse
import sys

from triplestream.pipelines.imdb.flow import imdb_raw_ingest
from triplestream.pipelines.imdb.subflows import (
    finalize_lineage_subflow,
    generate_triples_subflow,
    prepare_templates_subflow,
    resolve_task_results,
    stage_raw_subflow,
    validate_shacl_subflow,
)
from triplestream.pipelines.imdb.ingest_tasks import compute_source_fingerprint, list_source_files
from triplestream.pipelines.imdb.scope import load_imdb_scope
from triplestream.sources.imdb import IMDB_SOURCE


def _parse_only(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [part.strip() for part in value.split(",") if part.strip()]


def cmd_ingest(args: argparse.Namespace) -> int:
    result = imdb_raw_ingest(
        batch_id=args.batch_id,
        only_transforms=_parse_only(args.only),
    )
    print(f"batch_id={result['batch_id']} triples={result['total_triples']:,}")
    print(f"provenance={result['provenance_path']}")
    return 0


def cmd_stage(args: argparse.Namespace) -> int:
    source_paths = list_source_files()
    fingerprint = compute_source_fingerprint(source_paths)
    batch_id = args.batch_id or fingerprint["batch_id"]
    batch_dir = IMDB_SOURCE.staging_dir / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)
    _, meta = stage_raw_subflow(
        batch_dir=str(batch_dir),
        batch_id=batch_id,
        source_paths=source_paths,
        file_hashes=fingerprint["file_hashes"],
    )
    finalize_lineage_subflow(batch_dir=str(batch_dir), batch_id=batch_id)
    print(f"batch_id={batch_id} manifest={meta['manifest_path']}")
    return 0


def cmd_materialize(args: argparse.Namespace) -> int:
    scope = load_imdb_scope()
    transforms = scope.resolve(only_transforms=_parse_only(args.only))
    source_paths = list_source_files()
    fingerprint = compute_source_fingerprint(source_paths)
    batch_id = args.batch_id or fingerprint["batch_id"]
    batch_dir = IMDB_SOURCE.staging_dir / batch_id
    if not batch_dir.is_dir():
        print(f"Batch not found: {batch_dir}", file=sys.stderr)
        return 1

    prepare_templates_subflow(
        batch_dir=str(batch_dir),
        batch_id=batch_id,
        only_transforms=_parse_only(args.only),
    )
    materialized = generate_triples_subflow(
        transforms=transforms,
        batch_dir=str(batch_dir),
        batch_id=batch_id,
        file_hashes=fingerprint["file_hashes"],
    )
    if args.validate:
        validate_shacl_subflow(
            transforms=transforms,
            batch_dir=str(batch_dir),
            batch_id=batch_id,
            materialized=materialized,
        )
    provenance_path = finalize_lineage_subflow(batch_dir=str(batch_dir), batch_id=batch_id)
    total = sum(r["total_triples"] for r in resolve_task_results(materialized))
    print(f"batch_id={batch_id} triples={total:,} provenance={provenance_path}")
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    scope = load_imdb_scope()
    transforms = scope.resolve(only_transforms=_parse_only(args.only))
    source_paths = list_source_files()
    fingerprint = compute_source_fingerprint(source_paths)
    batch_id = args.batch_id or fingerprint["batch_id"]
    batch_dir = IMDB_SOURCE.staging_dir / batch_id
    if not batch_dir.is_dir():
        print(f"Batch not found: {batch_dir}", file=sys.stderr)
        return 1

    # Validation reads existing work/ or asserted/ parts; pass empty materialize stubs.
    from triplestream.pipelines.imdb.materialize_tasks import _graph_iri_for_transform

    stubs = [
        {
            "transform_id": t.id,
            "graph_iri": _graph_iri_for_transform(batch_dir, batch_id, t.id, IMDB_SOURCE.source_id),
            "total_triples": 0,
            "skipped": True,
        }
        for t in transforms
    ]
    class _Stub:
        def __init__(self, payload: dict) -> None:
            self._payload = payload

        def result(self) -> dict:
            return self._payload

    validated = validate_shacl_subflow(
        transforms=transforms,
        batch_dir=str(batch_dir),
        batch_id=batch_id,
        materialized=[_Stub(s) for s in stubs],
    )
    provenance_path = finalize_lineage_subflow(batch_dir=str(batch_dir), batch_id=batch_id)
    for r in resolve_task_results(validated):
        print(f"{r['transform_id']}: {r['status']} violations={r.get('violations', 0)}")
    print(f"provenance={provenance_path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="neo-imdb", description="IMDB ontology-first ingest pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "--batch-id",
            help="Use an existing or forced batch id (default: content fingerprint)",
        )

    p_ingest = sub.add_parser("ingest", help="Full pipeline: stage → templates → triples → SHACL → lineage")
    add_common(p_ingest)
    p_ingest.add_argument("--only", help="Comma-separated transform ids (e.g. title-ratings,name-basics)")
    p_ingest.set_defaults(func=cmd_ingest)

    p_stage = sub.add_parser("stage", help="Level 1 only: land raw TSVs and profile")
    add_common(p_stage)
    p_stage.set_defaults(func=cmd_stage)

    p_mat = sub.add_parser("materialize", help="Level 2b: generate N-Quads for selected transforms")
    add_common(p_mat)
    p_mat.add_argument("--only", help="Comma-separated transform ids")
    p_mat.add_argument("--validate", action="store_true", help="Also run SHACL gate after materialize")
    p_mat.set_defaults(func=cmd_materialize)

    p_val = sub.add_parser("validate", help="Level 2c: SHACL validate existing work/ or asserted/ parts")
    add_common(p_val)
    p_val.add_argument("--only", help="Comma-separated transform ids")
    p_val.set_defaults(func=cmd_validate)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
