"""Map staged TSV files to stOTTR templates via pipeline scope config."""

from __future__ import annotations

from triplestream.pipelines.imdb.scope import PipelineScope, TransformScope, load_imdb_scope


def enabled_stottr_map(
    scope: PipelineScope | None = None,
    *,
    only_transforms: list[str] | None = None,
) -> dict[str, str]:
    scope = scope or load_imdb_scope()
    transforms = scope.resolve(only_transforms=only_transforms)
    return {t.tsv: t.stottr for t in transforms if t.stottr}


def pending_tsv(scope: PipelineScope | None = None) -> tuple[str, ...]:
    scope = scope or load_imdb_scope()
    return tuple(t.tsv for t in scope.pending)


def transform_for_tsv(tsv_name: str, scope: PipelineScope | None = None) -> TransformScope | None:
    scope = scope or load_imdb_scope()
    return scope.by_tsv(tsv_name)
