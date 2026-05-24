"""Load IMDB pipeline scope from config/pipelines/imdb.toml."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from triplestream.paths import CONFIG_DIR

SCOPE_PATH = CONFIG_DIR / "pipelines" / "imdb.toml"


@dataclass(frozen=True, slots=True)
class TransformScope:
    """One TSV → OTTR materialization unit (independently triggerable)."""

    id: str
    tsv: str
    stottr: str | None
    enabled: bool
    transform_spec: str
    chunk_rows: int
    notes: str = ""


@dataclass(frozen=True, slots=True)
class PipelineScope:
    source_id: str
    default_chunk_rows: int
    transforms: tuple[TransformScope, ...]

    @property
    def enabled(self) -> tuple[TransformScope, ...]:
        return tuple(t for t in self.transforms if t.enabled)

    @property
    def pending(self) -> tuple[TransformScope, ...]:
        return tuple(t for t in self.transforms if not t.enabled)

    def by_id(self, transform_id: str) -> TransformScope | None:
        return next((t for t in self.transforms if t.id == transform_id), None)

    def by_tsv(self, tsv_name: str) -> TransformScope | None:
        return next((t for t in self.transforms if t.tsv == tsv_name), None)

    def resolve(self, *, only_transforms: list[str] | None = None) -> tuple[TransformScope, ...]:
        """Enabled transforms, optionally filtered by transform id."""
        items = self.enabled
        if only_transforms:
            allowed = set(only_transforms)
            items = tuple(t for t in items if t.id in allowed)
        return items


@lru_cache(maxsize=1)
def load_imdb_scope(path: Path = SCOPE_PATH) -> PipelineScope:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    transforms = tuple(
        TransformScope(
            id=item["id"],
            tsv=item["tsv"],
            stottr=item["stottr"] or None,
            enabled=item["enabled"],
            transform_spec=item["transform_spec"],
            chunk_rows=int(item.get("chunk_rows", data.get("default_chunk_rows", 100_000))),
            notes=item.get("notes", ""),
        )
        for item in data["transforms"]
    )
    return PipelineScope(
        source_id=data["source_id"],
        default_chunk_rows=int(data.get("default_chunk_rows", 100_000)),
        transforms=transforms,
    )
