"""Load and validate stOTTR templates via pyOTTR."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ottr import OttrGenerator

from triplestream.ottr.registry import enabled_stottr_map, pending_tsv
from triplestream.paths import TEMPLATES_DIR
from triplestream.pipelines.imdb.scope import PipelineScope, load_imdb_scope


@dataclass(frozen=True, slots=True)
class TemplateFileEntry:
    tsv: str
    stottr: str
    templates: tuple[str, ...]


def load_imdb_templates(
    templates_dir: Path = TEMPLATES_DIR,
    scope: PipelineScope | None = None,
    *,
    only_transforms: list[str] | None = None,
) -> tuple[OttrGenerator, list[TemplateFileEntry]]:
    """
    Parse enabled stOTTR files from pipeline scope into one generator.

    Uses ``load_defaults=False`` so the catalog lists only project templates.
    """
    scope = scope or load_imdb_scope()
    generator = OttrGenerator(load_defaults=False)
    catalog: list[TemplateFileEntry] = []

    for tsv_name, stottr_name in sorted(
        enabled_stottr_map(scope, only_transforms=only_transforms).items()
    ):
        path = templates_dir / stottr_name
        if not path.is_file():
            msg = f"Missing OTTR template for {tsv_name}: {path}"
            raise FileNotFoundError(msg)

        before = set(generator._templates)
        generator.load_templates(path.read_text(encoding="utf-8"))
        added = sorted(str(uri) for uri in generator._templates.keys() - before)
        catalog.append(TemplateFileEntry(tsv=tsv_name, stottr=stottr_name, templates=tuple(added)))

    return generator, catalog


def catalog_to_dict(catalog: list[TemplateFileEntry], scope: PipelineScope | None = None) -> dict[str, Any]:
    scope = scope or load_imdb_scope()
    return {
        "template_count": sum(len(entry.templates) for entry in catalog),
        "files": [
            {
                "tsv": entry.tsv,
                "stottr": entry.stottr,
                "templates": list(entry.templates),
                "transform_spec": scope.by_tsv(entry.tsv).transform_spec if scope.by_tsv(entry.tsv) else None,
            }
            for entry in catalog
        ],
        "pending_tsv": list(pending_tsv(scope)),
    }
