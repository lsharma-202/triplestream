"""Load and validate stOTTR templates via pyOTTR."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ottr import OttrGenerator

from triplestream.ottr.registry import STOTTR_BY_TSV
from triplestream.paths import TEMPLATES_DIR


@dataclass(frozen=True, slots=True)
class TemplateFileEntry:
    file: str
    templates: tuple[str, ...]


def load_imdb_templates(templates_dir: Path = TEMPLATES_DIR) -> tuple[OttrGenerator, list[TemplateFileEntry]]:
    """
    Parse all IMDB stOTTR files into one generator.

    Uses ``load_defaults=False`` so the catalog lists only project templates.
    """
    generator = OttrGenerator(load_defaults=False)
    catalog: list[TemplateFileEntry] = []

    for tsv_name, stottr_name in sorted(STOTTR_BY_TSV.items()):
        path = templates_dir / stottr_name
        if not path.is_file():
            msg = f"Missing OTTR template for {tsv_name}: {path}"
            raise FileNotFoundError(msg)

        before = set(generator._templates)
        generator.load_templates(path.read_text(encoding="utf-8"))
        added = sorted(str(uri) for uri in generator._templates.keys() - before)
        catalog.append(TemplateFileEntry(file=stottr_name, templates=tuple(added)))

    return generator, catalog


def catalog_to_dict(catalog: list[TemplateFileEntry]) -> dict[str, Any]:
    return {
        "template_count": sum(len(entry.templates) for entry in catalog),
        "files": [
            {"tsv": tsv, "stottr": entry.file, "templates": list(entry.templates)}
            for tsv, entry in zip(sorted(STOTTR_BY_TSV), catalog, strict=True)
        ],
    }
