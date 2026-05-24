"""OTTR template loading and registry."""

from triplestream.ottr.loader import TemplateFileEntry, catalog_to_dict, load_imdb_templates
from triplestream.ottr.registry import STOTTR_BY_TSV, TSV_WITHOUT_TEMPLATE

__all__ = [
    "STOTTR_BY_TSV",
    "TSV_WITHOUT_TEMPLATE",
    "TemplateFileEntry",
    "catalog_to_dict",
    "load_imdb_templates",
]
