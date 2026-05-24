"""Project path helpers shared by pipelines and source definitions."""

from pathlib import Path

# src/triplestream/paths.py → repo root is three levels up
PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
STAGING_DIR = DATA_DIR / "staging"
CONFIG_DIR = PROJECT_ROOT / "config"
TEMPLATES_DIR = PROJECT_ROOT / "templates"
ONTOLOGY_DIR = PROJECT_ROOT / "ontology"
SHAPES_DIR = PROJECT_ROOT / "shapes"
