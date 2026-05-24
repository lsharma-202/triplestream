"""
IMDB source definition (data-plane metadata).

This module is NOT Prefect code. It describes *where* raw data lives and how
Prefect flows should find it. One file per source scales to 160+ sources.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from triplestream.paths import DATA_DIR, STAGING_DIR

IMDB_RAW_DIR = DATA_DIR / "imdb"
IMDB_STAGING_DIR = STAGING_DIR / "imdb"

EXPECTED_FILES = (
    "name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.ratings.tsv.gz",
)

# IMDb TSV uses tab separation; names may contain literal double quotes (not RFC CSV).
IMDB_TSV_READ_KWARGS: dict[str, Any] = {
    "separator": "\t",
    "has_header": True,
    "null_values": ["\\N"],
    "infer_schema_length": 1000,
    "quote_char": None,
}

IMDB_RAW_DIR = DATA_DIR / "imdb"
IMDB_STAGING_DIR = STAGING_DIR / "imdb"

EXPECTED_FILES = (
    "name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.ratings.tsv.gz",
)


@dataclass(frozen=True, slots=True)
class SourceDefinition:
    """Static config for a single upstream source."""

    source_id: str
    display_name: str
    raw_dir: Path
    staging_dir: Path
    expected_files: tuple[str, ...]


IMDB_SOURCE = SourceDefinition(
    source_id="imdb",
    display_name="IMDB Non-Commercial Datasets",
    raw_dir=IMDB_RAW_DIR,
    staging_dir=IMDB_STAGING_DIR,
    expected_files=EXPECTED_FILES,
)
