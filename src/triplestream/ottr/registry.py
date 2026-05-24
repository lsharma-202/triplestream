"""Map IMDB staged files to OTTR template definitions."""

from __future__ import annotations

# Staged TSV → stOTTR file. title.crew has no template yet (Level 2b).
STOTTR_BY_TSV: dict[str, str] = {
    "name.basics.tsv.gz": "name.basics.stottr",
    "title.akas.tsv.gz": "title.akas.stottr",
    "title.basics.tsv.gz": "title.basics.stottr",
    "title.episode.tsv.gz": "title.episode.stottr",
    "title.ratings.tsv.gz": "title.ratings.stottr",
}

TSV_WITHOUT_TEMPLATE = ("title.crew.tsv.gz",)
