"""Instance IRIs — no /id/ segment; aligned with OTTR ottr:IRI parameters."""

from __future__ import annotations

BASE = "http://example.org/"
IMDB_NS = f"{BASE}imdb/"
ONTOLOGY_IMDB = f"{BASE}ontology/domain/imdb#"
VOCAB_NS = f"{BASE}ontology/domain/imdb/vocab#"

TITLE_TYPE_CLASSES: dict[str, str] = {
    "movie": "Movie",
    "short": "Short",
    "tvEpisode": "TVEpisode",
    "tvMiniSeries": "TVMiniSeries",
    "tvMovie": "TVMovie",
    "tvPilot": "TVPilot",
    "tvSeries": "TVSeries",
    "tvShort": "TVShort",
    "tvSpecial": "TVSpecial",
    "video": "Video",
    "videoGame": "VideoGame",
}


def title_iri(tconst: str) -> str:
    return f"{IMDB_NS}title/{tconst}"


def person_iri(nconst: str) -> str:
    return f"{IMDB_NS}person/{nconst}"


def title_type_iri(title_type: str) -> str:
    class_name = TITLE_TYPE_CLASSES.get(title_type)
    if class_name is None:
        msg = f"Unknown IMDb titleType: {title_type!r}"
        raise KeyError(msg)
    return f"{ONTOLOGY_IMDB}{class_name}"


def vocab_concept_iri(raw_label: str) -> str:
    """Map IMDB genre/profession label to vocab local name (e.g. Film-Noir → film-noir)."""
    slug = raw_label.strip().lower().replace("_", "-").replace(" ", "-")
    return f"{VOCAB_NS}{slug}"


def split_csv_field(value: str | None) -> list[str]:
    if value is None or value == "\\N" or not str(value).strip():
        return []
    return [part.strip() for part in str(value).split(",") if part.strip()]
