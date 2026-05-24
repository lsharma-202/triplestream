"""Row-batch mappers aligned to templates/*.stottr semantics."""

from __future__ import annotations

from collections.abc import Callable

import polars as pl

from triplestream.entity.uris import (
    person_iri,
    split_csv_field,
    title_iri,
    title_type_iri,
    vocab_concept_iri,
)
from triplestream.materialize.nq import (
    CORE_BIRTH_YEAR,
    CORE_DEATH_YEAR,
    CORE_HAS_PROFESSION,
    CORE_NAME,
    CORE_PERSON,
    IMDB_ALTERNATE_TITLE,
    IMDB_AVERAGE_RATING,
    IMDB_END_YEAR,
    IMDB_EPISODE_NUMBER,
    IMDB_HAS_GENRE,
    IMDB_IS_ADULT,
    IMDB_KNOWN_FOR,
    IMDB_NUM_VOTES,
    IMDB_ORIGINAL_TITLE,
    IMDB_PART_OF_SERIES,
    IMDB_PRIMARY_TITLE,
    IMDB_RUNTIME,
    IMDB_SEASON_NUMBER,
    IMDB_START_YEAR,
    IMDB_TITLE,
    NQuadWriter,
    XSD_BOOLEAN,
    XSD_FLOAT,
    XSD_GYEAR,
    XSD_INTEGER,
    XSD_STRING,
)

Mapper = Callable[[pl.DataFrame, NQuadWriter], None]


def _is_set(value: object) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    return text not in ("", "\\N")


def map_title_ratings(df: pl.DataFrame, writer: NQuadWriter) -> None:
    for row in df.iter_rows(named=True):
        subject = title_iri(row["tconst"])
        writer.add_literal(subject, IMDB_AVERAGE_RATING, str(row["averageRating"]), XSD_FLOAT)
        writer.add_literal(subject, IMDB_NUM_VOTES, str(row["numVotes"]), XSD_INTEGER)


def map_title_akas(df: pl.DataFrame, writer: NQuadWriter) -> None:
    for row in df.iter_rows(named=True):
        if not _is_set(row.get("title")):
            continue
        subject = title_iri(row["titleId"])
        writer.add_literal(subject, IMDB_ALTERNATE_TITLE, str(row["title"]), XSD_STRING)


def map_title_episode(df: pl.DataFrame, writer: NQuadWriter) -> None:
    for row in df.iter_rows(named=True):
        episode = title_iri(row["tconst"])
        writer.add_iri(episode, IMDB_PART_OF_SERIES, title_iri(row["parentTconst"]))
        if _is_set(row.get("seasonNumber")):
            writer.add_literal(episode, IMDB_SEASON_NUMBER, str(row["seasonNumber"]), XSD_INTEGER)
        if _is_set(row.get("episodeNumber")):
            writer.add_literal(episode, IMDB_EPISODE_NUMBER, str(row["episodeNumber"]), XSD_INTEGER)


def map_title_basics(df: pl.DataFrame, writer: NQuadWriter) -> None:
    for row in df.iter_rows(named=True):
        subject = title_iri(row["tconst"])
        writer.add_type(subject, IMDB_TITLE)
        if _is_set(row.get("titleType")):
            writer.add_type(subject, title_type_iri(str(row["titleType"])))
        if _is_set(row.get("primaryTitle")):
            writer.add_literal(subject, IMDB_PRIMARY_TITLE, str(row["primaryTitle"]), XSD_STRING)
        if _is_set(row.get("originalTitle")):
            writer.add_literal(subject, IMDB_ORIGINAL_TITLE, str(row["originalTitle"]), XSD_STRING)
        if _is_set(row.get("isAdult")):
            flag = "true" if str(row["isAdult"]) == "1" else "false"
            writer.add_literal(subject, IMDB_IS_ADULT, flag, XSD_BOOLEAN)
        if _is_set(row.get("startYear")):
            writer.add_literal(subject, IMDB_START_YEAR, str(row["startYear"]), XSD_GYEAR)
        if _is_set(row.get("endYear")):
            writer.add_literal(subject, IMDB_END_YEAR, str(row["endYear"]), XSD_GYEAR)
        if _is_set(row.get("runtimeMinutes")):
            writer.add_literal(subject, IMDB_RUNTIME, str(row["runtimeMinutes"]), XSD_INTEGER)
        for genre in split_csv_field(row.get("genres")):
            writer.add_iri(subject, IMDB_HAS_GENRE, vocab_concept_iri(genre))


def map_name_basics(df: pl.DataFrame, writer: NQuadWriter) -> None:
    for row in df.iter_rows(named=True):
        subject = person_iri(row["nconst"])
        writer.add_type(subject, CORE_PERSON)
        if _is_set(row.get("primaryName")):
            writer.add_literal(subject, CORE_NAME, str(row["primaryName"]), XSD_STRING)
        if _is_set(row.get("birthYear")):
            writer.add_literal(subject, CORE_BIRTH_YEAR, str(row["birthYear"]), XSD_GYEAR)
        if _is_set(row.get("deathYear")):
            writer.add_literal(subject, CORE_DEATH_YEAR, str(row["deathYear"]), XSD_GYEAR)
        for profession in split_csv_field(row.get("primaryProfession")):
            writer.add_iri(subject, CORE_HAS_PROFESSION, vocab_concept_iri(profession))
        for tconst in split_csv_field(row.get("knownForTitles")):
            writer.add_iri(subject, IMDB_KNOWN_FOR, title_iri(tconst))


MAPPERS: dict[str, Mapper] = {
    "name-basics": map_name_basics,
    "title-akas": map_title_akas,
    "title-basics": map_title_basics,
    "title-episode": map_title_episode,
    "title-ratings": map_title_ratings,
}


def get_mapper(transform_id: str) -> Mapper:
    try:
        return MAPPERS[transform_id]
    except KeyError as exc:
        msg = f"No materialize mapper registered for transform {transform_id!r}"
        raise KeyError(msg) from exc
