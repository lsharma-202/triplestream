"""Minimal N-Quads line writer (no rdflib Graph)."""

from __future__ import annotations

from typing import BinaryIO, TextIO

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"
XSD_BOOLEAN = "http://www.w3.org/2001/XMLSchema#boolean"
XSD_INTEGER = "http://www.w3.org/2001/XMLSchema#integer"
XSD_FLOAT = "http://www.w3.org/2001/XMLSchema#float"
XSD_GYEAR = "http://www.w3.org/2001/XMLSchema#gYear"

CORE_PERSON = "http://example.org/ontology/core#Person"
CORE_NAME = "http://example.org/ontology/core#name"
CORE_BIRTH_YEAR = "http://example.org/ontology/core#birthYear"
CORE_DEATH_YEAR = "http://example.org/ontology/core#deathYear"
CORE_HAS_PROFESSION = "http://example.org/ontology/core#hasProfession"

IMDB_TITLE = "http://example.org/ontology/domain/imdb#Title"
IMDB_PRIMARY_TITLE = "http://example.org/ontology/domain/imdb#primaryTitle"
IMDB_ORIGINAL_TITLE = "http://example.org/ontology/domain/imdb#originalTitle"
IMDB_IS_ADULT = "http://example.org/ontology/domain/imdb#isAdult"
IMDB_START_YEAR = "http://example.org/ontology/domain/imdb#startYear"
IMDB_END_YEAR = "http://example.org/ontology/domain/imdb#endYear"
IMDB_RUNTIME = "http://example.org/ontology/domain/imdb#runtimeMinutes"
IMDB_HAS_GENRE = "http://example.org/ontology/domain/imdb#hasGenre"
IMDB_ALTERNATE_TITLE = "http://example.org/ontology/domain/imdb#alternateTitle"
IMDB_AVERAGE_RATING = "http://example.org/ontology/domain/imdb#averageRating"
IMDB_NUM_VOTES = "http://example.org/ontology/domain/imdb#numVotes"
IMDB_PART_OF_SERIES = "http://example.org/ontology/domain/imdb#partOfSeries"
IMDB_SEASON_NUMBER = "http://example.org/ontology/domain/imdb#seasonNumber"
IMDB_EPISODE_NUMBER = "http://example.org/ontology/domain/imdb#episodeNumber"
IMDB_KNOWN_FOR = "http://example.org/ontology/domain/imdb#knownForTitle"


def _escape_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _iri(value: str) -> str:
    return f"<{value}>"


def _literal(value: str, datatype: str) -> str:
    caret = "^^"
    return f'"{_escape_literal(value)}"{caret}<{datatype}>'


class NQuadWriter:
    """Append N-Quads lines to a text stream."""

    __slots__ = ("_graph", "_handle", "triple_count")

    def __init__(self, graph_iri: str, handle: TextIO | BinaryIO) -> None:
        self._graph = _iri(graph_iri)
        self._handle = handle
        self.triple_count = 0

    def add_iri(self, subject: str, predicate: str, obj_iri: str) -> None:
        self._write(_iri(subject), _iri(predicate), _iri(obj_iri))

    def add_literal(self, subject: str, predicate: str, value: str, datatype: str) -> None:
        self._write(_iri(subject), _iri(predicate), _literal(value, datatype))

    def add_type(self, subject: str, class_iri: str) -> None:
        self.add_iri(subject, RDF_TYPE, class_iri)

    def _write(self, s: str, p: str, o: str) -> None:
        self._handle.write(f"{s} {p} {o} {self._graph} .\n")
        self.triple_count += 1
