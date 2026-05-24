"""Build PROV-O triples for one pipeline derivation step."""

from __future__ import annotations

from datetime import UTC, datetime

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import PROV, RDF, RDFS, XSD

PIPELINE = Namespace("http://example.org/ontology/platform/pipeline#")


def record_derivation(
    *,
    activity: URIRef,
    agent: URIRef,
    input_entity: URIRef,
    output_entity: URIRef,
    transform_spec: str,
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
    record_count: int | None = None,
    output_types: tuple[URIRef, ...] = (PROV.Entity,),
    input_types: tuple[URIRef, ...] = (PROV.Entity,),
    extra_input: dict[URIRef, Literal] | None = None,
    extra_output: dict[URIRef, Literal] | None = None,
) -> Graph:
    """
    Emit batch-level PROV-O for one transform: used → activity → generated.

    Returns a plain Graph (one named graph wrapper applied at store time).
    """
    g = Graph()
    g.bind("prov", PROV)
    g.bind("pipeline", PIPELINE)

    started = started_at or datetime.now(UTC)
    ended = ended_at or datetime.now(UTC)

    for t in input_types:
        g.add((input_entity, RDF.type, t))
    for t in output_types:
        g.add((output_entity, RDF.type, t))

    if extra_input:
        for predicate, literal in extra_input.items():
            g.add((input_entity, predicate, literal))

    g.add((activity, RDF.type, PROV.Activity))
    g.add((activity, PROV.used, input_entity))
    g.add((activity, PROV.generated, output_entity))
    g.add((activity, PROV.wasAssociatedWith, agent))
    g.add((activity, PROV.startedAtTime, Literal(started, datatype=XSD.dateTime)))
    g.add((activity, PROV.endedAtTime, Literal(ended, datatype=XSD.dateTime)))

    g.add((output_entity, PROV.wasGeneratedBy, activity))
    g.add((output_entity, PROV.wasDerivedFrom, input_entity))

    generation = URIRef(f"{output_entity}/generation")
    g.add((generation, RDF.type, PROV.Generation))
    g.add((generation, PROV.entity, output_entity))
    g.add((generation, PROV.activity, activity))
    g.add((generation, PIPELINE.transformSpec, Literal(transform_spec)))
    if record_count is not None:
        g.add((generation, PIPELINE.recordCount, Literal(record_count, datatype=XSD.integer)))
    g.add((output_entity, PROV.qualifiedGeneration, generation))

    if extra_output:
        for predicate, literal in extra_output.items():
            g.add((output_entity, predicate, literal))

    return g


def record_batch_start(
    *,
    batch_entity: URIRef,
    data_source: URIRef,
    agent: URIRef,
    activity: URIRef,
    source_id: str,
    batch_id: str,
    display_name: str,
    source_fingerprint: str | None = None,
) -> Graph:
    """Register DataSource + IngestBatch at the start of a flow run."""
    g = Graph()
    g.bind("prov", PROV)
    g.bind("pipeline", PIPELINE)

    g.add((data_source, RDF.type, PIPELINE.DataSource))
    g.add((data_source, RDF.type, PROV.Entity))
    g.add((data_source, PIPELINE.sourceId, Literal(source_id)))
    g.add((data_source, RDFS.label, Literal(display_name)))

    g.add((batch_entity, RDF.type, PIPELINE.IngestBatch))
    g.add((batch_entity, RDF.type, PROV.Entity))
    g.add((batch_entity, PIPELINE.batchId, Literal(batch_id)))
    if source_fingerprint is not None:
        g.add((batch_entity, PIPELINE.sourceFingerprint, Literal(source_fingerprint)))
    g.add((batch_entity, PROV.wasDerivedFrom, data_source))

    g.add((agent, RDF.type, PROV.SoftwareAgent))
    g.add((agent, RDFS.label, Literal(f"prefect-flow:{source_id}")))

    g.add((activity, RDF.type, PROV.Activity))
    g.add((activity, PROV.generated, batch_entity))
    g.add((activity, PROV.wasAssociatedWith, agent))
    g.add((activity, PROV.startedAtTime, Literal(datetime.now(UTC), datatype=XSD.dateTime)))
    g.add((batch_entity, PROV.wasGeneratedBy, activity))

    return g
