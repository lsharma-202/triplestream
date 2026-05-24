"""Stable IRIs for provenance entities — aligned with ontology/platform/pipeline."""

from __future__ import annotations

from rdflib import URIRef

# Instance IRIs live under /data/ and /graph/; TBox uses /ontology/platform/pipeline#
BASE = "http://example.org/"


def data_source_uri(source_id: str) -> URIRef:
    return URIRef(f"{BASE}data/source/{source_id}")


def raw_file_uri(source_id: str, file_name: str) -> URIRef:
    return URIRef(f"{BASE}data/{source_id}/raw/{file_name}")


def staged_file_uri(source_id: str, batch_id: str, file_name: str) -> URIRef:
    return URIRef(f"{BASE}data/{source_id}/staging/{batch_id}/raw/{file_name}")


def profile_report_uri(source_id: str, batch_id: str, file_name: str) -> URIRef:
    stem = file_name.removesuffix(".gz").removesuffix(".tsv")
    return URIRef(f"{BASE}data/{source_id}/staging/{batch_id}/profiles/{stem}")


def ingest_batch_uri(source_id: str, batch_id: str) -> URIRef:
    return URIRef(f"{BASE}data/{source_id}/staging/{batch_id}")


def manifest_uri(source_id: str, batch_id: str) -> URIRef:
    return URIRef(f"{BASE}data/{source_id}/staging/{batch_id}/manifest")


def flow_agent_uri(flow_run_id: str) -> URIRef:
    return URIRef(f"{BASE}agents/prefect/flow/{flow_run_id}")


def task_activity_uri(task_run_id: str) -> URIRef:
    return URIRef(f"{BASE}activities/prefect/task/{task_run_id}")


def ottr_catalog_uri(source_id: str, batch_id: str) -> URIRef:
    return URIRef(f"{BASE}data/{source_id}/staging/{batch_id}/templates/catalog")


def provenance_graph_uri(source_id: str, batch_id: str) -> URIRef:
    return URIRef(f"{BASE}graph/provenance/{source_id}/{batch_id}")


def graph_entity_uri(source_id: str, batch_id: str, tsv: str, zone: str) -> URIRef:
    """PROV entity for a named graph zone (work / asserted / quarantine)."""
    stem = tsv.removesuffix(".gz").removesuffix(".tsv")
    return URIRef(f"{BASE}graph/{zone}/{source_id}/{batch_id}/{stem}")


def validation_report_uri(source_id: str, batch_id: str, tsv: str) -> URIRef:
    stem = tsv.removesuffix(".gz").removesuffix(".tsv")
    return URIRef(f"{BASE}data/{source_id}/staging/{batch_id}/reports/{stem}/validation")
