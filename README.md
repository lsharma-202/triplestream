# triplestream

[![Python](https://img.shields.io/badge/python-3.14+-blue.svg)](https://www.python.org/downloads/)
[![RDFLib](https://img.shields.io/badge/RDFLib-7.6+-green.svg)](https://rdflib.readthedocs.io/)
[![pySHACL](https://img.shields.io/badge/pySHACL-0.31+-orange.svg)](https://github.com/RDFLib/pySHACL)
[![Prefect](https://img.shields.io/badge/Prefect-3.0+-605DEC.svg)](https://www.prefect.io/)
[![Polars](https://img.shields.io/badge/Polars-1.0+-0078D4.svg)](https://pola.rs/)
[![uv](https://img.shields.io/badge/uv-managed-DE5FE9.svg)](https://docs.astral.sh/uv/)

Ontology-first knowledge graph for IMDB (and future sources): OWL/SKOS TBoxes, SHACL gates, stOTTR mapping contracts, and Prefect pipelines that land raw data, materialize N-Quads at scale, and emit PROV-O lineage.

## What's implemented

| Level | Capability | Status |
|-------|------------|--------|
| **1** | Discover raw TSV.gz, content-hash batch id, land + profile, `manifest.json` | Done |
| **2a** | Load stOTTR templates (pyOTTR catalog), init `graphs/` layout | Done |
| **2b** | Polars streaming → chunked N-Quads under `graphs/work/` | Done (5 TSVs; `title.crew` pending template) |
| **2c** | pySHACL validation gate → `graphs/asserted/` or `graphs/quarantine/` | Done |
| **—** | PROV-O fragments consolidated to `provenance/provenance.trig` | Done |

**Enabled transforms** (see `config/pipelines/imdb.toml`): `name-basics`, `title-akas`, `title-basics`, `title-episode`, `title-ratings`.  
**Pending:** `title.crew` (no stOTTR template yet).

## Quick start

```bash
uv sync --extra pipeline
```

Place IMDB `.tsv.gz` files in `data/imdb/`, then:

```bash
# Full pipeline (all enabled transforms)
uv run neo-imdb ingest

# Single transform (smoke test)
uv run neo-imdb ingest --only title-ratings

# Phase-by-phase
uv run neo-imdb stage
uv run neo-imdb materialize --only title-basics
uv run neo-imdb validate --only title-basics
```

Or run the Prefect flow module directly:

```bash
uv run python -m triplestream.pipelines.imdb.flow
```

Staged output: `data/staging/imdb/<batch-id>/`

## Batch layout

```
data/staging/imdb/<batch-id>/
├── raw/                    # copied TSV.gz inputs
├── manifest.json           # profiles + content hashes
├── templates/catalog.json  # loaded stOTTR templates
├── graphs/
│   ├── layout.json         # named-graph IRIs per transform
│   ├── work/               # materialized N-Quads (pre-validation)
│   ├── asserted/           # SHACL-conformant parts
│   ├── quarantine/         # failed parts
│   └── reports/            # validation.json + SHACL reports
└── provenance/
    ├── fragments/          # one TriG file per pipeline step
    └── provenance.trig     # consolidated PROV-O graph
```

## Repository layout

```
ontology/     # core, platform/pipeline, domain/imdb TBoxes + SKOS vocabs
shapes/       # SHACL shapes (data quality, vocab schemes)
config/       # pipeline scope (imdb.toml) and shape bindings
templates/    # stOTTR mapping templates
src/          # Python package: sources, lineage, materialize, pipelines
data/         # raw inputs + staged artifacts
docs/         # architecture notes, SPARQL exercises, plans
```

## Stack

| Layer | Tools |
|-------|-------|
| Graph model | OWL, SKOS, PROV-O, stOTTR |
| Validation | pySHACL + config-driven shape bindings |
| Orchestration | Prefect 3 (phase subflows) |
| Transform | Polars (streaming), custom N-Quad writer |
| Serialization | RDFLib (TriG provenance) |

## Docs

- [Architecture overview](docs/palantir/ARCHITECTURE.md)
- [IMDB competency queries](docs/queries/imdb/README.md)
- [RDF practice exercises](docs/practice/EXERCISES.md)
- [Idempotent staging plan](docs/plans/2026-05-24-idempotent-staging-provenance.md)
