# RAG Corpus Contract

This document defines what is allowed into the local retrieval corpus and how
those chunks should be interpreted.

## Corpus purpose

The corpus exists to benchmark retrieval quality over vetted project artifacts.
It does not create a new source of truth.

## Approved sources

The corpus builder may index these artifact families when present:

- Gold publishing decisions and their publishable or blocked subsets
- Gold reconciliation outputs
- Gold reference coverage outputs
- Gold validation flags
- Gold provenance registry records
- validation summary markdown
- optional normalized manual reference notes under `data/reference/`

## Excluded sources

Excluded by default:

- `data/raw/`
- `data/bronze/`
- `data/silver/`
- raw workbook files

Reason:

- the workbook is a hypothesis artifact
- Bronze and Silver are working layers
- retrieval experiments must not bypass Gold validation, reconciliation, or publishing decisions

## Required chunk fields

Each chunk carries these fields:

- `doc_id`
- `chunk_id`
- `source_kind`
- `title`
- `text`
- `artifact_path`
- `brand_names`
- `metric_names`
- `as_of_date`
- `publish_status`
- `confidence_score`
- `source_name`
- `source_url_or_doc_id`
- `metadata_json`

## Metadata rules

- chunk IDs must be deterministic
- provenance metadata must be preserved on every chunk
- blocked and advisory labels must remain explicit in metadata
- retrieval output must surface publish status and provenance where available

## Interpretation rules

- `publishable` means the originating KPI row passed the Gold gate
- `advisory` means context only, not safe for external KPI export
- `blocked` means unresolved issues remain

Retrieval results are navigation aids. They are not audited facts by themselves.
