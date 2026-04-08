# Reference Templates

Use these CSVs as manual-ingestion templates for reconciliation inputs.

- Leave unknown values blank.
- Do not infer or backfill missing fields.
- Record the original source in the provenance columns.
- Prefer one source observation per row.
- Keep estimates clearly marked in `method_reported_or_estimated`.
- For `sec_filings_reference.csv`, populate the issuer fields and mark each metric
  scope explicitly:
  - `direct_comparable` when the metric can be compared straight against the
    workbook claim
  - `scope_mismatch` when the source is real but not definitionally aligned
  - `not_available` when the source does not provide that metric cleanly
- Use `scope_notes` to explain why a primary-source row is directly comparable
  or why it must stay blocked as a scope mismatch.
