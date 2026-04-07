# Security and Privacy Controls

This repo is built around one operational rule: the workbook is a hypothesis artifact, not a source of truth. The controls below exist to keep working-layer data, Gold decisions, and experimental outputs from being confused with audited facts.

## Data classification

- `public`: safe for broad sharing without internal context. This should be rare in this repo.
- `internal`: safe for team-internal operations, runbooks, and engineering coordination.
- `confidential`: contains analyst-facing business context, working-layer evidence, or internal audit detail that should not be shared broadly.
- `restricted`: the tightest label for especially sensitive material or operational controls.

Classification is attached to command manifests, not used as a substitute for analyst judgment.

## Manifest and lineage controls

- Release-relevant commands write manifests under `artifacts/manifests/<command>/`.
- Each manifest records:
  - command name and run timestamp
  - Git SHA when available
  - input and output paths
  - row counts when available
  - output file hashes when practical
  - data classification
  - intended audience
  - publish-status scope
  - upstream artifact references
  - warning and error counts
- These manifests are internal lineage controls. They make it easier to audit how Gold artifacts were produced and whether release preconditions were satisfied.

## Structured audit logs

- Release-relevant CLI runs write machine-readable logs under `artifacts/audit_logs/<command>/`.
- Audit logs capture:
  - start and end timestamps
  - success or failure status
  - input and output paths
  - warning and error counts
  - manifest path when created
- Audit logs intentionally avoid raw secret values and avoid dumping raw environment contents.

## Secret handling and redaction

- `.env.example` documents supported configuration without including live values.
- Safe debug output redacts secret-like environment values rather than printing them.
- Do not commit real API keys, tokens, or private document identifiers into source control.
- Do not paste secrets into analyst notes, manifests, or release comments.

## Analyst-facing vs experimental outputs

- `reports/` and `strategy/` are analyst-facing output zones.
- `artifacts/forecasting/` and `artifacts/rag/` are experimental zones.
- Experimental outputs must stay outside `reports/` and `strategy/` unless a future gate explicitly promotes them.
- Retrieval and forecast outputs are not audited facts. They remain internal experiment artifacts until separately reviewed and promoted.

## Operational expectations

- Use `qsr-audit gate-gold` before external KPI export decisions.
- Use `qsr-audit preflight-release` before any external-facing handoff.
- Treat missing manifests, missing provenance, and missing reference coverage as real control failures, not paperwork issues.
