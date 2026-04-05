# Model Candidates

This document tracks research-only model candidates for future experimentation.
None of the models listed here are part of the supported `qsr-audit` production
pipeline today.

## Guardrails

- Experimental models may read local Silver or Gold snapshots for offline
  evaluation.
- Experimental models must not redefine source-of-truth business metrics.
- Experimental models must not gate validation, reconciliation, reporting, or
  strategy outputs.
- Experimental models must not write analyst-facing outputs under `reports/` or
  `strategy/` unless a separate hardening change is approved.
- Evaluation plans should focus on measurable task fit, failure modes, and
  operational cost, not hype.
- Forecast experiments should use repeated-period Gold history assembled through
  `snapshot-gold` and `build-forecast-panel`, not raw workbook snapshots.

## Forecasting candidates

| Candidate | Why it is interesting | Current role |
| --- | --- | --- |
| [`amazon/chronos-bolt-small`](https://hf.co/amazon/chronos-bolt-small) | Apache-2.0, 47.7M parameters, tuned for time-series forecasting and materially smaller than large general forecasting stacks. | First lightweight zero-shot benchmark once we have real longitudinal Gold data. |
| [`amazon/chronos-bolt-base`](https://hf.co/amazon/chronos-bolt-base) | Apache-2.0, larger Bolt-family checkpoint for a second pass if `bolt-small` shows promise but leaves measurable headroom. | Follow-up quality benchmark, not the first local candidate. |
| [`amazon/chronos-t5-small`](https://hf.co/amazon/chronos-t5-small) | Apache-2.0, 46.2M parameters, widely used Chronos baseline from the original 2024 paper lineage. | Historical control for verifying whether Bolt is buying real gains. |

Research note:
The Chronos paper on Hugging Face is
[`Chronos: Learning the Language of Time Series`](https://hf.co/papers/2403.07815).
Treat raw Chronos as a baseline, not as a complete forecasting system. If
weather, promotions, or other covariates matter, a pure zero-shot univariate
model may not be enough.

Current repo posture:

- Chronos is not part of the supported production CLI.
- The repo now includes only an opt-in readiness guard for offline experiments.
- CI should never auto-download Chronos weights.
- Benchmark artifacts belong under `artifacts/forecasting/`, not `reports/` or
  `strategy/`.

## ASR candidates

| Candidate | Why it is interesting | Current role |
| --- | --- | --- |
| [`distil-whisper/distil-large-v3`](https://hf.co/distil-whisper/distil-large-v3) | MIT license, distilled Whisper family, English ASR focus, designed as a smaller/faster alternative to full Whisper. | First ASR candidate when we need low-latency English transcription experiments. |
| [`openai/whisper-large-v3-turbo`](https://hf.co/openai/whisper-large-v3-turbo) | Apache-2.0, multilingual Whisper-family control with strong speed/quality tradeoffs. | Primary baseline when multilingual or accent robustness matters. |
| [`distil-whisper/distil-small.en`](https://hf.co/distil-whisper/distil-small.en) | Smaller English-only Distil-Whisper checkpoint for memory-constrained or on-device experiments. | Budget fallback if `distil-large-v3` is too heavy. |

Research note:
The Distil-Whisper paper on Hugging Face is
[`Distil-Whisper: Robust Knowledge Distillation via Large-Scale Pseudo Labelling`](https://hf.co/papers/2311.00430).
The Distil-Whisper checkpoints above are best treated as English-first. If
code-switching, multilingual audio, or heavy accent/noise shift matters, keep a
full Whisper-family baseline in the evaluation set.

## Lightweight embedding and RAG candidates

| Candidate | Why it is interesting | Current role |
| --- | --- | --- |
| [`sentence-transformers/all-MiniLM-L6-v2`](https://hf.co/sentence-transformers/all-MiniLM-L6-v2) | Apache-2.0, 22.7M parameters, very small, widely used retrieval baseline, compatible with `sentence-transformers` and Text Embeddings Inference. | Default low-friction retrieval baseline. |
| [`BAAI/bge-small-en-v1.5`](https://hf.co/BAAI/bge-small-en-v1.5) | MIT license, 33.4M parameters, strong English retrieval candidate with broad community adoption. | Accuracy-focused lightweight retrieval candidate. |
| [`intfloat/e5-small-v2`](https://hf.co/intfloat/e5-small-v2) | MIT license, small sentence-transformer family candidate with retrieval-oriented benchmarks. | Alternative baseline for contrastive retrieval quality and prompt-format sensitivity testing. |
| [`intfloat/multilingual-e5-small`](https://hf.co/intfloat/multilingual-e5-small) | Lightweight multilingual option when retrieval cannot stay English-only. | Multilingual fallback, not the default baseline. |

Optional rerankers:

- [`cross-encoder/ms-marco-MiniLM-L6-v2`](https://hf.co/cross-encoder/ms-marco-MiniLM-L6-v2)
  as a cheap top-k reranker.
- [`BAAI/bge-reranker-base`](https://hf.co/BAAI/bge-reranker-base) only if the
  lightweight reranker leaves clear quality headroom.

Suggested lightweight stack shapes:

1. BM25 lexical retrieval as the default non-ML baseline.
2. In-process embeddings via `sentence-transformers` for opt-in local dense
   benchmarks only.
3. `e5-small-v2` as an explicit follow-up comparison, not a default.

Current repo posture:

- The repo now includes a retrieval-only scaffold under `artifacts/rag/`.
- There is still no answer generation or production RAG service path.
- Dense retrieval must be explicitly enabled and is skipped in CI.
- Retrieval experiments must index only vetted Gold or provenance-aware reviewed artifacts.

## Decision posture

- Prefer the smallest candidate that is good enough for the experiment.
- Always compare against a simple non-ML baseline where possible.
- Record license, model size, language coverage, and evaluation assumptions
  before treating any candidate as promising.
- For `e5-small-v2`, verify query/document prompt formatting during
  implementation instead of assuming the defaults are interchangeable.
- Keep all model experimentation downstream of validated data products.
- Treat retrieval quality as a benchmark problem first; do not blur retrieval hits
  with audited facts or analyst-facing conclusions.
