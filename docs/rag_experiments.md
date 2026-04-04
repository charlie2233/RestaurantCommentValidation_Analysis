# RAG Experiments

This document outlines lightweight retrieval experiments for future internal
research. It does not approve production RAG inside the workbook audit pipeline.

## Goal

Test whether compact embedding models can improve analyst lookup over validated
reference material, provenance notes, and reconciliation summaries without
introducing a heavy serving stack.

The first experiment should focus on retrieval quality, not answer generation.

## Candidate embeddings

| Candidate | Primary use | Notes |
| --- | --- | --- |
| [`sentence-transformers/all-MiniLM-L6-v2`](https://hf.co/sentence-transformers/all-MiniLM-L6-v2) | Default baseline | Very small, cheap to run, broad community baseline. |
| [`BAAI/bge-small-en-v1.5`](https://hf.co/BAAI/bge-small-en-v1.5) | Quality-focused lightweight option | Good retrieval candidate when we want a stronger English dense baseline. |
| [`intfloat/e5-small-v2`](https://hf.co/intfloat/e5-small-v2) | Alternative dense baseline | Useful when prompt formatting and retrieval behavior need comparison. |
| [`intfloat/multilingual-e5-small`](https://hf.co/intfloat/multilingual-e5-small) | Multilingual fallback | Use only if the corpus or analyst questions are not reliably English-only. |

## Lightweight stack options

### Option A: in-process baseline

- Embed documents directly in-process with `sentence-transformers`.
- Build a simple local vector index.
- Use notebook or test-fixture scale corpora only.
- Keep a lexical baseline such as BM25 for comparison.

Why start here:

- Lowest operational complexity.
- Easiest to test deterministically.
- Enough to compare chunking and embedding choices.

### Option B: thin service boundary

- Serve the embedding model through Hugging Face Text Embeddings Inference.
- Keep the retriever and evaluation harness local.
- Use the same corpus and queries as Option A.
- Add reranking only after first-pass dense retrieval is stable.

Why this matters:

- Cleaner separation between embedding inference and retrieval logic.
- Easier apples-to-apples comparison across models.
- Still lightweight compared with a full production search stack.

### Optional reranking

If top-k retrieval is close but not good enough, test a cheap reranker such as
[`cross-encoder/ms-marco-MiniLM-L6-v2`](https://hf.co/cross-encoder/ms-marco-MiniLM-L6-v2)
on the top 20 or top 50 retrieved chunks before trying larger rerankers.

## Retrieval corpus candidates

Only use vetted local material:

- Gold validation outputs
- Gold reconciliation outputs
- Provenance registry records
- Manual reference notes that have already been normalized and reviewed

Do not index raw workbooks as if they were trusted facts.

## Evaluation plan

Use a small analyst-authored benchmark:

1. Write representative analyst questions.
2. Mark the documents or chunks that should support each answer.
3. Measure retrieval hit rate at `k`, ranking quality, and failure cases.
4. Record whether errors come from chunking, metadata filtering, or embedding
   quality.

Suggested checks:

- Recall@k
- MRR or nDCG
- Citation precision
- Answer faithfulness if answer generation is added later
- Metadata filter correctness
- Query latency on local hardware
- Index size

## Guardrails

- Start with retrieval-only evaluation before any answer synthesis.
- Do not let RAG outputs bypass existing Gold validation and reconciliation
  outputs.
- Do not claim factual authority beyond the indexed Gold/provenance material.
- Keep experiments local, reproducible, and easy to delete.

## Recommendation

Start with `all-MiniLM-L6-v2` as the baseline, compare against
`bge-small-en-v1.5`, and bring in `e5-small-v2` only if the first two leave
retrieval gaps worth investigating. Add multilingual E5 only if the benchmark
proves English-only retrieval is not enough.
