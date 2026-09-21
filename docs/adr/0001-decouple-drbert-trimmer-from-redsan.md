---
status: accepted
date: 2026-09-21
---

# Decouple DrBERT trimmer lifecycle from warehouse package

## Context

A DOCEDS document carries 25%–35% non-clinical administrative boilerplate (letterheads,
signatures, transport vouchers). While heuristic regexes (`trim_doceds_text()`) remove known
patterns, contextual ambiguity requires sequence classification with asymmetric class weights
(10x penalty on clinical errors).

Embedding model training, weak supervision loops, Hugging Face transformers, and dataset
curation directly inside `redsan` would introduce heavy Python ML dependencies, break the
single-responsibility principle of a warehouse normalization package, and complicate
distribution in air-gapped hospital environments.

## Decision

Isolate the ML model lifecycle in a dedicated repository (`edsan-doc-trimmer`):

1. **`edsan-doc-trimmer` owns**:
   - Model architecture (DrBERT), training, and asymmetric loss optimization.
   - Weak supervision teacher, active learning loop, and gold-standard curation.
   - Benchmark evaluation and ONNX/safetensors graph export.
   - The standalone batch inference worker script.
   - Runtime tokenization, hardware selection, and model-specific inference rules.

2. **`redsan` owns**:
   - The R interface (`trim_doceds_onnx()`).
   - Warehouse data structures (`data.frame`, `edsan_event_bundle`, cohort lists).
   - Warehouse integrity guarantees (no list-columns on normalized tables, schema preservation).
   - Versioned artifact validation, air-gapped registration (`edsan_install_trimmer()`),
     process orchestration, and identity-safe cohort mapping.

`redsan` treats the trimmer as a versioned artifact and runtime service, never as an internal training target.

## Consequences

- Documentation does not duplicate: model internals, active learning metrics, and training
  instructions live solely in `edsan-doc-trimmer`.
- `redsan` remains lightweight to install and test on standard R warehouse nodes, requiring only
  an ONNX runtime or Python environment at execution time.
- Trimming model versions can be audited and upgraded independently of `redsan` release tags.
