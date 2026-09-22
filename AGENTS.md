# Working in redsan

## Scope

`redsan` is an R package for retrieving, normalizing, and processing hospital electronic health record payloads from the EDSAN data warehouse (CHU de Rouen). It manages patient stay event bundles (`edsan_event_bundle`), document text processing (`DOCEDS`), PMSI stays/diagnoses, and laboratory/microbiology data.

## Coding Standards & Conventions

### R File Naming (`kebab-case`)
Source files in `R/` follow `<module>-<subsystem>.R`:
- `doceds-trim-onnx.R`, `doceds-structure.R`
- `event-bundle.R`, `event-bundle-render.R`
- `cora-diet.R`, `cora-explore.R`
- `edsan-ct.R`, `edsan-patient-reidentify.R`
*(Base single-word modules: `biol.R`, `cora.R`, `doceds.R`, `pmsi.R`, `sources.R`, `get_edsan.R`).*

### Collation Order Overrides (`zzz-*.R`)
R collates and loads source files alphabetically. Files prefixed with `zzz-`, `zzzz-`, or `zzzzz-` are deliberate **collation overrides** that monkey-patch or extend earlier function definitions (e.g. `zzz-cora-diet-rdv.R`, `zzzz-edsan-desktop-direct.R`). Do not rename them to earlier alphabetical names.

### R Function Naming (`snake_case`)
- **Public exported functions**: `<action>_<module>_<detail>()` (e.g. `trim_doceds_onnx()`, `process_doceds()`, `process_pmsi()`, `process_biol()`).
- **Internal helpers**: Dot-prefixed `.<module>_<helper>()` (e.g. `.edsan_get_trimmer_dir()`, `.doceds_onnx_validate_artifact()`).

### Test Conventions
Unit tests live in `tests/testthat/test-<kebab-case>.R`, matching their source module (e.g. `test-doceds-trim-onnx.R`).

---

## Document Trimming

`trim_doceds_onnx()` is the sole document trimmer. It delegates inference to a
versioned `edsan-doc-trimmer` artifact and accepts character vectors, tables,
event bundles, and lists of bundles. Tables retain original text and receive
`RECTXT_TRIMMED`, `TRIM_REDUCTION_PCT`, and `TRIM_PRESERVED_INTERVALS`.
Preserved intervals refer to exact source coordinates and are stored as JSON.

`doceds_onnx_spec()` identifies the runtime artifact by hashing `model.onnx`,
`tokenizer.json`, `trim_batch_service.py`, and `artifact.json`.
The regex trimmer and its audit workflows have been retired; do not restore
parallel heuristic trimming or its provenance API.

**Why the ONNX digest is cached.** `model.onnx` is 442 MB, and a caller that
builds one catalog per stay would hash it once per stay - about 13 minutes over
a 544-stay cohort. `doceds_onnx_spec()` therefore stores each result in a
package-local environment, under a key built from the artifact's path and from
the size and modification time of each of the four files. A repeated call finds
the key and returns the stored digest: 1.5 s cold, 40 ms afterwards.

The key is what makes the cache safe to trust. It is not the artifact path
alone. Installing a different archive writes different files, which changes
their sizes or their modification times, which builds a different key, which
finds nothing stored and hashes again. The cache can only return a digest for
an artifact whose four files are still exactly as they were when that digest
was computed.

### Boundary with `edsan-doc-trimmer`
`edsan-doc-trimmer` (Python repo) owns model training, active learning, DrBERT tokenization, dataset curation, the batch worker, hardware selection, and model-specific inference rules. `redsan` owns the R integration: data structures (`data.frame`, `edsan_event_bundle`), warehouse table contracts (no list-columns), environment discovery (`REDSAN_PYTHON_PATH`), artifact validation, and identity-safe cohort mapping. Do not duplicate training or model-specific documentation inside `redsan`. See [`docs/adr/0001-decouple-drbert-trimmer-from-redsan.md`](docs/adr/0001-decouple-drbert-trimmer-from-redsan.md).

---

## Key Reference Documentation

- **Trimmer artifact validation**: See [`tools/README.md`](tools/README.md).
- **Hospital database connection & driver setup**: See [`docs/source-access/`](docs/source-access).

---

## Agent skills

### Issue tracker
GitHub Issues (`gh` CLI) with PR triage disabled. See `docs/agents/issue-tracker.md`.

### Triage labels
Five canonical roles (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs
Single-context (`CONTEXT.md` at root, ADRs in `docs/adr/`). See `docs/agents/domain.md`.
