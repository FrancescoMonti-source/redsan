# Working in redsan

## Scope

`redsan` is an R package for retrieving, normalizing, and processing hospital electronic health record payloads from the EDSAN data warehouse (CHU de Rouen). It manages patient stay event bundles (`edsan_event_bundle`), document text processing (`DOCEDS`), PMSI stays/diagnoses, and laboratory/microbiology data.

## Coding Standards & Conventions

### R File Naming (`kebab-case`)
Source files in `R/` follow `<module>-<subsystem>.R`:
- `doceds-trim.R`, `doceds-trim-onnx.R`, `doceds-structure.R`, `doceds-trim-patterns.R`
- `event-bundle.R`, `event-bundle-render.R`
- `cora-diet.R`, `cora-explore.R`
- `edsan-ct.R`, `edsan-patient-reidentify.R`
*(Base single-word modules: `biol.R`, `cora.R`, `doceds.R`, `pmsi.R`, `sources.R`, `get_edsan.R`).*

### Collation Order Overrides (`zzz-*.R`)
R collates and loads source files alphabetically. Files prefixed with `zzz-`, `zzzz-`, or `zzzzz-` are deliberate **collation overrides** that monkey-patch or extend earlier function definitions (e.g. `zzz-cora-diet-rdv.R`, `zzzz-edsan-desktop-direct.R`). Do not rename them to earlier alphabetical names.

### R Function Naming (`snake_case`)
- **Public exported functions**: `<action>_<module>_<detail>()` (e.g. `trim_doceds_onnx()`, `trim_doceds_text()`, `process_doceds()`, `process_pmsi()`, `process_biol()`).
- **Internal helpers**: Dot-prefixed `.<module>_<helper>()` (e.g. `.edsan_get_trimmer_dir()`, `.widest_join()`, `.merge_intervals()`).

### Test Conventions
Unit tests live in `tests/testthat/test-<kebab-case>.R`, matching their source module (e.g. `test-doceds-trim-spec.R`, `test-doceds-trim-onnx.R`).

---

## Dual Document Trimmer Architecture

`redsan` provides two complementary trimmers for `DOCEDS` texts:

| Property | Heuristic Regex Trimmer (`trim_doceds_text`) | DrBERT ML Trimmer (`trim_doceds_onnx`) |
|---|---|---|
| **Location** | `R/doceds-trim.R` | `R/doceds-trim-onnx.R` |
| **Input** | Scalar character string (`text`) | Character vector, `data.frame`/`tibble`, single `edsan_event_bundle`, or list of bundles |
| **Output** | List with `text` and `removed_intervals` | Character vector, or augmented table/bundle(s) with `RECTXT_TRIMMED`, `TRIM_REDUCTION_PCT`, and `TRIM_PRESERVED_INTERVALS` (serialized JSON character) |
| **Strategy** | Deterministic removal of known CHU Rouen letterhead patterns | Contextual sequence classification using `DrBERT/DrBERT-7GB` |
| **Grounding** | Negative: tracks removed character spans | Positive: tracks preserved clinical character intervals `[start, end]` |
| **Runtime** | Pure R, zero external dependencies | Versioned background Python worker invoked with `processx` |
| **Batching** | Sequential character string mapping | High-performance cohort batching: extracts texts across stays, runs one forward pass, maps back without altering schemas |
| **Offline HDW** | Built-in | Requires one-time `edsan_install_trimmer("path/to/zip")` |

### Boundary with `edsan-doc-trimmer`
`edsan-doc-trimmer` (Python repo) owns model training, active learning, DrBERT tokenization, dataset curation, the batch worker, hardware selection, and model-specific inference rules. `redsan` owns the R integration: data structures (`data.frame`, `edsan_event_bundle`), warehouse table contracts (no list-columns), environment discovery (`REDSAN_PYTHON_PATH`), artifact validation, and identity-safe cohort mapping. Do not duplicate training or model-specific documentation inside `redsan`. See [`docs/adr/0001-decouple-drbert-trimmer-from-redsan.md`](docs/adr/0001-decouple-drbert-trimmer-from-redsan.md).

---

## Key Reference Documentation

- **Trimming heuristics rationale & prose audit**: See [`tools/README.md`](tools/README.md).
- **Hospital database connection & driver setup**: See [`docs/source-access/`](docs/source-access).

---

## Agent skills

### Issue tracker
GitHub Issues (`gh` CLI) with PR triage disabled. See `docs/agents/issue-tracker.md`.

### Triage labels
Five canonical roles (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs
Single-context (`CONTEXT.md` at root, ADRs in `docs/adr/`). See `docs/agents/domain.md`.
