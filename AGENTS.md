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
| **Input** | Scalar character string (`text`) | Data frame or tibble with `RECTXT` column |
| **Output** | List with `text` and `removed_intervals` | Augmented data frame with `RECTXT_TRIMMED` and `TRIM_PRESERVED_INTERVALS` |
| **Strategy** | Deterministic removal of known CHU Rouen letterhead patterns | Contextual sequence classification using `DrBERT/DrBERT-7GB` |
| **Grounding** | Negative: tracks removed character spans | Positive: tracks preserved clinical character intervals `[start, end]` |
| **Runtime** | Pure R, zero external dependencies | Background Python worker (`processx`) with `onnxruntime` |
| **Offline HDW** | Built-in | Requires one-time `edsan_install_trimmer("path/to/zip")` |

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

