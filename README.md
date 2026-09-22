# redsan

`redsan` is a small R package for retrieving and normalizing EDSAN health data
warehouse sources. It is intended to be the executable source-access layer:
module query rules, batching, parsing, and normalized source tables live here.

Downstream projects should use `redsan` outputs for evidence selection and
measurement rather than reimplementing EDSAN batching or payload parsing.

## Source model

Use `edsan_sources()` to inspect the package's known source contracts:

```r
edsan_sources()
edsan_sources("pmsi")
edsan_sources("pmsi", "diag")
```

The registry records each module's normalized table, row grain, identifiers,
query date keys, default batching key, and source time kind.
Across modules, each `ELTID` belongs to exactly one `EVTID`, and each `EVTID`
belongs to exactly one `PATID`. This provenance relationship does not imply
that `ELTID` alone is always sufficient for normalized row uniqueness.
All normalized modules expose that source-element coordinate as `ELTID`.
`BIOL_ID` and `VIRO_ID` are accepted only when reading older biology and
virology artifacts and are converted to `ELTID` at normalization or bundling.

Current modules:

- `doceds`: clinical documents, point time through `RECDATE`
- `pmsi`: stays, acts, and diagnoses, with stay intervals through `DATENT` and
  `DATSORT`
- `biol`: biology results, point time through `DATEXAM`
- `viro`: virology results, point time through `DATEPRELEV`

## Basic workflow

Live retrieval through `get_edsan()` requires the EDSAN client package `d2imr`
to be installed in the calling environment.

```r
raw_pmsi <- get_edsan(
  module = "pmsi",
  what = "data",
  query = list(DATENT = c("2024-01-01", "2024-01-31")),
  fields = c(
    "PATID", "EVTID", "ELTID", "DATENT", "DATSORT", "SEJUM", "SEJUF",
    "SRC", "DALL"
  ),
  process = FALSE
)

pmsi <- process_pmsi(raw_pmsi)
pmsi$main
pmsi$actes
pmsi$diag

pmsi_all_sources <- process_pmsi(raw_pmsi, source_policy = "all")
```

`process_pmsi()` returns exactly `list(main, actes, diag)`. Its default
`source_policy = "c_over_dw"` applies the PMSI rule `C > DW` within each
`PATID + EVTID + SEJUM + SEJUF`: `DW` is removed where `C` exists and remains
the fallback otherwise. `source_policy = "all"` retains every normalized
`main` row. Event limits inherited by `actes` and `diag` are always derived
from the complete `main` before that policy is applied; the two detail tables
are not source-filtered. `process_pmsi()` reuses `label_pmsi()` to add the
matching CIM-10 `CODE_LABEL` to `diag` and CCAM/CDAM `CODEACTE_LABEL` to
`actes`. Original codes and rows are preserved; unknown codes receive a
missing label.

The same choice is available without breaking the retrieval flow:

```r
pmsi_all_sources <- get_edsan(
  module = "pmsi",
  what = "data",
  query = list(DATENT = c("2024-01-01", "2024-01-31")),
  source_policy = "all"
)
```

Passing `process = FALSE` to `get_edsan()` instead keeps the raw payload
available when retrieval and normalization need to be audited separately.

```r
raw_biol <- get_edsan(
  module = "biol",
  what = "data",
  query = list(DATEXAM = "{2024-01-01,2024-01-31}")
)

biology <- process_biol(raw_biol)
```

`process_biol()` reuses `label_biol()` to add the matching
`TYPEANA_LABEL`. Original analyte codes and rows are preserved; unknown codes
receive a missing label.

## Reference mappings

`edsan_references()` lists the mappings distributed with the package.
`edsan_reference()` returns one normalized mapping as a tibble.
`process_pmsi()` and `process_biol()` call their labelling helpers
automatically; the same helpers can enrich older normalized artifacts without
replacing their source codes.

Not every mapping has a labelling helper. `ghm`, `modeent`, `modesort`, and
`rectypes` describe columns that `redsan` already normalizes (`GHM`, `MODEENT`,
and `MODESORT` in `pmsi$main`, `RECTYPE` in DOCEDS) but are not joined
automatically; `bact` describes the EDSAN `bact` module, which `redsan` does not
retrieve yet. All of them are available through `edsan_reference()` for explicit
joins. A code whose label the source system leaves undocumented is retained with
an `NA` label rather than dropped.

```r
edsan_references()

labelled_pmsi <- label_pmsi(pmsi)
labelled_pmsi$diag
labelled_pmsi$actes

biology_labelled <- label_biol(biology)
```

`label_biol()` uses `TYPEANA` for the biology reference. `label_pmsi()` uses
`diag = CODE` for CIM-10 and
`NOMENCLATURE + CODEACTE` for the combined acts reference, which covers CCAM,
CDAM, CSARR, and NGAP. Unmatched codes
are retained with an `NA` label. The underlying references remain available
for custom joins:

```r
actes_ref <- edsan_reference("actes")
actes_labelled <- dplyr::left_join(
  pmsi$actes,
  actes_ref,
  by = c("NOMENCLATURE", "CODEACTE")
)
```

## Event bundles

`get_event_bundle()` retrieves the normalized output of several modules for one
`EVTID`. Each module follows its normal `get_edsan()` retrieval and field
defaults; the bundle layer adds no clinical or content filtering after
normalization. By default it uses every module in `edsan_sources()`; callers
may instead request modules explicitly.

```r
bundle <- get_event_bundle("123456789")

bundle <- get_event_bundle(
  "123456789",
  modules = c("doceds", "pmsi", "biol")
)

bundle$sources$doceds
bundle$sources$pmsi$main
bundle$sources$pmsi$actes
bundle$sources$pmsi$diag
bundle$sources$biol
```

`get_event_bundle()` is a wrapper around `get_event_bundles()`: it retrieves
through the same code path and only unwraps the single bundle, so normalization
and reference labels are identical in both forms. `bundle$sources$biol`
therefore carries `TYPEANA_LABEL` and the PMSI tables carry their CIM-10 and
CCAM/CDAM labels. `build_event_bundles()` also labels a `biol` source that
carries `TYPEANA` without `TYPEANA_LABEL`, so bundles assembled from biology
artifacts normalized before labelling existed expose the same columns. It also
renames legacy `BIOL_ID` and `VIRO_ID` columns to canonical `ELTID`.

Printing the bundle reports compact row counts while leaving the normalized
source objects unchanged. Retrieval is fail-fast: if one requested module
fails, `get_event_bundle()` does not return a silently partial bundle.

`render_event_bundle()` serializes the retrieved object to neutral JSON. By
default every source already present in the bundle is rendered; callers may
select whole retrieved sources without triggering new EDSAN calls.

```r
full_context <- render_event_bundle(bundle)

compact_context <- render_event_bundle(
  bundle,
  pretty = FALSE
)

documents_and_biology <- render_event_bundle(
  bundle,
  sources = c("doceds", "biol")
)
```

The renderer preserves all rows and columns of the selected sources. It does
not decide which information is clinically relevant and does not construct a
model-specific prompt.

## Trimming DOCEDS boilerplate

For contextual document trimming, `redsan` provides `trim_doceds_onnx()`, backed
by a compatible versioned runtime artifact from `edsan-doc-trimmer`.

It can be applied at any level of granularity:

```r
# 1. On a simple data frame or tibble (adds RECTXT_TRIMMED, TRIM_REDUCTION_PCT,
#    and TRIM_PRESERVED_INTERVALS)
clean_table <- trim_doceds_onnx(bundle$sources$doceds)

# 2. Directly on a character vector of texts
clean_texts <- trim_doceds_onnx(bundle$sources$doceds$RECTXT)

# 3. On a single event bundle
clean_bundle <- trim_doceds_onnx(bundle)

# 4. In batch across an entire cohort (e.g. 779 stays)
clean_cohort <- trim_doceds_onnx(denut)
```

Cohort batching sends all non-empty texts in one worker request and maps results
back by document identity. Original rows, source identifiers, unrelated columns,
classes, names, and attributes are preserved. Empty DOCEDS tables receive the
same three output columns without starting the worker.

The versioned runtime artifact must contain the model, tokenizer, worker, and an
`artifact.json` manifest declaring the compatible worker contract. `redsan`
validates request/result identity and verifies that every reported preserved
interval matches the original text exactly. `trimmed_text` must contain those
ordered interval contents without introducing other text, while the worker owns
their whitespace assembly. Model choice and inference rules remain owned by the
runtime artifact.

Before a release claims compatibility with a specific artifact, install the
package candidate and run the acceptance gate against that exact unpacked
artifact and Python environment:

```sh
Rscript tools/check_doceds_trimmer_artifact.R /path/to/python /path/to/versioned/artifact
```

This invokes the artifact through the public `trim_doceds_onnx()` interface and
fails unless the versioned protocol, output schema, identity mapping, and exact
source grounding are accepted. Protocol-fixture unit tests do not substitute for
this gate. Model development and behavior are documented in the
[`edsan-doc-trimmer`](https://github.com/FrancescoMonti-source/edsan-doc-trimmer) repository.

## Privacy

Request only the fields needed for the task. Keep patient-derived exports,
clinical text, and analysis artifacts outside version control. Prefer aggregate
counts in logs and examples.
