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

The registry records each module's normalized table, row grain, native
identifiers, query date keys, default batching key, and source time kind.
Across modules, each `ELTID` belongs to exactly one `EVTID`, and each `EVTID`
belongs to exactly one `PATID`. This provenance relationship does not imply
that `ELTID` alone is always sufficient for normalized row uniqueness.

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
are not source-filtered.

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

## Event bundles

`get_event_bundle()` retrieves the normalized output of several modules for one
`EVTID` without selecting rows or fields within those sources. By default it
uses every module in `edsan_sources()`; callers may instead request whole
modules explicitly.

```r
bundle <- get_event_bundle("123456789")

bundle <- get_event_bundle(
  "123456789",
  sources = c("doceds", "pmsi", "biol")
)

bundle$sources$doceds
bundle$sources$pmsi$main
bundle$sources$pmsi$actes
bundle$sources$pmsi$diag
bundle$sources$biol
```

Printing the bundle reports compact row counts while leaving the normalized
source objects unchanged. Retrieval is fail-fast: if one requested module
fails, `get_event_bundle()` does not return a silently partial bundle.

## Privacy

Request only the fields needed for the task. Keep patient-derived exports,
clinical text, and analysis artifacts outside version control. Prefer aggregate
counts in logs and examples.
