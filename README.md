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

Current modules:

- `doceds`: clinical documents, point time through `RECDATE`
- `pmsi`: stays, acts, and diagnoses, with stay intervals through `DATENT` and
  `DATSORT`
- `biol`: biology results, point time through `DATEXAM`

## Basic workflow

Live retrieval through `get_edsan()` requires the EDSAN client package `d2imr`
to be installed in the calling environment.

```r
raw_pmsi <- get_edsan(
  module = "pmsi",
  what = "data",
  query = list(DATENT = c("2024-01-01", "2024-01-31")),
  fields = c("PATID", "EVTID", "ELTID", "DATENT", "DATSORT", "DALL"),
  process = FALSE
)

pmsi <- process_pmsi(raw_pmsi)
pmsi$main
pmsi$actes
pmsi$diag

unit_stays <- prefer_pmsi_main_source(pmsi$main)
```

`process_pmsi()` keeps the complete normalized `main`, `actes`, and `diag`
tables. Event limits inherited by `actes` and `diag` are derived from the
complete `main`. Use `prefer_pmsi_main_source()` explicitly for a unit-level
view where source `C` takes precedence over `DW`; do not use that view to
derive global event limits. Passing `process = FALSE` to `get_edsan()` keeps
the raw payload available when retrieval and normalization need to be audited
separately.

```r
raw_biol <- get_edsan(
  module = "biol",
  what = "data",
  query = list(DATEXAM = "{2024-01-01,2024-01-31}")
)

biology <- process_biol(raw_biol)
```

## Privacy

Request only the fields needed for the task. Keep patient-derived exports,
clinical text, and analysis artifacts outside version control. Prefer aggregate
counts in logs and examples.
