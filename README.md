# redsan

`redsan` is a small R package for retrieving and normalizing EDSAN health data
warehouse sources. It is intended to be the executable source-access layer:
module query rules, batching, parsing, and normalized source tables live here.

Downstream projects should use `redsan` outputs for evidence selection and
measurement rather than reimplementing EDSAN batching or payload parsing.

## Public API grammar

The central API is domain-first. Function names identify the system or source
first and then the operation:

```r
# Discover available contracts and references.
edsan_source_catalog()
edsan_reference_catalog()

# Retrieve, then normalize an already available raw payload when needed.
raw <- edsan_get("pmsi", process = FALSE)
pmsi <- pmsi_normalize(raw)

# Translate identifiers through EDSaN CT.
ids <- edsan_ct(c("123456789"), from = "EVTID")

# Build locally, or retrieve and build.
sources <- list(pmsi = pmsi)
local_bundle <- edsan_event_bundle("123456789", sources)
retrieved_bundle <- edsan_get_event_bundle("123456789")
```

Operations ending in `_catalog()` discover available things. `_get()` retrieves
data, `_normalize()` transforms data already available to the caller, and
`_render()` serializes an existing object.

## Source model

Use `edsan_source_catalog()` to inspect the package's known source contracts:

```r
edsan_source_catalog()
edsan_source_catalog("pmsi")
edsan_source_catalog("pmsi", "diag")
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

Live retrieval through `edsan_get()` requires the EDSAN client package `d2imr`
to be installed in the calling environment.

```r
raw_pmsi <- edsan_get(
  module = "pmsi",
  what = "data",
  query = list(DATENT = c("2024-01-01", "2024-01-31")),
  fields = c(
    "PATID", "EVTID", "ELTID", "DATENT", "DATSORT", "SEJUM", "SEJUF",
    "SRC", "DALL"
  ),
  process = FALSE
)

pmsi <- pmsi_normalize(raw_pmsi)
pmsi$main
pmsi$actes
pmsi$diag

pmsi_all_sources <- pmsi_normalize(raw_pmsi, source_policy = "all")
```

`pmsi_normalize()` returns exactly `list(main, actes, diag)`. Its default
`source_policy = "c_over_dw"` applies the PMSI rule `C > DW` within each
`PATID + EVTID + SEJUM + SEJUF`: `DW` is removed where `C` exists and remains
the fallback otherwise. `source_policy = "all"` retains every normalized
`main` row. Event limits inherited by `actes` and `diag` are always derived
from the complete `main` before that policy is applied; the two detail tables
are not source-filtered. `pmsi_normalize()` reuses `label_pmsi()` to add the
matching CIM-10 `CODE_LABEL` to `diag` and CCAM/CDAM `CODEACTE_LABEL` to
`actes`. Original codes and rows are preserved; unknown codes receive a
missing label.

The same choice is available without breaking the retrieval flow:

```r
pmsi_all_sources <- edsan_get(
  module = "pmsi",
  what = "data",
  query = list(DATENT = c("2024-01-01", "2024-01-31")),
  source_policy = "all"
)
```

Passing `process = FALSE` to `edsan_get()` instead keeps the raw payload
available when retrieval and normalization need to be audited separately.

```r
raw_biol <- edsan_get(
  module = "biol",
  what = "data",
  query = list(DATEXAM = "{2024-01-01,2024-01-31}")
)

biology <- biol_normalize(raw_biol)
```

`biol_normalize()` reuses `label_biol()` to add the matching
`TYPEANA_LABEL`. Original analyte codes and rows are preserved; unknown codes
receive a missing label.

## Reference mappings

`edsan_reference_catalog()` lists the mappings distributed with the package.
`edsan_reference()` returns one normalized mapping as a tibble.
`pmsi_normalize()` and `biol_normalize()` call their labelling helpers
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
edsan_reference_catalog()

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

## Identifier correspondence

`edsan_ct()` resolves one explicit identifier type at a time. Results use real
identifier names and can be joined directly:

```r
evtids <- edsan_ct(iep, from = "IEP")
patient_ids <- edsan_ct(evtid, from = "EVTID", identity = TRUE)

dplyr::left_join(stays, patient_ids, by = "EVTID")
```

The four direct directions are `IPP -> PATID`, `PATID -> IPP`, `IEP -> EVTID`,
and `EVTID -> IEP`. A valid response without a correspondence is retained as
`status = "not_found"`; network, authentication, HTTP, and malformed-response
failures remain errors.

## Event bundles

`edsan_get_event_bundle()` retrieves the normalized output of several modules for one
`EVTID`. Each module follows its normal `edsan_get()` retrieval and field
defaults; the bundle layer adds no clinical or content filtering after
normalization. By default it uses every module in `edsan_source_catalog()`; callers
may instead request modules explicitly.

```r
bundle <- edsan_get_event_bundle("123456789")

bundle <- edsan_get_event_bundle(
  "123456789",
  modules = c("doceds", "pmsi", "biol")
)

bundle$sources$doceds
bundle$sources$pmsi$main
bundle$sources$pmsi$actes
bundle$sources$pmsi$diag
bundle$sources$biol
```

`edsan_get_event_bundle()` is a wrapper around `edsan_get_event_bundles()`: it retrieves
through the same code path and only unwraps the single bundle, so normalization
and reference labels are identical in both forms. `bundle$sources$biol`
therefore carries `TYPEANA_LABEL` and the PMSI tables carry their CIM-10 and
CCAM/CDAM labels. `edsan_event_bundles()` also labels a `biol` source that
carries `TYPEANA` without `TYPEANA_LABEL`, so bundles assembled from biology
artifacts normalized before labelling existed expose the same columns. It also
renames legacy `BIOL_ID` and `VIRO_ID` columns to canonical `ELTID`.

When normalized sources are already available, construct the bundle locally:

```r
sources <- list(pmsi = pmsi, biol = biology)
local_bundle <- edsan_event_bundle("123456789", sources)
```

Printing the bundle reports compact row counts while leaving the normalized
source objects unchanged. Retrieval is fail-fast: if one requested module
fails, `edsan_get_event_bundle()` does not return a silently partial bundle.

`edsan_render_event_bundle()` serializes the retrieved object to neutral JSON. By
default every source already present in the bundle is rendered; callers may
select whole retrieved sources without triggering new EDSAN calls.

```r
full_context <- edsan_render_event_bundle(bundle)

compact_context <- edsan_render_event_bundle(
  bundle,
  pretty = FALSE
)

documents_and_biology <- edsan_render_event_bundle(
  bundle,
  sources = c("doceds", "biol")
)
```

The renderer preserves all rows and columns of the selected sources. It does
not decide which information is clinically relevant and does not construct a
model-specific prompt.

## CORA and ICCA

Use `cora_query()` for read-only CORA SQL. Use `icca_get()` for EVTID-keyed
ICCA retrieval and `icca_query()` for unrestricted read-only ICCA queries.
Already compliant discovery and description names, including
`cora_describe_table()`, `cora_dig()`, `icca_catalog()`, `icca_describe()`, and
`icca_relations()`, remain unchanged.

```r
# Inspect a CORA table, then query its schema explicitly.
cora_describe_table("CORA_REC.MY_TABLE")
cora_rows <- cora_query(
  "SELECT * FROM CORA_REC.MY_TABLE WHERE ROWNUM <= 10"
)

# Retrieve a known ICCA source by EDSaN stay identifier.
ventilation <- icca_get(
  evtids = c("123456789"),
  source = "DAR.PatientVentilation"
)

# Use a direct read-only query when retrieval by EVTID is not the intended path.
icca_rows <- icca_query(
  "SELECT TOP 10 * FROM DAR.PatientVentilation"
)
```

## Deprecated names

The former names remain temporarily available and emit deprecation warnings.
They delegate to the canonical implementations:

| Former name | Canonical name |
| --- | --- |
| `get_edsan()` | `edsan_get()` |
| `edsan_sources()` | `edsan_source_catalog()` |
| `edsan_references()` | `edsan_reference_catalog()` |
| `build_event_bundle()`, `build_event_bundles()` | `edsan_event_bundle()`, `edsan_event_bundles()` |
| `get_event_bundle()`, `get_event_bundles()` | `edsan_get_event_bundle()`, `edsan_get_event_bundles()` |
| `render_event_bundle()` | `edsan_render_event_bundle()` |
| `process_doceds()` | `doceds_normalize()` |
| `process_pmsi()` | `pmsi_normalize()` |
| `process_biol()` | `biol_normalize()` |
| `process_viro()` | `viro_normalize()` |
| `query_cora()` | `cora_query()` |
| `get_icca()` | `icca_get()` |
| `query_icca()` | `icca_query()` |
| `edsan_pseudonymize()`, `edsan_reidentify()` | `edsan_ct()` |

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
