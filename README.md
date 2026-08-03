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
artifacts normalized before labelling existed expose the same columns.

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

A DOCEDS document is mostly not clinical text. A consultation letter is fifty
lines of letterhead, correspondence block and RGPD notice around one paragraph,
plus whatever residue the Word template left behind. `trim_doceds_text()`
removes that frame from one document and reports exactly what it took.

```r
trimmed <- trim_doceds_text(bundle$sources$doceds$RECTXT[[1L]])
trimmed$text                  # what survives
trimmed$net_removed_chars     # the only total in the list
trimmed$removed_intervals     # every span removed, in original coordinates
```

The administrative families are normalization: they remove the document frame
and are meant never to touch what a clinician wrote. The optional `lab_table`
family is deliberately different. With `remove_lab_tables = TRUE` (the default),
it removes recognised pasted laboratory tables, including the clinical values
they contain. That switch is an explicit evidence-scope policy and can be set to
`FALSE` when those tables must remain visible.

Two properties are the reason it can be trusted, and both are worth knowing
before changing anything here:

- **Every rule contributes spans, none edits the string.** Candidate spans are
  collected in the coordinates of the original document, lines carrying a
  measured constant (`TA : 130/80`, `Poids : 144 kg`) are subtracted from them,
  and what survives is applied in one pass. That is what makes removals
  auditable and order-independent, and what lets a family that swallowed a vital
  sign give it back.
- **Every per-rule count is standalone.** They measure what a rule would remove
  on its own, so they overlap each other and must not be summed. Only
  `net_removed_chars` is a total. `doceds_family_chars()` aggregates the
  per-family counts across documents on the same basis.

`near_total_match_detected` is a diagnostic for one failure — a rule that ran
away on an unseen layout and matched essentially the whole document — and not a
safety margin. A document losing 99.4 percent is not clinically different from
one losing 99.6.

`doceds_trim_spec()` reports which rules ran, for a caller that wants to record
what produced a result alongside the result. Compare its `digest`: it is a
SHA-256 digest, computed with `digest::digest()` over canonical UTF-8 rule bytes
with R serialization disabled. It covers every pattern and threshold the
trimmer holds, and the set is read from the namespace rather than listed, so a
rule added tomorrow enters it by itself. The rule names carry no version on
purpose — a version somebody has to remember can only fail by staying put while
the rules move. What the digest does not cover is the code applying the rules,
which is what `version` records.

The families are **site-specific** to the Rouen corpus they were measured
against, and a family that fires on nothing is wrong rather than inapplicable.
`tools/` holds the three instruments that priced them and check the
administrative boundary, with the measured baseline and the reasoning in
`tools/README.md`. Read it before adding or widening a family.

## Privacy

Request only the fields needed for the task. Keep patient-derived exports,
clinical text, and analysis artifacts outside version control. Prefer aggregate
counts in logs and examples.
