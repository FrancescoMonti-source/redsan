# CORA observations in EDSaN event bundles

CORA observations (diététique, kinésithérapie, ergothérapie, etc.) are not
currently included in the EDSaN DOCEDS service. The bundle API can now
include them directly without exporting an intermediate Excel file.

```r
bundles <- redsan::edsan_get_event_bundles(
  evtids,
  modules = "all",
  include_observations = c("DIET", "KINE", "ERGO")
)

# Available types are read from the live CORA observations catalog:
redsan::cora_observation_catalog()
# Use include_observations = "all" for all active categories.
```

By default, `include_observations = NULL`: no CORA lookup is made, to
avoid requiring database privileges for existing callers.

## Other entry points

```r
observations <- redsan::cora_get_observations(
  evtids, id_type = "EVTID", types = "DIET"
)
bundles <- redsan::cora_add_observations(bundles, observations)
```

`cora_add_observations()` works on one bundle or a list and is idempotent.
When DOCEDS was not requested, it creates `sources$doceds` from CORA
rows alone; it does **not** issue an additional EDSaN DOCEDS request.

`cora_get_diet()` is a shortcut for `cora_get_observations(types = "DIET")`.
The old `get_cora_diet()` remains available with its historical 11-column
output and a deprecation warning.

## Row contract and provenance

- Observations are rows in `sources$doceds`, not a separate EDSaN module.
- `RECTXT` preserves the original decompressed clinical content; trimming,
  if requested, is performed later using `trim_doceds_onnx()`.
- `RECTYPE = "FICHE_DIET"` remains unchanged for dietary observations.
  Other observation categories use `RECTYPE = "CORA_<CODE>"`.
- `SOURCE = "CORA"` distinguishes imported rows from native DOCEDS rows.
- `ELTID = "CORA:<TYPEDOC>:<NODOCUMENT>"` is source-qualified and stable.
  Original `NODOCUMENT`, `TYPEDOC`, `NOSOUSVOLET`, `OBS_CODE`,
  `TYPEEVT`, and `CORA_NOEVT` are retained.
- Native DOCEDS rows are left unchanged. Basic patient identity fields on
  CORA rows can be derived from an existing native DOCEDS row for that EVTID.
  When no native DOCEDS exists, those identity fields may be missing.

IEP and EVTID correspondence uses EDSaN CT. A missing mapping yields no
attachment for that EVTID; technical errors raise an exception, and ambiguous
mappings are rejected rather than guessed.

The default `event_type = c("H", "R")` supports hospital movement and
rendez-vous documents. SQL queries batch IEPs into groups of at most 900
and load only documents belonging to the selected observation sub-volets.

## Validation before deployment

Unit tests use mocked CORA/CT responses. The following checks require the
hospital environment and must be completed before adopting the workflow
for production or removing the old Excel script:

1. Check `cora_observation_catalog()` against `ICSF.T_SOUS_VOLET` and
   confirm active categories and query permissions.
2. Retrieve a small mixed DIET/KINE/ERGO EVTID sample and verify H/R linking,
   documents and original keys against the source system.
3. Confirm `FICHE_DIET` compatibility with the downstream denutrition corpus,
   including stays with no EDSaN DOCEDS records.
4. Exercise `trim_doceds_onnx()` on the merged bundles and compare clinical
   content, including converted Wingdings checkbox markers.
5. Check performance on representative larger cohorts and inspect missing
   or ambiguous CT mappings.

The downstream `redsan-coding` Excel workflow is intentionally not modified
by this change.
