# redsan 0.3.0

- Add `edsan_references()` and `edsan_reference()` for the code mappings
  distributed with the package.
- Add `label_pmsi()` to enrich normalized diagnoses with CIM-10 labels and acts
  with nomenclature-aware CCAM/CDAM labels. `process_pmsi()` reuses it for new
  normalized outputs, while older artifacts can call it directly.
- Add `label_biol()` to enrich normalized biology results with
  `TYPEANA_LABEL`. `process_biol()` reuses it for new outputs, while older
  artifacts can call it directly.
- Flatten `TYPEANA` to character when normalizing biology and virology results.
  Raw EDSAN payloads wrap each scalar in a one-element list, so `TYPEANA` reached
  the reference join as a list column and `process_biol()` failed with
  "Can't join `x$TYPEANA` with `y$TYPEANA` due to incompatible types".
  `label_biol()` applies the same flattening, so older artifacts can be labelled
  directly.
- Keep BIOL and VIRO batch results that the backend returns already in table
  form: `get_edsan()` bound them through `purrr::list_flatten()`, which exploded
  the table into a list of columns and silently dropped every row. Such batches
  are now row-bound and normalized as the result tables they are.
- Guarantee `TYPEANA_LABEL` in every event bundle: `build_event_bundles()`
  applies `label_biol()` to a `biol` source that carries `TYPEANA` without the
  label, so bundles built from older biology artifacts match those retrieved by
  `get_event_bundle()` and `get_event_bundles()`. `get_event_bundle()` and
  `build_event_bundle()` now delegate their whole body to the plural forms.
- Rename the module selector of `get_event_bundle()` and
  `get_event_bundles()` from `sources` to `modules`; `sources` continues to
  mean normalized payloads in bundle construction and rendering.

# redsan 0.2.0

- Normalize registered source identifiers as character without scientific
  notation, including event-bundle and identifier-query inputs.
- Make `C`-over-`DW` the default PMSI `main` source policy while deriving
  `actes` and `diag` event limits from the complete normalized `main` first.
- Add `source_policy = "all"` to `process_pmsi()` and `get_edsan()` for callers
  that need every normalized PMSI `main` source row.
- Rename the explicit helper to `prefer_pmsi_src_c_over_dw()` so its behavior
  is visible at the call site.
- Document the EDSAN identifier provenance `ELTID -> EVTID -> PATID` across
  source modules.

# redsan 0.1.2

- Add `prefer_pmsi_main_source()` for an explicit `C`-over-`DW` PMSI unit view.
- Derive PMSI detail-table event limits by `PATID + EVTID`, with an `EVTID`
  fallback for legacy payloads without `PATID`.
