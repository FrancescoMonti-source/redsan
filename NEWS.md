# redsan 0.4.0

- Add `trim_doceds_text()`, which removes the administrative frame from one
  DOCEDS document — letterhead, correspondence block, RGPD notice, unfilled
  identity banner, page furniture, pasted laboratory table, and the placeholder
  and fill-run residue a Word template leaves behind — and reports every span it
  took in the coordinates of the original document. Add `doceds_family_chars()`
  to aggregate the per-family counts across documents.

  The rules were measured against a corpus of 64,871 documents and 205 M
  characters, where they remove 36.9 percent of it. They come from
  `redsancoding`, which had been carrying document normalization it should only
  have been consuming. Nothing about what they match changed in the move.

  Two properties are the reason this can be trusted, and both are load-bearing:
  every rule contributes spans rather than editing the string, so lines carrying
  a measured constant are subtracted before a single cut is applied; and every
  per-rule count is standalone and overlapping, so only `net_removed_chars` is a
  total. The families are site-specific to that corpus, and a family that fires
  on nothing there is wrong rather than inapplicable.

- Add `doceds_trim_spec()`, reporting the rule names, the thresholds and the
  family list the trimmer actually applies, together with the installed package
  version. A trimmed document is not self-describing: two runs a year apart can
  differ because the families changed, because a bound moved, or because neither
  did. Consumers that record provenance should read this rather than keeping
  their own copy of a rule name — a copy reports what the caller believes ran,
  which stops being true the moment the two drift apart.

- Add `tools/`, the three instruments the trimming rules are priced and policed
  by: an exploration of what noise still reaches a reader, a per-family
  measurement, and a prose audit that reads every removed span looking for
  clinical narrative. `tools/README.md` records the measured baseline, the
  residue that was accepted with its number, and the reasoning behind each rule.

# redsan 0.3.0

- Add `label_doceds()`, applied by `process_doceds()` and guaranteed on every
  event bundle, joining `RECTYPE` against the packaged `rectypes` reference to
  add `RECTYPE_LABEL`. Documents now carry an authoritative type label the same
  way biology carries `TYPEANA_LABEL`. A payload without `RECTYPE` is left
  untouched.

- Add seven packaged references: `bact` (bacteriology analytes, kept separate
  from `bio` because shared `TYPEANA` codes carry module-specific labels),
  `csarr` and `ngap` (act nomenclatures now part of the derived `actes`
  reference, so `label_pmsi()` applies them through `NOMENCLATURE + CODEACTE`),
  and `ghm`, `modeent`, `modesort`, `rectypes` for columns `redsan` already
  normalizes but does not label automatically.

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
