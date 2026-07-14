# redsan 0.2.0

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
