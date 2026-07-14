# redsan 0.1.2

- Add `prefer_pmsi_main_source()` for an explicit `C`-over-`DW` PMSI unit view.
- Derive PMSI detail-table event limits by `PATID + EVTID`, with an `EVTID`
  fallback for legacy payloads without `PATID`.
