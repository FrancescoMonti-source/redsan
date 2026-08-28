# Unified EDSaN reidentification helpers ------------------------------------

.edsan_patid_ipp_map <- function(patids, env = "edsan-ct", ks_path = NULL) {
  patids <- unique(patids[!is.na(patids) & nzchar(patids)])
  if (!length(patids)) {
    return(tibble::tibble(PATID = character(), IPP = character()))
  }

  out <- .edsan_ct_translate(
    patids,
    rep.int("PATID", length(patids)),
    "edsan_to_his",
    env,
    ks_path
  )

  tibble::tibble(PATID = out$input_id, IPP = out$output_id)
}
