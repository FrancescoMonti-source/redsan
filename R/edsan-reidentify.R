# Unified EDSaN reidentification --------------------------------------------

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

#' Reidentify EDSaN identifiers
#'
#' Translates `PATID` to `IPP` or `EVTID` to `IEP`. With `identity = TRUE`, the
#' same function also resolves the patient and appends the identity fields exposed
#' by EDSaN CT. For an `EVTID`, the patient `PATID` is resolved internally from
#' the PMSI module before querying the patient-reidentification endpoint.
#'
#' @param ids Character vector of EDSaN `PATID` or `EVTID` values.
#' @param id_type Input type: `"PATID"` or `"EVTID"`.
#' @param identity If `FALSE` (default), return only the direct identifier
#'   correspondence. If `TRUE`, also return patient identifiers and identity
#'   fields.
#' @param env EDSaN CT web-service environment name used for identifier
#'   correspondence.
#' @param ks_path Optional d2imr keystore path. When `NULL`, the active path is
#'   used.
#'
#' @return With `identity = FALSE`, a two-column tibble (`PATID | IPP` or
#'   `EVTID | IEP`). With `identity = TRUE`, patient identifiers and the identity
#'   columns returned by `getPatientReidentificationInformations/{patId}` are
#'   appended. For `EVTID` input the result includes `PATID` and `IPP` as well.
#'   Missing correspondences remain `NA`. Multiple direct correspondences remain
#'   multiple rows. If PMSI maps one EVTID to multiple PATID values, the function
#'   errors rather than choosing one arbitrarily.
#'
#' @details This function deliberately exposes real hospital identifiers and,
#'   when `identity = TRUE`, directly identifying patient information.
#' @export
edsan_reidentify <- function(ids, id_type, identity = FALSE,
                             env = "edsan-ct", ks_path = NULL) {
  ids <- .edsan_ct_validate_ids(ids, require_character = TRUE)
  id_type <- match.arg(id_type, c("PATID", "EVTID"))
  if (!is.logical(identity) || length(identity) != 1L || is.na(identity)) {
    stop("`identity` must be TRUE or FALSE.", call. = FALSE)
  }

  type_spec <- .edsan_ct_specs$edsan_to_his$types[[id_type]]
  out <- .edsan_ct_translate(
    ids,
    rep.int(id_type, length(ids)),
    "edsan_to_his",
    env,
    ks_path
  )

  result <- tibble::tibble(out$input_id, out$output_id)
  names(result) <- c(id_type, type_spec$output_type)

  if (!isTRUE(identity)) return(result)

  if (identical(id_type, "PATID")) {
    patient <- .edsan_patient_rows(unique(result$PATID), ks_path = ks_path)
    return(dplyr::left_join(result, patient, by = "PATID"))
  }

  evtid_patid <- .edsan_evtid_patid_map(unique(result$EVTID))
  result <- dplyr::left_join(result, evtid_patid, by = "EVTID")

  patid_ipp <- .edsan_patid_ipp_map(result$PATID, env = env, ks_path = ks_path)
  result <- dplyr::left_join(result, patid_ipp, by = "PATID")

  patient <- .edsan_patient_rows(
    unique(result$PATID[!is.na(result$PATID) & nzchar(result$PATID)]),
    ks_path = ks_path
  )
  dplyr::left_join(result, patient, by = "PATID")
}
