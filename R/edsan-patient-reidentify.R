# EDSaN CT patient reidentification -----------------------------------------

.edsan_ct_patient_call <- function(patid, ks_path = NULL) {
  if (!requireNamespace("d2imr", quietly = TRUE)) {
    stop("Package `d2imr` is required for EDSaN CT patient reidentification.",
         call. = FALSE)
  }
  if (!requireNamespace("httr", quietly = TRUE)) {
    stop("Package `httr` is required for EDSaN CT patient reidentification.",
         call. = FALSE)
  }

  active_path <- getExportedValue("d2imr", "get_activ_keystore_path")
  unlock <- getExportedValue("d2imr", "d2im_keystore.unlock")
  keystore_get <- getExportedValue("d2imr", "d2im_keystore.get")

  if (is.null(ks_path)) ks_path <- active_path()
  if (!is.character(ks_path) || length(ks_path) != 1L || is.na(ks_path) ||
      !nzchar(ks_path)) {
    stop("No active d2imr keystore path is available.", call. = FALSE)
  }
  if (!isTRUE(unlock(ks_path))) {
    stop("Unable to unlock the active d2imr keystore.", call. = FALSE)
  }

  api_url <- as.character(keystore_get("ws.edsan-ct.url"))
  usr <- as.character(keystore_get("ws.edsan-ct.usr"))
  pwd <- as.character(keystore_get("ws.edsan-ct.pwd"))

  proxy_fn <- tryCatch(
    getFromNamespace("d2im_wsc.proxy_config", "d2imr"),
    error = function(e) NULL
  )
  proxy <- if (is.function(proxy_fn)) proxy_fn() else httr::config()

  response <- httr::GET(
    sprintf(
      "%s/getPatientReidentificationInformations/%s",
      sub("/$", "", api_url),
      utils::URLencode(patid, reserved = TRUE)
    ),
    httr::authenticate(usr, pwd),
    httr::add_headers(Accept = "application/json"),
    httr::config(
      ssl_verifypeer = FALSE,
      ssl_verifyhost = 0L,
      timeout = 30
    ),
    proxy
  )

  status <- httr::status_code(response)
  if (status < 200L || status >= 300L) {
    stop(
      "EDSaN CT patient reidentification failed with HTTP status ", status, ".",
      call. = FALSE
    )
  }

  payload <- httr::content(response, as = "parsed")
  if (!is.list(payload) || !is.list(payload$patient)) {
    stop("EDSaN CT returned an unrecognized patient reidentification response.",
         call. = FALSE)
  }

  payload$patient
}

#' Retrieve patient identity information from an EDSaN PATID
#'
#' Uses the EDSaN CT endpoint
#' `getPatientReidentificationInformations/{patId}`. This is distinct from
#' identifier correspondence: it returns the patient identity fields exposed by
#' that endpoint. Fields not exposed by the endpoint (for example death-status
#' fields from other sources) are deliberately not inferred or joined here.
#'
#' @param patids Character vector of EDSaN patient identifiers (`PATID`).
#' @param ks_path Optional d2imr keystore path. When `NULL`, the active keystore
#'   is used.
#' @return A tibble with `PATID` followed by the patient fields returned by EDSaN
#'   CT. Different server versions may expose different patient columns.
#' @details This function deliberately exposes directly identifying patient data.
#' @export
edsan_reidentify_patient <- function(patids, ks_path = NULL) {
  patids <- .edsan_ct_validate_ids(patids, require_character = TRUE)

  rows <- lapply(patids, function(patid) {
    patient <- .edsan_ct_patient_call(patid, ks_path = ks_path)
    values <- lapply(patient, function(x) {
      if (is.null(x) || length(x) == 0L) return(NA_character_)
      paste(as.character(x), collapse = ";")
    })
    tibble::as_tibble(c(list(PATID = patid), values), .name_repair = "minimal")
  })

  dplyr::bind_rows(rows)
}
