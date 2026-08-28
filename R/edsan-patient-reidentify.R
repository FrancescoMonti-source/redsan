# EDSaN CT patient identity enrichment --------------------------------------

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
  if (identical(status, 204L)) return(NULL)
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

.edsan_patient_rows <- function(patids, ks_path = NULL) {
  patids <- unique(.edsan_ct_validate_ids(patids, require_character = TRUE))

  rows <- lapply(patids, function(patid) {
    patient <- .edsan_ct_patient_call(patid, ks_path = ks_path)
    if (is.null(patient)) return(tibble::tibble(PATID = patid))

    values <- lapply(patient, function(x) {
      if (is.null(x) || length(x) == 0L) return(NA_character_)
      paste(as.character(x), collapse = ";")
    })
    tibble::as_tibble(c(list(PATID = patid), values), .name_repair = "minimal")
  })

  dplyr::bind_rows(rows)
}

.edsan_evtid_patid_map <- function(evtids) {
  evtids <- unique(.edsan_ct_validate_ids(evtids, require_character = TRUE))

  pmsi <- get_edsan(
    module = "pmsi",
    what = "data",
    query = list(EVTID = evtids),
    batch_ids_key = "EVTID",
    fields = c("PATID", "EVTID"),
    process = TRUE,
    source_policy = "all"
  )

  main <- pmsi$main
  if (!is.data.frame(main) || !all(c("EVTID", "PATID") %in% names(main))) {
    stop("PMSI lookup did not return the expected EVTID/PATID columns.",
         call. = FALSE)
  }

  map <- main[, c("EVTID", "PATID"), drop = FALSE]
  map$EVTID <- as.character(map$EVTID)
  map$PATID <- as.character(map$PATID)
  map <- map[!is.na(map$EVTID) & nzchar(map$EVTID) &
             !is.na(map$PATID) & nzchar(map$PATID), , drop = FALSE]
  map <- unique(map)

  counts <- table(map$EVTID)
  ambiguous <- names(counts[counts > 1L])
  if (length(ambiguous)) {
    stop(
      "PMSI returned multiple PATID values for EVTID(s): ",
      paste(ambiguous, collapse = ", "),
      ". Refusing to choose one arbitrarily.",
      call. = FALSE
    )
  }

  missing <- setdiff(evtids, map$EVTID)
  if (length(missing)) {
    missing_rows <- data.frame(
      EVTID = missing,
      PATID = NA_character_,
      stringsAsFactors = FALSE
    )
    map <- rbind(map, missing_rows)
  }

  tibble::as_tibble(map)
}
