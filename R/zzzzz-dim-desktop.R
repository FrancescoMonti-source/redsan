.redsan_keystore_context <- function(path = NULL) {
  if (.edsan_ct_valid_scalar(path)) {
    return(list(path = path, source = "explicit"))
  }

  if (!requireNamespace("d2imr", quietly = TRUE)) {
    return(list(path = NULL, source = "none"))
  }

  info <- .d2imr_keystore_info()
  if (!is.list(info) || is.null(info$path)) {
    return(list(path = NULL, source = "none"))
  }

  list(path = info$path, source = info$resolution_source)
}

.d2imr_keystore_info <- function() {
  tryCatch(
    getExportedValue("d2imr", "keystore_info")(),
    error = function(e) NULL
  )
}

.redsan_keystore_path <- function(path = NULL) {
  .redsan_keystore_context(path)$path
}

.redsan_keystore_has <- function(required_keys) {
  if (!requireNamespace("d2imr", quietly = TRUE)) return(FALSE)

  info <- .d2imr_keystore_info()
  if (!is.list(info) || is.null(info$path)) return(FALSE)

  checker <- tryCatch(
    getExportedValue("d2imr", "keystore_has"),
    error = function(e) NULL
  )
  if (!is.function(checker)) {
    stop("Package `d2imr` must export `keystore_has()` for workflow routing.",
         call. = FALSE)
  }

  tryCatch(
    isTRUE(checker(required_keys)),
    error = function(e) {
      stop(
        "Unable to inspect the active d2imr keystore capabilities: ",
        conditionMessage(e),
        call. = FALSE
      )
    }
  )
}

.redsan_workflow_capabilities <- function() {
  cora_configured <- .redsan_keystore_has(c(
    "db.cora.url", "db.cora.usr", "db.cora.pwd"
  ))
  edsan_ct_url_configured <- .edsan_ct_valid_scalar(
    .edsan_ct_configured_url(env = "edsan-ct")
  )
  edsan_ct_auth_available <- !is.null(.edsan_ct_keystore_auth(env = "edsan-ct")) ||
    .edsan_ct_interactive_available()

  list(
    pmsi = .redsan_keystore_has(c(
      "ws.edsan.url", "ws.edsan.usr", "ws.edsan.pwd"
    )),
    edsan_ct_cora = isTRUE(cora_configured) &&
      edsan_ct_url_configured &&
      isTRUE(edsan_ct_auth_available)
  )
}

# CORA uses the single path selected by d2imr, unless a caller supplies one.
.cora_keystore_value <- function(key, ks_path = NULL) {
  .cora_require_namespace("d2imr", "connection setup")

  context <- .redsan_keystore_context(ks_path)
  path <- context$path
  if (!.edsan_ct_valid_scalar(path) || !file.exists(path)) {
    stop(
      "No readable keystore is available for CORA. Check the active d2imr ",
      "keystore or supply an explicit path.",
      call. = FALSE
    )
  }

  getter <- tryCatch(
    getExportedValue("d2imr", "d2im_keystore.get"),
    error = function(e) NULL
  )
  if (!is.function(getter)) {
    stop(
      "Package `d2imr` must export `d2im_keystore.get()` for CORA connection setup.",
      call. = FALSE
    )
  }

  value <- tryCatch(
    getter(key = key, path = path),
    error = function(e) {
      stop(
        "Could not read CORA connection key `", key,
        "` from the resolved keystore: ", conditionMessage(e),
        call. = FALSE
      )
    }
  )

  value <- as.character(value)
  if (length(value) != 1L || is.na(value) || !nzchar(value)) {
    stop(
      "Required CORA connection key `", key,
      "` is missing or empty in the resolved keystore.",
      call. = FALSE
    )
  }

  value
}

.edsan_cora_iep_ipp_map <- function(ieps, query = .cora_query,
                                    max_in_ids = 900L) {
  ieps <- as.character(ieps)
  ieps <- unique(ieps[!is.na(ieps) & nzchar(trimws(ieps))])
  if (!length(ieps)) {
    return(tibble::tibble(IEP = character(), IPP = character()))
  }
  ieps <- trimws(ieps)
  if (any(!grepl("^[0-9]+$", ieps))) {
    stop("CORA IEP lookup accepts digit strings only.", call. = FALSE)
  }

  max_in_ids <- as.integer(max_in_ids)
  if (length(max_in_ids) != 1L || is.na(max_in_ids) || max_in_ids < 1L ||
      max_in_ids > 900L) {
    stop("`max_in_ids` must be an integer between 1 and 900.", call. = FALSE)
  }

  chunks <- split(seq_along(ieps), ceiling(seq_along(ieps) / max_in_ids))
  rows <- lapply(chunks, function(idx) {
    values <- paste0("'", ieps[idx], "'", collapse = ",")
    sql <- paste0(
      "SELECT DISTINCT s.NO_SEJOUR AS IEP, p.IPP_PATIENT AS IPP ",
      "FROM CORA_REC.TB_SEJOUR s ",
      "JOIN CORA_REC.TB_PATIENT p ON p.ID_PATIENT = s.ID_PATIENT ",
      "WHERE s.NO_SEJOUR IN (", values, ")"
    )
    query(sql)
  })

  map <- dplyr::bind_rows(rows)
  if (!is.data.frame(map) || !all(c("IEP", "IPP") %in% names(map))) {
    stop("CORA lookup did not return the expected IEP/IPP columns.",
         call. = FALSE)
  }

  map <- tibble::tibble(
    IEP = trimws(as.character(map$IEP)),
    IPP = trimws(as.character(map$IPP))
  )
  map <- unique(map[
    !is.na(map$IEP) & nzchar(map$IEP) & !is.na(map$IPP) & nzchar(map$IPP),
    , drop = FALSE
  ])

  counts <- table(map$IEP)
  ambiguous <- names(counts[counts > 1L])
  if (length(ambiguous)) {
    stop(
      "CORA returned multiple IPP values for IEP(s): ",
      paste(ambiguous, collapse = ", "),
      ". Refusing to choose one arbitrarily.",
      call. = FALSE
    )
  }

  missing_ieps <- setdiff(ieps, map$IEP)
  if (length(missing_ieps)) {
    map <- dplyr::bind_rows(
      map,
      tibble::tibble(IEP = missing_ieps, IPP = NA_character_)
    )
  }

  map[match(ieps, map$IEP), , drop = FALSE]
}

.edsan_evtid_patid_via_cora <- function(evtids, env = "edsan-ct",
                                         ks_path = NULL,
                                         query = .cora_query,
                                         translate = .edsan_ct_translate) {
  evtids <- unique(.edsan_ct_validate_ids(evtids, require_character = TRUE))
  if (!length(evtids)) {
    return(tibble::tibble(EVTID = character(), PATID = character()))
  }

  evtid_rows <- translate(
    ids = evtids,
    input_types = rep.int("EVTID", length(evtids)),
    direction = "edsan_to_his",
    env = env,
    ks_path = ks_path
  )
  evtid_iep <- tibble::tibble(
    EVTID = as.character(evtid_rows$input_id),
    IEP = as.character(evtid_rows$output_id)
  )

  valid_ieps <- unique(evtid_iep$IEP[
    !is.na(evtid_iep$IEP) & nzchar(evtid_iep$IEP)
  ])
  iep_ipp <- .edsan_cora_iep_ipp_map(valid_ieps, query = query)
  bridge <- dplyr::left_join(evtid_iep, iep_ipp, by = "IEP")

  valid_ipps <- unique(bridge$IPP[!is.na(bridge$IPP) & nzchar(bridge$IPP)])
  if (length(valid_ipps)) {
    ipp_rows <- translate(
      ids = valid_ipps,
      input_types = rep.int("IPP", length(valid_ipps)),
      direction = "his_to_edsan",
      env = env,
      ks_path = ks_path
    )
    ipp_patid <- tibble::tibble(
      IPP = as.character(ipp_rows$input_id),
      PATID = as.character(ipp_rows$output_id)
    )
    bridge <- dplyr::left_join(bridge, ipp_patid, by = "IPP")
  } else {
    bridge$PATID <- NA_character_
  }

  map <- unique(bridge[, c("EVTID", "PATID"), drop = FALSE])
  valid <- map[!is.na(map$PATID) & nzchar(map$PATID), , drop = FALSE]
  counts <- table(valid$EVTID)
  ambiguous <- names(counts[counts > 1L])
  if (length(ambiguous)) {
    stop(
      "CORA/EDSaN bridge returned multiple PATID values for EVTID(s): ",
      paste(ambiguous, collapse = ", "),
      ". Refusing to choose one arbitrarily.",
      call. = FALSE
    )
  }

  out <- tibble::tibble(EVTID = evtids)
  dplyr::left_join(out, valid, by = "EVTID")
}

# The EVTID -> PATID route is independent of how d2imr resolved the keystore.
# Capability-based workflow selection belongs to the downstream routing ticket.
.edsan_evtid_patid_map <- function(evtids, get = get_edsan) {
  evtids <- unique(.edsan_ct_validate_ids(evtids, require_character = TRUE))

  if (missing(get)) {
    capabilities <- .redsan_workflow_capabilities()
    if (!isTRUE(capabilities$pmsi) && isTRUE(capabilities$edsan_ct_cora)) {
      return(.edsan_evtid_patid_via_cora(evtids))
    }
    if (!isTRUE(capabilities$pmsi) && !isTRUE(capabilities$edsan_ct_cora)) {
      stop(
        "No configured keystore capability can resolve EVTID to PATID. ",
        "Configure the PMSI keys or the EDSaN CT and CORA fallback keys.",
        call. = FALSE
      )
    }
  }

  pmsi <- get(
    module = "pmsi",
    what = "idtriplets",
    query = list(EVTID = evtids),
    batch_ids_key = "EVTID",
    fields = c("PATID", "EVTID")
  )

  if (!is.data.frame(pmsi) || !all(c("EVTID", "PATID") %in% names(pmsi))) {
    stop("PMSI lookup did not return the expected EVTID/PATID columns.",
         call. = FALSE)
  }

  map <- pmsi[, c("EVTID", "PATID"), drop = FALSE]
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

  missing_ids <- setdiff(evtids, map$EVTID)
  if (length(missing_ids)) {
    map <- rbind(
      map,
      data.frame(
        EVTID = missing_ids,
        PATID = NA_character_,
        stringsAsFactors = FALSE
      )
    )
  }

  tibble::as_tibble(map)
}
