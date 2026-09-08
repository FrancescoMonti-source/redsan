# DIM desktop integration ---------------------------------------------------
#
# DIM_KEYSTORE_PATH is a workstation-level pointer to the generic DIM
# keystore. The keystore is not owned by CORA or EDSaN and may contain keys for
# several DIM services. redsan reads only the keys needed by the current
# backend and never changes the active d2imr keystore as a side effect.

.dim_keystore_path <- function(path = NULL) {
  if (.edsan_ct_valid_scalar(path)) return(path)

  configured <- Sys.getenv("DIM_KEYSTORE_PATH", unset = "")
  if (.edsan_ct_valid_scalar(configured)) return(configured)

  if (!requireNamespace("d2imr", quietly = TRUE)) return(NULL)
  active_path <- tryCatch(
    getExportedValue("d2imr", "get_activ_keystore_path"),
    error = function(e) NULL
  )
  if (!is.function(active_path)) return(NULL)

  value <- tryCatch(active_path(), error = function(e) NULL)
  if (!.edsan_ct_valid_scalar(value)) return(NULL)
  value
}

# CORA uses the generic DIM keystore when DIM_KEYSTORE_PATH is configured,
# otherwise retaining the historical d2imr active-keystore fallback.
.cora_keystore_value <- function(key, ks_path = NULL) {
  .cora_require_namespace("d2imr", "connection setup")

  path <- .dim_keystore_path(ks_path)
  if (!.edsan_ct_valid_scalar(path) || !file.exists(path)) {
    stop(
      "No readable DIM keystore is available. Set `DIM_KEYSTORE_PATH` or ",
      "activate a d2imr keystore.",
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
        "` from the DIM keystore: ", conditionMessage(e),
        call. = FALSE
      )
    }
  )

  value <- as.character(value)
  if (length(value) != 1L || is.na(value) || !nzchar(value)) {
    stop(
      "Required CORA connection key `", key,
      "` is missing or empty in the DIM keystore.",
      call. = FALSE
    )
  }

  value
}

# EDSaN may also inspect the generic DIM keystore for ws.edsan-ct.* keys, but a
# missing EDSaN entry simply falls through to interactive desktop credentials.
.edsan_ct_resolve_keystore_path <- function(ks_path = NULL) {
  .dim_keystore_path(ks_path)
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
  out <- dplyr::left_join(out, valid, by = "EVTID")
  out
}

# Preserve the historical PMSI/idtriplets implementation everywhere except a
# workstation that explicitly configures DIM_KEYSTORE_PATH. On that desktop
# path, use only the already-validated EDSaN CT + CORA services.
.edsan_evtid_patid_map <- function(evtids, get = get_edsan) {
  evtids <- unique(.edsan_ct_validate_ids(evtids, require_character = TRUE))

  dim_path <- Sys.getenv("DIM_KEYSTORE_PATH", unset = "")
  if (missing(get) && .edsan_ct_valid_scalar(dim_path)) {
    return(.edsan_evtid_patid_via_cora(evtids))
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
