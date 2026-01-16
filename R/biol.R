# -----------------------------
# BIOL processing API
# -----------------------------

.biol_prepare <- function(data) {
  # data: list of lab exams; each element has metadata + RESULTATS (data.frame)
  purrr::imap(data, function(entry, biol_id) {
    if (is.null(entry) || !is.list(entry)) return(NULL)
    results <- entry$RESULTATS
    if (is.null(results) || (is.data.frame(results) && nrow(results) == 0)) return(NULL)

    meta <- tibble::tibble(
      PATID = entry$PATID,
      EVTID = entry$EVTID,
      ELTID = entry$ELTID,
      BIOL_ID = biol_id,
      DATEXAM = entry$DATEXAM,
      SEJUM = entry$SEJUM,
      SEJUF = entry$SEJUF,
      PATBD = entry$PATBD,
      PATAGE = entry$PATAGE,
      PATSEX = entry$PATSEX,
      CSTE_LABO = entry$CSTE_LABO
    )

    # Ensure results is a tibble
    res_tbl <- tibble::as_tibble(results)

    # Repeat meta for each result row, then bind columns
    out <- dplyr::bind_cols(meta[rep(1, nrow(res_tbl)), , drop = FALSE], res_tbl)

    out
  }) %>% purrr::compact()
}

.biol_results <- function(data) {
  # Accept either raw list (API chunks) or already prepared list of tibbles
  if (is.list(data) && !is.data.frame(data)) {
    rows <- .biol_prepare(data)
    if (length(rows) == 0) return(tibble::tibble())
    df <- dplyr::bind_rows(rows)
  } else {
    df <- tibble::as_tibble(data)
  }

  # Parse DATEXAM and compute HEURE_DATEXAM (only if time was explicit in raw string)
  if ("DATEXAM" %in% names(df)) {
    raw <- as.character(df$DATEXAM)
    df$DATEXAM <- .pmsi_parse_datetime(raw)
    df$HEURE_DATEXAM <- .pmsi_time_hms(df$DATEXAM, raw)
  }

  df
}

#' Process BIOL results
#'
#' Flatten and parse BIOL lab results from the EDSAN API. DATEXAM is parsed
#' into POSIXct and HEURE_DATEXAM is derived only when a time is present.
#'
#' @param data List of BIOL API entries or a data.frame/tibble already in result form.
#' @return A tibble with metadata columns and lab results.
#' @export
process_biol <- function(data) {
  .biol_results(data)
}
