#' Prepare raw BIOL/VIRO payload into per-exam result tables
#'
#' Converts a list of BIOL API entries (lab exams) into a list of tibbles where
#' each tibble contains one row per result in `RESULTATS`, with exam-level
#' metadata replicated across those rows.
#'
#' Entries without usable results are dropped:
#' - non-list or `NULL` entries
#' - missing `RESULTATS`
#' - `RESULTATS` is a data.frame with zero rows
#'
#' The `BIOL_ID` column is derived from the list names/indices via `purrr::imap()`
#' and can be used to keep traceability to the source exam entry within the input
#' list.
#'
#' @param data List of BIOL API entries. Each element is expected to be a list
#'   containing metadata fields (e.g., `PATID`, `EVTID`, ...) and a `RESULTATS`
#'   element (data.frame-like) holding lab results.
#'
#' @return A list of tibbles (possibly empty). Each tibble has:
#' - metadata columns: `PATID`, `EVTID`, `ELTID`, `BIOL_ID`, `DATEXAM`, `SEJUM`,
#'   `SEJUF`, `PATBD`, `PATAGE`, `PATSEX`, `CSTE_LABO`
#' - all columns present in `RESULTATS`
#'
#' @examples
#' x <- list(
#'   examA = list(
#'     PATID = "P1", EVTID = "E1", ELTID = "L1",
#'     DATEXAM = "2020-01-01 08:30", CSTE_LABO = "LAB1",
#'     RESULTATS = data.frame(ANALYTE = "Hb", VALEUR = "13.2")
#'   ),
#'   examB = list(PATID = "P2", RESULTATS = data.frame())  # dropped
#' )
#' out <- .biol_prepare(x)
#' length(out)  # 1
#' out[[1]]
#'
#' @importFrom purrr imap compact
#' @importFrom tibble tibble as_tibble
#' @importFrom dplyr bind_cols
#' @keywords internal
#' @noRd
.biol_prepare <- function(data, id_col = "BIOL_ID", date_col = "DATEXAM", extra_meta = "ELTID") {
  # data: list of lab exams; each element has metadata + RESULTATS (data.frame)
  identifiers <- c("PATID", "EVTID", id_col, extra_meta)
  get_value <- function(entry, name) {
    value <- entry[[name]]
    if (is.null(value) || length(value) == 0) return(NA_character_)
    if (name %in% identifiers) {
      return(.edsan_as_identifier(value)[[1]])
    }
    as.character(value)[[1]]
  }

  purrr::imap(data, function(entry, biol_id) {
    if (is.null(entry) || !is.list(entry)) return(NULL)
    results <- entry$RESULTATS
    if (is.null(results) || (is.data.frame(results) && nrow(results) == 0)) return(NULL)

    meta <- tibble::tibble(
      PATID = get_value(entry, "PATID"),
      EVTID = get_value(entry, "EVTID"),
      .SOURCE_ID = .edsan_as_identifier(biol_id)[[1]],
      .SOURCE_DATE = get_value(entry, date_col),
      SEJUM = get_value(entry, "SEJUM"),
      SEJUF = get_value(entry, "SEJUF"),
      PATBD = get_value(entry, "PATBD"),
      PATAGE = get_value(entry, "PATAGE"),
      PATSEX = get_value(entry, "PATSEX"),
      CSTE_LABO = get_value(entry, "CSTE_LABO")
    )

    names(meta)[names(meta) == ".SOURCE_ID"] <- id_col
    names(meta)[names(meta) == ".SOURCE_DATE"] <- date_col

    for (col in rev(extra_meta)) {
      if (!col %in% names(meta)) {
        existing_names <- names(meta)
        evtid_position <- match("EVTID", existing_names)
        meta[[col]] <- get_value(entry, col)
        meta <- meta[c(
          existing_names[seq_len(evtid_position)],
          col,
          existing_names[-seq_len(evtid_position)]
        )]
      }
    }

    # Ensure results is a tibble
    res_tbl <- tibble::as_tibble(results)

    # Repeat meta for each result row, then bind columns
    out <- dplyr::bind_cols(meta[rep(1, nrow(res_tbl)), , drop = FALSE], res_tbl)

    out
  }) %>% purrr::compact()
}

#' Flatten and parse BIOL lab results into a single tibble
#'
#' Accepts either:
#' - a raw list of BIOL API entries (lab exams), or
#' - an existing data.frame/tibble already in "results" form.
#'
#' When given a raw list, `.biol_prepare()` is used to build per-exam tibbles,
#' which are then row-bound. If no usable results are found, an empty tibble is
#' returned.
#'
#' If `DATEXAM` is present, it is parsed to `POSIXct` via `.pmsi_parse_datetime()`
#' and `HEURE_DATEXAM` is derived via `.pmsi_time_hms()`, returning an `hms` time
#' only when the raw `DATEXAM` value contained an explicit time component.
#'
#' @param data Either a list of BIOL API entries (raw) or a data.frame/tibble
#'   already containing results and metadata columns.
#'
#' @return A tibble of BIOL results. When present, `DATEXAM` is converted to
#'   `POSIXct` and a `HEURE_DATEXAM` (`hms`) column is added.
#'
#' @examples
#' raw <- list(list(
#'   PATID="P1", EVTID="E1", ELTID="L1",
#'   DATEXAM="2020-01-01 08:30",
#'   RESULTATS=data.frame(ANALYTE="Hb", VALEUR="13.2")
#' ))
#' .biol_results(raw)
#'
#' already <- tibble::tibble(PATID="P1", DATEXAM="2020-01-01", ANALYTE="Hb")
#' .biol_results(already)
#'
#' @importFrom tibble tibble as_tibble
#' @importFrom dplyr bind_rows
#' @keywords internal
#' @noRd
.biol_results <- function(data, id_col = "BIOL_ID", date_col = "DATEXAM", extra_meta = "ELTID") {
  # Accept either raw list (API chunks) or already prepared list of tibbles
  if (is.list(data) && !is.data.frame(data)) {
    rows <- .biol_prepare(data, id_col = id_col, date_col = date_col, extra_meta = extra_meta)
    if (length(rows) == 0) return(tibble::tibble())
    df <- dplyr::bind_rows(rows)
  } else {
    df <- tibble::as_tibble(data)
  }

  # Parse source date and compute HEURE_* (only if time was explicit in raw string)
  if (date_col %in% names(df)) {
    raw <- as.character(df[[date_col]])
    df[[date_col]] <- .pmsi_parse_datetime(raw)
    df[[paste0("HEURE_", date_col)]] <- .pmsi_time_hms(df[[date_col]], raw)
  }

  if ("PATAGE" %in% names(df)) {
    df$PATAGE <- suppressWarnings(as.numeric(as.character(df$PATAGE)))
  }

  # EDSAN exposes numeric results in NUMRES and qualitative results in STRRES.
  # Raw API payloads may store each numeric scalar in a one-element list, while
  # prepared extracts already use a numeric vector. Normalize both shapes in
  # place and leave STRRES untouched.
  if ("NUMRES" %in% names(df)) {
    df$NUMRES <- suppressWarnings(as.numeric(as.character(df$NUMRES)))
  }

  df
}

#' Process BIOL results
#'
#' Flattens BIOL lab results from the EDSaN API into a single tibble.
#' If `DATEXAM` is present, it is parsed into `POSIXct` and `HEURE_DATEXAM`
#' (`hms`) is derived only when an explicit time is present in the raw string.
#'
#' @param data List of BIOL API entries (raw exams with `RESULTATS`) or a
#'   data.frame/tibble already in result form.
#'
#' @return A tibble with exam metadata columns and lab result columns. When
#'   available, adds `HEURE_DATEXAM`. `PATAGE` and `NUMRES`, when present, are
#'   numeric. Qualitative result fields such as `STRRES` are preserved.
#'
#' @examples
#' raw <- list(list(
#'   PATID="P1", EVTID="E1", ELTID="L1",
#'   DATEXAM="2020-01-01 08:30",
#'   RESULTATS=data.frame(ANALYTE="Hb", VALEUR="13.2")
#' ))
#' process_biol(raw)
#'
#' @export
process_biol <- function(data) {
  .edsan_normalize_identifier_columns(
    .biol_results(data),
    "biol",
    "results"
  )
}

#' Process VIRO results
#'
#' Flattens VIRO results from the EDSaN API into a single tibble. VIRO shares
#' the BIOL result shape but uses `VIRO_ID` for source traceability and
#' `DATEPRELEV` as its source date.
#'
#' @param data List of VIRO API entries (raw exams with `RESULTATS`) or a
#'   data.frame/tibble already in result form.
#'
#' @return A tibble with virology metadata columns and result columns. When
#'   available, adds `HEURE_DATEPRELEV`. `PATAGE` and `NUMRES`, when present, are
#'   numeric.
#'
#' @examples
#' raw <- list(list(
#'   PATID = "P1", EVTID = "E1",
#'   DATEPRELEV = "2024-01-01 08:30",
#'   RESULTATS = data.frame(ANALYTE = "PCR", STRRES = "NEGATIF")
#' ))
#' process_viro(raw)
#'
#' @export
process_viro <- function(data) {
  .edsan_normalize_identifier_columns(
    .biol_results(
      data,
      id_col = "VIRO_ID",
      date_col = "DATEPRELEV",
      extra_meta = character()
    ),
    "viro",
    "results"
  )
}
