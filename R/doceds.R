#' Process DOCEDS documents
#'
#' Converts a flat DOCEDS payload to a tibble and normalizes its recorded time
#' and patient age. Native identifiers and document payload columns are
#' otherwise preserved.
#'
#' @param data A data frame returned by the DOCEDS data endpoint.
#'
#' @return A tibble. When `RECDATE` is present, it is parsed to `POSIXct` and
#'   `HEURE_RECDATE` records an `hms` value only when the source string included
#'   an explicit time. When `PATAGE` is present, it is converted to numeric.
#'
#' @examples
#' process_doceds(data.frame(
#'   ELTID = "L1",
#'   RECDATE = "2024-01-01 08:30",
#'   RECTYPE = "CR"
#' ))
#'
#' @export
process_doceds <- function(data) {
  if (!is.data.frame(data)) {
    stop("process_doceds() requires a data frame.", call. = FALSE)
  }

  out <- tibble::as_tibble(data)
  if ("RECDATE" %in% names(out)) {
    raw <- as.character(out$RECDATE)
    out$RECDATE <- .pmsi_parse_datetime(raw)
    out$HEURE_RECDATE <- .pmsi_time_hms(out$RECDATE, raw)
  }

  if ("PATAGE" %in% names(out)) {
    out$PATAGE <- suppressWarnings(as.numeric(as.character(out$PATAGE)))
  }

  out <- .edsan_normalize_identifier_columns(out, "doceds", "documents")
  # A payload without RECTYPE stays untouched: the label is an enrichment, not
  # a requirement the document grain has to satisfy.
  if ("RECTYPE" %in% names(out)) {
    out <- label_doceds(out)
  }
  out
}

#' Label DOCEDS document types
#'
#' Adds the authoritative document-type label to a normalized DOCEDS table by
#' matching `RECTYPE` against the packaged `rectypes` reference.
#'
#' @details
#' The join is exact, preserves every document row and leaves unmatched labels
#' as `NA`. An existing `RECTYPE_LABEL` column is refreshed from the packaged
#' reference. Several `rectypes` entries are documented with an empty label in
#' the source system; those keep `NA` rather than being dropped.
#'
#' A `RECTYPE` column that raw EDSAN payloads expose as a one-element list per
#' row is flattened to character before matching, since the reference key must
#' be atomic. Values holding several codes are collapsed with `;`, and empty
#' values become `NA`.
#'
#' @param documents A normalized documents data frame containing `RECTYPE`,
#'   normally returned by [process_doceds()].
#'
#' @return The input documents data frame with `RECTYPE_LABEL` added, and
#'   `RECTYPE` as character. All other columns and rows are preserved.
#'
#' @examples
#' documents <- data.frame(
#'   ELTID = c("L1", "L2"),
#'   RECTYPE = c("AJ", "PAS_UN_TYPE")
#' )
#' labelled <- label_doceds(documents)
#' labelled[c("RECTYPE", "RECTYPE_LABEL")]
#'
#' @export
label_doceds <- function(documents) {
  if (!is.data.frame(documents)) {
    stop("`documents` must be a data frame.", call. = FALSE)
  }
  if (!"RECTYPE" %in% names(documents)) {
    if (!nrow(documents)) {
      documents$RECTYPE <- character()
    } else {
      stop(
        "`documents` is missing required column: RECTYPE.",
        call. = FALSE
      )
    }
  }

  documents$RECTYPE <- .edsan_flatten_scalar_column(documents$RECTYPE)

  documents %>%
    dplyr::select(-dplyr::any_of("RECTYPE_LABEL")) %>%
    dplyr::left_join(edsan_reference("rectypes"), by = "RECTYPE")
}
