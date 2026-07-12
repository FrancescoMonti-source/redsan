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

  out
}
