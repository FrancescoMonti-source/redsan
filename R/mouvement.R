#' Normalize EDSAN movement records
#'
#' Convert movement records or a data frame into a tibble, preserving all rows
#' and fields. Identifiers are character, age is numeric, and entry and exit
#' dates use the package's Europe/Paris datetime convention.
#'
#' @param data Raw movement records returned by EDSAN, or a data frame.
#' @return A tibble of movement records. Explicit times are also exposed in
#'   `HEURE_DATENT` and `HEURE_DATSORT`.
#' @export
mouvement_normalize <- function(data) {
  if (length(data) == 0L) {
    data <- tibble::tibble(
      PATID = character(), EVTID = character(), ELTID = character(),
      PATAGE = numeric(), PATSEX = character(), DATENT = character(),
      DATSORT = character(), SEJUF = character(), SEJUM = character()
    )
  }
  if (is.data.frame(data)) {
    out <- tibble::as_tibble(data)
  } else {
    identifiers <- .edsan_identifier_names("mouvement", "movements")
    out <- dplyr::bind_rows(lapply(data, function(record) {
      record <- .edsan_normalize_identifier_fields(record, identifiers)
      tibble::as_tibble(lapply(record, function(value) {
        .edsan_flatten_scalar_column(list(value))
      }))
    }))
  }
  list_columns <- vapply(out, is.list, logical(1))
  out[list_columns] <- lapply(out[list_columns], .edsan_flatten_scalar_column)
  out <- .edsan_normalize_identifier_columns(out, "mouvement", "movements")
  for (column in intersect(c("DATENT", "DATSORT"), names(out))) {
    raw <- out[[column]]
    out[[column]] <- .pmsi_parse_datetime(raw)
    out[[paste0("HEURE_", column)]] <- .pmsi_time_hms(out[[column]], raw)
  }
  if ("PATAGE" %in% names(out)) {
    out$PATAGE <- suppressWarnings(as.numeric(as.character(out$PATAGE)))
  }
  out
}
