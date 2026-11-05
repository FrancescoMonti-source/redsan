#' Normalize EDSAN pancarte measurements
#'
#' Converts the raw list of measurement records into one tibble. Record names
#' are transport keys: source identifiers are read from the record fields, and
#' several rows may carry the same `ELTID`.
#'
#' @param data Raw PANCARTE records returned by EDSAN, or a data frame of
#'   measurement rows.
#'
#' @return A tibble retaining every measurement. `RES` becomes `STRRES`,
#'   `NUMRES` and `PATAGE` are numeric, and identifiers are character.
#'   `DATEMES`, `DATENT`, and `DATSORT` are parsed as `POSIXct`, with `HEURE_*`
#'   columns recording explicitly supplied times. `JOURMES` is omitted because
#'   it repeats the date of `DATEMES`. `TYPE_LABEL` comes from [label_pancarte()].
#'
#' @details
#' Datetimes use the existing `Europe/Paris` parsing convention. A date-only
#' input has no explicit time in its `HEURE_*` column.
#' `PATSEX`, `SEJUM`, `SEJUF`, and `CMT` retain their native names. Several unit
#' codes in one field are joined with `;`, keeping one row per measurement.
#' Textual and numeric results are retained independently; no unit or corrected
#' clinical value is inferred from the measurement type.
#'
#' @examples
#' raw <- list(
#'   measurement_0 = list(
#'     ELTID = "L1", EVTID = "E1", PATID = "P1",
#'     DATEMES = "2024-01-01T08:30:00", JOURMES = "2024-01-01",
#'     TYPE = "POIDS-KG", RES = "65.2", NUMRES = 65.2
#'   )
#' )
#' pancarte_normalize(raw)
#'
#' @export
pancarte_normalize <- function(data) {
  if (length(data) == 0L) {
    data <- tibble::tibble(
      ELTID = character(), EVTID = character(), PATID = character(),
      PATAGE = numeric(), PATSEX = character(),
      DATENT = character(), DATSORT = character(), DATEMES = character(),
      SEJUF = character(), TYPE = character(), RES = character(),
      CMT = character(), SEJUM = character(), NUMRES = numeric()
    )
  }

  if (is.data.frame(data)) {
    out <- tibble::as_tibble(data)
  } else {
    identifiers <- .edsan_identifier_names("pancarte", "results")
    out <- dplyr::bind_rows(lapply(data, function(record) {
      record <- .edsan_normalize_identifier_fields(record, identifiers)
      # Wrap each field so NULL is one missing cell and several unit codes
      # remain one cell. Flattening the record itself would change its columns.
      tibble::as_tibble(lapply(record, function(value) {
        .edsan_flatten_scalar_column(list(value))
      }))
    }))
  }

  list_columns <- vapply(out, is.list, logical(1))
  out[list_columns] <- lapply(out[list_columns], .edsan_flatten_scalar_column)
  out <- .edsan_normalize_identifier_columns(out, "pancarte", "results")

  if ("RES" %in% names(out)) {
    out <- dplyr::rename(out, STRRES = "RES")
  }
  out <- dplyr::select(out, -dplyr::any_of("JOURMES"))

  for (column in intersect(c("DATEMES", "DATENT", "DATSORT"), names(out))) {
    raw <- out[[column]]
    out[[column]] <- .pmsi_parse_datetime(raw)
    out[[paste0("HEURE_", column)]] <- .pmsi_time_hms(out[[column]], raw)
  }
  for (column in intersect(c("PATAGE", "NUMRES"), names(out))) {
    out[[column]] <- suppressWarnings(as.numeric(as.character(out[[column]])))
  }

  label_pancarte(out)
}

#' Add reference labels to pancarte measurement types
#'
#' Matches `TYPE` exactly against the packaged pancarte reference and adds
#' `TYPE_LABEL`. Every input row is retained, including unknown codes, whose
#' label is `NA`. An existing label column is refreshed from the reference.
#'
#' @param pancarte A normalized measurement data frame containing `TYPE`.
#' @return The input with character `TYPE` and a `TYPE_LABEL` column.
#' @examples
#' label_pancarte(data.frame(TYPE = c("POIDS-KG", "UNKNOWN")))
#' @export
label_pancarte <- function(pancarte) {
  if (!is.data.frame(pancarte)) {
    stop("`pancarte` must be a data frame.", call. = FALSE)
  }
  if (!"TYPE" %in% names(pancarte)) {
    if (!nrow(pancarte)) {
      pancarte$TYPE <- character()
    } else {
      stop("`pancarte` is missing required column: TYPE.", call. = FALSE)
    }
  }
  pancarte$TYPE <- .edsan_flatten_scalar_column(pancarte$TYPE)
  pancarte <- dplyr::select(pancarte, -dplyr::any_of("TYPE_LABEL"))
  dplyr::left_join(pancarte, edsan_reference("pancarte"), by = "TYPE")
}
