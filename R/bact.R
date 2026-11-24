#' Normalize EDSAN bacteriology records
#'
#' Convert a flat list of bacteriology records or a data frame into a tibble.
#' Source identifiers are character, age is numeric, and DATEPRELEV or
#' DATPRELEV is parsed using the Europe/Paris datetime convention. HEUREPRELEV
#' and HEURE_PRELEV are retained as supplied by the source. Leading and trailing whitespace is removed from
#' NATUREPVT, IDENTIFICATION, LBLANA, LBLRES, and STRRES. Other fields are
#' preserved.
#'
#' @param data Raw bacteriology records returned by EDSAN, or a data frame.
#' @return A tibble retaining all records and fields.
#' @export
bact_normalize <- function(data) {
  .bact_normalize(data, module = "bact", date_col = "DATPRELEV", time_col = "HEURE_PRELEV")
}

#' Normalize experimental EDSAN bactibs records
#'
#' Applies the same normalization as [bact_normalize()] to the experimental
#' bactibs module, parsing `DATEPRELEV` and preserving `HEUREPRELEV`.
#'
#' @param data Raw bactibs records returned by EDSAN, or a data frame.
#' @return A tibble retaining all records and fields.
#' @export
bactibs_normalize <- function(data) {
  .bact_normalize(data, module = "bactibs", date_col = "DATEPRELEV", time_col = "HEUREPRELEV")
}

.bact_normalize <- function(data, module, date_col, time_col) {
  if (length(data) == 0L) {
    data <- tibble::tibble(
      PATID = character(), EVTID = character(), ELTID = character(),
      PATAGE = numeric(), PATSEX = character(), DATPRELEV = character(),
      HEURE_PRELEV = character(), SEJUM = character(), SEJUF = character(),
      NATUREPVT = character(), IDENTIFICATION = character(), LBLANA = character(),
      LBLRES = character(), CMI = character(), STRRES = character(),
      NUMRES = numeric(), ST = character(), CASFM = character()
    )
    names(data)[names(data) == "DATPRELEV"] <- date_col
    names(data)[names(data) == "HEURE_PRELEV"] <- time_col
  }
  if (is.data.frame(data)) {
    out <- tibble::as_tibble(data)
  } else {
    identifiers <- .edsan_identifier_names(module, "results")
    out <- dplyr::bind_rows(lapply(data, function(record) {
      if (!is.list(record) || any(vapply(record, is.data.frame, logical(1)))) {
        stop("Unsupported ", module, " record shape; retrieve with process = FALSE to inspect the response.",
             call. = FALSE)
      }
      record <- .edsan_normalize_identifier_fields(record, identifiers)
      tibble::as_tibble(lapply(record, function(value) {
        .edsan_flatten_scalar_column(list(value))
      }))
    }))
  }
  list_columns <- vapply(out, is.list, logical(1))
  out[list_columns] <- lapply(out[list_columns], .edsan_flatten_scalar_column)
  out <- .edsan_normalize_identifier_columns(out, module, "results")
  for (column in intersect(c("NATUREPVT", "IDENTIFICATION", "LBLANA", "LBLRES", "STRRES"), names(out))) {
    out[[column]] <- stringr::str_trim(as.character(out[[column]]))
  }
  date_cols <- if (identical(module, "bact")) c("DATEPRELEV", "DATPRELEV") else date_col
  for (column in intersect(date_cols, names(out))) {
    out[[column]] <- .pmsi_parse_datetime(out[[column]])
  }
  if ("PATAGE" %in% names(out)) {
    out$PATAGE <- suppressWarnings(as.numeric(as.character(out$PATAGE)))
  }
  dplyr::select(out, dplyr::any_of(c(
    "PATID", "EVTID", "ELTID", "PATAGE", "PATSEX", date_cols, time_col,
    if (identical(module, "bact")) "HEUREPRELEV",
    "SEJUM", "SEJUF", "NATUREPVT", "IDENTIFICATION", "LBLANA", "LBLRES", "CMI",
    "STRRES", "NUMRES", "ST", "CASFM"
  )), dplyr::everything())
}
