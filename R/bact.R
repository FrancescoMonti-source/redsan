#' Normalize EDSAN bacteriology records
#'
#' Convert a flat list of bacteriology records or a data frame into a tibble.
#' Source identifiers are character, age is numeric, and DATPRELEV is parsed
#' using the Europe/Paris datetime convention. HEURE_PRELEV is retained as
#' supplied by the source. CMI and all other source fields are preserved.
#'
#' @param data Raw bacteriology records returned by EDSAN, or a data frame.
#' @return A tibble retaining all records and fields.
#' @export
bact_normalize <- function(data) {
  if (length(data) == 0L) {
    data <- tibble::tibble(
      PATID = character(), EVTID = character(), ELTID = character(),
      PATAGE = numeric(), PATSEX = character(), DATPRELEV = character(),
      HEURE_PRELEV = character(), SEJUM = character(), SEJUF = character(),
      NATUREPVT = character(), IDENTIFICATION = character(), LBLANA = character(),
      LBLRES = character(), CMI = character(), STRRES = character(),
      NUMRES = numeric(), ST = character(), CASFM = character()
    )
  }
  if (is.data.frame(data)) {
    out <- tibble::as_tibble(data)
  } else {
    identifiers <- .edsan_identifier_names("bact", "results")
    out <- dplyr::bind_rows(lapply(data, function(record) {
      if (!is.list(record) || any(vapply(record, is.data.frame, logical(1)))) {
        stop("Unsupported bact record shape; retrieve with process = FALSE to inspect the response.",
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
  out <- .edsan_normalize_identifier_columns(out, "bact", "results")
  if ("DATPRELEV" %in% names(out)) {
    out$DATPRELEV <- .pmsi_parse_datetime(out$DATPRELEV)
  }
  if ("PATAGE" %in% names(out)) {
    out$PATAGE <- suppressWarnings(as.numeric(as.character(out$PATAGE)))
  }
  dplyr::select(out, dplyr::any_of(c(
    "PATID", "EVTID", "ELTID", "PATAGE", "PATSEX", "DATPRELEV", "HEURE_PRELEV",
    "SEJUM", "SEJUF", "NATUREPVT", "IDENTIFICATION", "LBLANA", "LBLRES", "CMI",
    "STRRES", "NUMRES", "ST", "CASFM"
  )), dplyr::everything())
}
