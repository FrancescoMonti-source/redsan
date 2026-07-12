# EDSAN source registry ------------------------------------------------------

.edsan_source_registry <- function() {
  tibble::tibble(
    module = c("doceds", "pmsi", "pmsi", "pmsi", "biol"),
    table = c("documents", "main", "actes", "diag", "results"),
    grain = c("document", "stay", "acte", "diagnosis", "biology_result"),
    identifiers = list(
      c("PATID", "EVTID", "ELTID"),
      c("PATID", "EVTID", "ELTID"),
      c("PATID", "EVTID", "ELTID"),
      c("PATID", "EVTID", "ELTID"),
      c("PATID", "EVTID", "ELTID", "BIOL_ID")
    ),
    source_time_kind = c("point", "interval", "point", "interval", "point"),
    source_time_start = c("RECDATE", "DATENT", "DATEACTE", "DATENT", "DATEXAM"),
    source_time_end = c(NA_character_, "DATSORT", NA_character_, "DATSORT", NA_character_),
    query_date_keys = list(
      "RECDATE",
      c("DATENT", "DATSORT"),
      c("DATENT", "DATSORT"),
      c("DATENT", "DATSORT"),
      "DATEXAM"
    ),
    default_batch_key = c("RECDATE", "DATENT", "DATENT", "DATENT", "DATEXAM"),
    normalizer = c("process_doceds", "process_pmsi", "process_pmsi", "process_pmsi", "process_biol"),
    notes = c(
      "Clinical documents; RECTXT and RECTYPE are document payload fields.",
      "PMSI stay-level table; DATENT/DATSORT define the stay interval.",
      "PMSI procedure table; DATEACTE is a point time when present.",
      "PMSI diagnosis table parsed from DALL; diagnoses inherit the stay interval.",
      "Biology results; analyte/value/unit columns depend on the returned payload."
    )
  )
}

.edsan_supported_modules <- function() {
  unique(.edsan_source_registry()$module)
}

.edsan_all_date_keys <- function() {
  unique(unlist(.edsan_source_registry()$query_date_keys, use.names = FALSE))
}

.edsan_module_date_keys <- function(module) {
  sources <- .edsan_source_registry()
  unique(unlist(sources$query_date_keys[sources$module == module], use.names = FALSE))
}

.edsan_default_batch_key <- function(module) {
  sources <- .edsan_source_registry()
  unique(sources$default_batch_key[sources$module == module])[[1]]
}

.edsan_validate_date_keys <- function(module, present_dates) {
  if (length(present_dates) == 0) return(invisible(TRUE))

  allowed <- .edsan_module_date_keys(module)
  bad <- setdiff(present_dates, allowed)
  if (length(bad) == 0) return(invisible(TRUE))

  stop(
    module,
    " module only supports ",
    paste(allowed, collapse = " and "),
    " as date key",
    if (length(allowed) > 1) "s" else "",
    "; unsupported key(s): ",
    paste(bad, collapse = ", "),
    ". Please use ",
    paste(allowed, collapse = " or "),
    "."
  )
}

#' List EDSAN source contracts known by redsan
#'
#' Returns the package registry of EDSAN modules and normalized tables. The
#' registry is intentionally about source mechanics: identifiers, row grain,
#' query date keys, default batching keys, and point/interval time semantics.
#' Clinical concepts and study-specific measurement rules belong downstream.
#'
#' @param module Optional module filter. Supported values are `"doceds"`,
#'   `"pmsi"`, and `"biol"`.
#' @param table Optional normalized table filter, for example `"main"`,
#'   `"actes"`, `"diag"`, or `"results"`.
#'
#' @return A tibble with one row per known source table.
#'
#' @examples
#' edsan_sources()
#' edsan_sources("pmsi")
#' edsan_sources("pmsi", "diag")
#'
#' @export
edsan_sources <- function(module = NULL, table = NULL) {
  out <- .edsan_source_registry()

  if (!is.null(module)) {
    module <- match.arg(module, .edsan_supported_modules(), several.ok = TRUE)
    out <- out[out$module %in% module, , drop = FALSE]
  }

  if (!is.null(table)) {
    table <- as.character(table)
    bad <- setdiff(table, out$table)
    if (length(bad) > 0) {
      stop(
        "Unknown EDSAN table(s) for selected module(s): ",
        paste(bad, collapse = ", "),
        call. = FALSE
      )
    }
    out <- out[out$table %in% table, , drop = FALSE]
  }

  out
}
