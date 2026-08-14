# EDSAN source registry ------------------------------------------------------

.edsan_source_registry <- function() {
  tibble::tibble(
    module = c("doceds", "pmsi", "pmsi", "pmsi", "biol", "viro"),
    table = c("documents", "main", "actes", "diag", "results", "results"),
    grain = c("document", "stay", "acte", "diagnosis", "biology_result", "virology_result"),
    identifiers = list(
      c("PATID", "EVTID", "ELTID"),
      c("PATID", "EVTID", "ELTID"),
      c("PATID", "EVTID", "ELTID"),
      c("PATID", "EVTID", "ELTID"),
      c("PATID", "EVTID", "ELTID"),
      c("PATID", "EVTID", "ELTID")
    ),
    source_time_kind = c("point", "interval", "point", "interval", "point", "point"),
    source_time_start = c("RECDATE", "DATENT", "DATEACTE", "DATENT", "DATEXAM", "DATEPRELEV"),
    source_time_end = c(NA_character_, "DATSORT", NA_character_, "DATSORT", NA_character_, NA_character_),
    query_date_keys = list(
      "RECDATE",
      c("DATENT", "DATSORT"),
      c("DATENT", "DATSORT"),
      c("DATENT", "DATSORT"),
      "DATEXAM",
      "DATEPRELEV"
    ),
    default_batch_key = c("RECDATE", "DATENT", "DATENT", "DATENT", "DATEXAM", "DATEPRELEV"),
    normalizer = c("process_doceds", "process_pmsi", "process_pmsi", "process_pmsi", "process_biol", "process_viro"),
    notes = c(
      "Clinical documents; RECTXT and RECTYPE are document payload fields.",
      paste(
        "PMSI main table; DATENT/DATSORT define source intervals.",
        "Default processing applies C over DW within each patient-event unit;",
        "use source_policy = 'all' to retain every normalized main row."
      ),
      "Complete PMSI procedure table; DATEACTE is a point time when present.",
      paste(
        "Complete PMSI diagnosis table parsed from DALL; diagnoses inherit",
        "PATID + EVTID limits from the complete main table."
      ),
      "Biology results; analyte/value/unit columns depend on the returned payload.",
      "Virology results; BIOL-like payload with ELTID traceability and DATEPRELEV point time."
    )
  )
}

.edsan_identifier_names <- function(module, table = NULL) {
  sources <- .edsan_source_registry()
  keep <- sources$module %in% module
  if (!is.null(table)) {
    keep <- keep & sources$table %in% table
  }
  unique(unlist(sources$identifiers[keep], use.names = FALSE))
}

.edsan_as_identifier <- function(x) {
  if (is.character(x)) {
    return(x)
  }
  if (is.factor(x) || inherits(x, "integer64")) {
    return(as.character(x))
  }
  if (is.numeric(x) && !inherits(x, c("Date", "POSIXt"))) {
    return(vapply(
      x,
      function(value) {
        if (is.na(value)) {
          return(NA_character_)
        }
        format(
          value,
          scientific = FALSE,
          trim = TRUE,
          drop0trailing = TRUE,
          digits = 15L
        )
      },
      character(1)
    ))
  }
  as.character(x)
}

# BIOL_ID and VIRO_ID were historical names for the same source coordinate as
# ELTID. New normalizers expose only ELTID; accepting the aliases here lets the
# bundle boundary upgrade already-normalized artifacts in one place.
.edsan_canonicalize_eltid <- function(data, module) {
  legacy_id <- switch(
    module,
    biol = "BIOL_ID",
    viro = "VIRO_ID",
    NULL
  )
  if (is.null(legacy_id) || !is.data.frame(data)) {
    return(data)
  }
  if (!legacy_id %in% names(data)) {
    if (nrow(data) == 0L && !"ELTID" %in% names(data)) {
      data$ELTID <- character()
    }
    return(data)
  }

  legacy_values <- .edsan_as_identifier(data[[legacy_id]])
  if ("ELTID" %in% names(data)) {
    eltid_values <- .edsan_as_identifier(data$ELTID)
    if (!identical(unname(eltid_values), unname(legacy_values))) {
      stop(
        "`", legacy_id, "` and `ELTID` must contain the same values.",
        call. = FALSE
      )
    }
    data$ELTID <- eltid_values
    data[[legacy_id]] <- NULL
    return(data)
  }

  names(data)[names(data) == legacy_id] <- "ELTID"
  data$ELTID <- legacy_values
  data
}

.edsan_normalize_identifier_columns <- function(data, module, table = NULL) {
  identifiers <- intersect(
    .edsan_identifier_names(module, table),
    names(data)
  )
  for (identifier in identifiers) {
    data[[identifier]] <- .edsan_as_identifier(data[[identifier]])
  }
  data
}

.edsan_normalize_identifier_fields <- function(data, identifiers) {
  for (identifier in intersect(identifiers, names(data))) {
    data[[identifier]] <- .edsan_as_identifier(data[[identifier]])
  }
  data
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
#' @details
#' Across EDSAN modules, each `ELTID` belongs to exactly one `EVTID`, and each
#' `EVTID` belongs to exactly one `PATID`. These relationships describe document
#' provenance; additional source-row coordinates may still be required for row
#' uniqueness within normalized tables. Public module normalizers return every
#' identifier declared by this registry as character. Character input is
#' preserved exactly; numeric input is rendered without scientific notation.
#'
#' @param module Optional module filter. Supported values are `"doceds"`,
#'   `"pmsi"`, `"biol"`, and `"viro"`.
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
