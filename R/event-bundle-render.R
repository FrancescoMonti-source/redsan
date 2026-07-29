# Event bundle rendering ------------------------------------------------------

.validate_event_bundle_for_render <- function(bundle) {
  if (!inherits(bundle, "edsan_event_bundle") || !is.list(bundle)) {
    stop("`bundle` must be an edsan_event_bundle.", call. = FALSE)
  }
  if (!is.list(bundle$sources) || is.null(names(bundle$sources)) ||
      anyNA(names(bundle$sources)) || any(!nzchar(names(bundle$sources)))) {
    stop("`bundle$sources` must be a named list of retrieved EDSAN sources.",
         call. = FALSE)
  }
  invisible(bundle)
}

.event_bundle_sources_to_render <- function(bundle, sources) {
  available <- names(bundle$sources)

  if (is.null(sources) || identical(sources, "all")) {
    return(available)
  }
  if (!is.character(sources) || length(sources) == 0L || anyNA(sources)) {
    stop(
      "`sources` must be NULL, \"all\", or a non-empty character vector.",
      call. = FALSE
    )
  }

  sources <- unique(sources)
  missing <- setdiff(sources, available)
  if (length(missing) > 0L) {
    available_label <- if (length(available) == 0L) {
      "none"
    } else {
      paste(available, collapse = ", ")
    }
    stop(
      "Source(s) not present in this event bundle: ",
      paste(missing, collapse = ", "),
      ". Available sources: ", available_label, ".",
      call. = FALSE
    )
  }

  sources
}

#' Render an EDSAN event bundle
#'
#' Serializes a retrieved event bundle into one JSON character string suitable
#' for inspection, storage, or downstream model input. Rendering is neutral: it
#' preserves every row and column of each selected source and performs no new
#' EDSAN retrieval or clinical selection.
#'
#' @param bundle An `edsan_event_bundle` returned by [get_event_bundle()],
#'   [build_event_bundle()], or extracted from the collection returned by their
#'   plural counterparts.
#' @param sources Sources already present in `bundle` to include. `NULL` (the
#'   default) or `"all"` includes every source in the bundle. A character vector
#'   selects whole sources without filtering their contents.
#' @param pretty Whether to indent the JSON for readability. Set `FALSE` for a
#'   more compact representation.
#'
#' @return One JSON character string containing `event_id`, `created_at`, and
#'   the selected normalized source payloads.
#'
#' @details
#' `render_event_bundle()` never calls EDSAN. Asking for a source that was not
#' retrieved into the bundle is therefore an error rather than an implicit
#' network request. Empty source tables remain explicit empty arrays.
#'
#' @examples
#' \dontrun{
#' bundle <- get_event_bundle("123456789")
#' full_context <- render_event_bundle(bundle)
#' compact_context <- render_event_bundle(bundle, pretty = FALSE)
#' documents_only <- render_event_bundle(bundle, sources = "doceds")
#' }
#'
#' @export
render_event_bundle <- function(bundle, sources = NULL, pretty = TRUE) {
  .validate_event_bundle_for_render(bundle)
  sources <- .event_bundle_sources_to_render(bundle, sources)

  payload <- list(
    event_id = bundle$event_id,
    created_at = bundle$created_at,
    sources = bundle$sources[sources]
  )

  as.character(jsonlite::toJSON(
    payload,
    dataframe = "rows",
    matrix = "rowmajor",
    Date = "ISO8601",
    POSIXt = "ISO8601",
    factor = "string",
    complex = "string",
    raw = "base64",
    null = "null",
    na = "null",
    auto_unbox = TRUE,
    digits = NA,
    pretty = isTRUE(pretty)
  ))
}
