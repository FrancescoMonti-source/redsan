# Event bundle retrieval ------------------------------------------------------

#' Retrieve normalized EDSAN sources for one event
#'
#' Builds a source-complete representation of one EDSAN event by querying each
#' selected module with the same `EVTID`. The bundle preserves each module's
#' normalized return shape: `pmsi` remains a list of `main`, `actes`, and `diag`
#' tables, while `doceds`, `biol`, and `viro` remain normalized tibbles.
#'
#' @param event_id One non-missing EDSAN `EVTID`.
#' @param sources EDSAN modules to retrieve. Use `"all"` (the default) for every
#'   module registered by [edsan_sources()], or provide a character vector such
#'   as `c("doceds", "biol")`.
#'
#' @return An object of class `edsan_event_bundle`, implemented as a named list
#'   with `event_id`, `sources`, and `created_at` entries.
#'
#' @details
#' Retrieval is fail-fast. If any requested source cannot be retrieved, no
#' partial bundle is returned. Empty source results are retained as empty
#' normalized tables.
#'
#' @examples
#' \dontrun{
#' bundle <- get_event_bundle("123456789")
#' bundle <- get_event_bundle(
#'   "123456789",
#'   sources = c("doceds", "pmsi", "biol")
#' )
#' }
#'
#' @export
get_event_bundle <- function(event_id, sources = "all") {
  if (length(event_id) != 1L || is.na(event_id) || !nzchar(trimws(as.character(event_id)))) {
    stop("`event_id` must be one non-missing EVTID.", call. = FALSE)
  }
  event_id <- as.character(event_id)

  available <- .edsan_supported_modules()
  if (identical(sources, "all")) {
    sources <- available
  } else {
    if (!is.character(sources) || length(sources) == 0L || anyNA(sources)) {
      stop("`sources` must be \"all\" or a non-empty character vector.", call. = FALSE)
    }
    sources <- unique(sources)
    unknown <- setdiff(sources, available)
    if (length(unknown) > 0L) {
      stop(
        "Unknown EDSAN source(s): ", paste(unknown, collapse = ", "),
        ". Available sources: ", paste(available, collapse = ", "), ".",
        call. = FALSE
      )
    }
  }

  data <- stats::setNames(vector("list", length(sources)), sources)
  for (source in sources) {
    data[[source]] <- get_edsan(
      module = source,
      what = "data",
      query = list(EVTID = event_id),
      process = TRUE
    )
  }

  structure(
    list(
      event_id = event_id,
      sources = data,
      created_at = Sys.time()
    ),
    class = c("edsan_event_bundle", "list")
  )
}

.event_bundle_count <- function(x) {
  if (is.data.frame(x)) return(nrow(x))
  if (is.list(x) && !is.data.frame(x)) {
    counts <- vapply(x, function(item) {
      if (is.data.frame(item)) nrow(item) else NA_integer_
    }, integer(1))
    return(counts)
  }
  NA_integer_
}

#' @export
print.edsan_event_bundle <- function(x, ...) {
  cat("EDSaN event bundle: ", x$event_id, "\n", sep = "")
  cat("Sources:\n")

  for (source in names(x$sources)) {
    counts <- .event_bundle_count(x$sources[[source]])
    if (length(counts) == 1L) {
      label <- if (is.na(counts)) "unknown shape" else paste0(counts, " rows")
    } else {
      known <- !is.na(counts)
      label <- if (any(known)) {
        paste(paste0(names(counts)[known], "=", counts[known]), collapse = ", ")
      } else {
        "unknown shape"
      }
    }
    cat("  ", source, ": ", label, "\n", sep = "")
  }

  invisible(x)
}
