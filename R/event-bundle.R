# Event bundle retrieval and construction -------------------------------------

.validate_event_ids <- function(event_ids, argument = "event_ids") {
  if (!(is.character(event_ids) || is.numeric(event_ids)) || length(event_ids) == 0L) {
    stop("`", argument, "` must contain one or more EVTID values.", call. = FALSE)
  }
  event_ids <- .edsan_as_identifier(event_ids)
  if (anyNA(event_ids) || any(!nzchar(trimws(event_ids)))) {
    stop("`", argument, "` must contain one or more non-missing EVTID values.",
         call. = FALSE)
  }
  if (anyDuplicated(event_ids)) {
    stop("`", argument, "` must not contain duplicate EVTID values.", call. = FALSE)
  }
  event_ids
}

.resolve_event_bundle_modules <- function(sources) {
  available <- .edsan_supported_modules()
  if (identical(sources, "all")) return(available)

  if (!is.character(sources) || length(sources) == 0L || anyNA(sources) ||
      any(!nzchar(sources))) {
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
  sources
}

.validate_event_bundle_sources <- function(sources) {
  if (!is.list(sources) || length(sources) == 0L || is.null(names(sources)) ||
      anyNA(names(sources)) || any(!nzchar(names(sources)))) {
    stop("`sources` must be a non-empty named list of normalized EDSAN sources.",
         call. = FALSE)
  }
  if (anyDuplicated(names(sources))) {
    stop("`sources` must not contain duplicate source names.", call. = FALSE)
  }

  unknown <- setdiff(names(sources), .edsan_supported_modules())
  if (length(unknown) > 0L) {
    stop("Unknown EDSAN source(s): ", paste(unknown, collapse = ", "), ".",
         call. = FALSE)
  }
  sources
}

.slice_event_table <- function(x, event_id, label) {
  if (!is.data.frame(x)) {
    stop("Source table `", label, "` must be a data.frame or tibble.", call. = FALSE)
  }
  if (!"EVTID" %in% names(x)) {
    if (nrow(x) == 0L) return(x)
    stop("Non-empty source table `", label, "` must contain an EVTID column.",
         call. = FALSE)
  }
  x[.edsan_as_identifier(x$EVTID) == event_id, , drop = FALSE]
}

.slice_event_source <- function(source, event_id, source_name) {
  if (identical(source_name, "pmsi")) {
    required <- c("main", "actes", "diag")
    if (!is.list(source) || !all(required %in% names(source))) {
      stop("PMSI source must be a list containing main, actes, and diag tables.",
           call. = FALSE)
    }
    return(lapply(required, function(table) {
      .slice_event_table(source[[table]], event_id, paste0("pmsi$", table))
    }) %>% stats::setNames(required))
  }

  .slice_event_table(source, event_id, source_name)
}

.new_event_bundle <- function(event_id, sources, created_at) {
  structure(
    list(
      event_id = event_id,
      sources = sources,
      created_at = created_at
    ),
    class = c("edsan_event_bundle", "list")
  )
}

#' Build event bundles from normalized EDSAN sources
#'
#' Partitions already available normalized source objects by `EVTID`. No EDSAN
#' retrieval is performed and no rows or columns are selected beyond the event
#' boundary. PMSI retains its `main`, `actes`, and `diag` tables.
#'
#' @param event_ids Non-empty vector of unique EDSAN `EVTID` values.
#' @param sources Named list of normalized EDSAN source objects, for example
#'   `list(doceds = documents, pmsi = pmsi, biol = biology)`.
#'
#' @return `build_event_bundles()` returns a named list, in requested event order,
#'   containing one `edsan_event_bundle` per `EVTID`. `build_event_bundle()`
#'   returns the single bundle directly.
#'
#' @export
build_event_bundles <- function(event_ids, sources) {
  event_ids <- .validate_event_ids(event_ids)
  sources <- .validate_event_bundle_sources(sources)
  created_at <- Sys.time()

  bundles <- lapply(event_ids, function(event_id) {
    event_sources <- lapply(names(sources), function(source_name) {
      .slice_event_source(sources[[source_name]], event_id, source_name)
    })
    names(event_sources) <- names(sources)
    .new_event_bundle(event_id, event_sources, created_at)
  })
  names(bundles) <- event_ids
  bundles
}

#' @rdname build_event_bundles
#' @param event_id One non-missing EDSAN `EVTID`.
#' @export
build_event_bundle <- function(event_id, sources) {
  event_id <- .validate_event_ids(event_id, "event_id")
  if (length(event_id) != 1L) {
    stop("`event_id` must contain exactly one EVTID.", call. = FALSE)
  }
  build_event_bundles(event_id, sources)[[1L]]
}

#' Retrieve normalized EDSAN sources for several events
#'
#' Retrieves each selected EDSAN module once for the complete set of requested
#' `EVTID` values, then delegates local partitioning to [build_event_bundles()].
#' `get_edsan()` remains responsible for any technical ID batching.
#'
#' @param event_ids Non-empty vector of unique EDSAN `EVTID` values.
#' @param sources EDSAN modules to retrieve. Use `"all"` (the default) for every
#'   module registered by [edsan_sources()], or provide a character vector.
#'
#' @return A named list of `edsan_event_bundle` objects in requested event order.
#'
#' @details Retrieval is fail-fast. If any requested source cannot be retrieved,
#' no partial bundle collection is returned. Empty source results are retained.
#'
#' @export
get_event_bundles <- function(event_ids, sources = "all") {
  event_ids <- .validate_event_ids(event_ids)
  modules <- .resolve_event_bundle_modules(sources)

  retrieved <- stats::setNames(vector("list", length(modules)), modules)
  for (module in modules) {
    retrieved[[module]] <- get_edsan(
      module = module,
      what = "data",
      query = list(EVTID = event_ids),
      process = TRUE
    )
  }

  build_event_bundles(event_ids, retrieved)
}

#' Retrieve normalized EDSAN sources for one event
#'
#' Singular convenience wrapper around [get_event_bundles()].
#'
#' @param event_id One non-missing EDSAN `EVTID`.
#' @param sources EDSAN modules to retrieve. Use `"all"` (the default) for every
#'   registered module, or provide a character vector.
#'
#' @return One object of class `edsan_event_bundle`.
#'
#' @export
get_event_bundle <- function(event_id, sources = "all") {
  event_id <- .validate_event_ids(event_id, "event_id")
  if (length(event_id) != 1L) {
    stop("`event_id` must contain exactly one EVTID.", call. = FALSE)
  }
  get_event_bundles(event_id, sources)[[1L]]
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
