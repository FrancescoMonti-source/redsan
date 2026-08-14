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

.validate_single_event_id <- function(event_id) {
  event_id <- .validate_event_ids(event_id, "event_id")
  if (length(event_id) != 1L) {
    stop("`event_id` must contain exactly one EVTID.", call. = FALSE)
  }
  event_id
}

.resolve_event_bundle_modules <- function(modules) {
  available <- .edsan_supported_modules()
  if (identical(modules, "all")) return(available)

  if (!is.character(modules) || length(modules) == 0L || anyNA(modules) ||
      any(!nzchar(modules))) {
    stop("`modules` must be \"all\" or a non-empty character vector.", call. = FALSE)
  }

  modules <- unique(modules)
  unknown <- setdiff(modules, available)
  if (length(unknown) > 0L) {
    stop(
      "Unknown EDSAN module(s): ", paste(unknown, collapse = ", "),
      ". Available modules: ", paste(available, collapse = ", "), ".",
      call. = FALSE
    )
  }
  modules
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

# Bundles must expose the same normalized columns whichever entry point produced
# them. New retrieval already uses ELTID and applies module labellers; artifacts
# created before that contract may still carry BIOL_ID or VIRO_ID or lack a
# reference label. Upgrade both differences once at this boundary.
.EVENT_BUNDLE_LABELLERS <- list(
  biol = list(key = "TYPEANA", label = "TYPEANA_LABEL"),
  doceds = list(key = "RECTYPE", label = "RECTYPE_LABEL")
)

.label_event_bundle_source <- function(source, source_name) {
  spec <- .EVENT_BUNDLE_LABELLERS[[source_name]]
  if (is.null(spec) || !is.data.frame(source)) return(source)
  if (spec$label %in% names(source)) return(source)
  if (!spec$key %in% names(source) && nrow(source) > 0L) return(source)
  switch(source_name, biol = label_biol(source), doceds = label_doceds(source))
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
#' retrieval is performed. Legacy `BIOL_ID` and `VIRO_ID` columns are renamed to
#' canonical `ELTID`; otherwise no rows or columns are selected beyond the event
#' boundary. PMSI retains its `main`, `actes`, and `diag` tables.
#'
#' @param event_ids Non-empty vector of unique EDSAN `EVTID` values.
#' @param sources Named list of normalized EDSAN source objects, for example
#'   `list(doceds = documents, pmsi = pmsi, biol = biology)`.
#'
#' @return `build_event_bundles()` returns a named list, in requested event order,
#'   containing one `edsan_event_bundle` per `EVTID`. `build_event_bundle()`
#'   returns the single bundle directly. Each bundle is a list with:
#'   * `event_id`: the requested `EVTID`, stored as character;
#'   * `sources`: the named normalized source objects restricted to that event;
#'   * `created_at`: the common bundle creation time.
#'
#' @details
#' Every table being partitioned must contain `EVTID`, including each of the
#' `main`, `actes`, and `diag` PMSI tables. Empty tables are retained. All rows
#' and columns belonging to the requested event are preserved.
#'
#' A `biol` source carrying `TYPEANA` without `TYPEANA_LABEL` is labelled with
#' [label_biol()] before partitioning, so bundles built from biology artifacts
#' normalized before labelling existed carry the same columns as bundles built
#' from [get_event_bundles()]. Existing `TYPEANA_LABEL` values are left as they
#' are, and a `biol` source without `TYPEANA` is passed through untouched.
#' Biology and virology artifacts using the former `BIOL_ID` or `VIRO_ID` names
#' are upgraded to `ELTID`. When an artifact contains both names, their values
#' must agree and only `ELTID` is retained.
#'
#' @examples
#' sources <- list(
#'   doceds = tibble::tibble(
#'     EVTID = c("E1", "E2"),
#'     ELTID = c("D1", "D2"),
#'     RECTXT = c("First document", "Second document")
#'   ),
#'   biol = tibble::tibble(
#'     EVTID = c("E1", "E2"),
#'     ELTID = c("B1", "B2"),
#'     NUMRES = c(12.3, 9.8)
#'   )
#' )
#'
#' bundles <- build_event_bundles(c("E2", "E1"), sources)
#' names(bundles)
#' bundles[["E1"]]
#'
#' one_bundle <- build_event_bundle("E1", sources)
#'
#' @export
build_event_bundles <- function(event_ids, sources) {
  event_ids <- .validate_event_ids(event_ids)
  sources <- .validate_event_bundle_sources(sources)
  sources[] <- lapply(names(sources), function(source_name) {
    source <- .edsan_canonicalize_eltid(sources[[source_name]], source_name)
    .label_event_bundle_source(source, source_name)
  })
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
  build_event_bundles(.validate_single_event_id(event_id), sources)[[1L]]
}

#' Retrieve normalized EDSAN sources for several events
#'
#' Retrieves each selected EDSAN module once for the complete set of requested
#' `EVTID` values, then delegates local partitioning to [build_event_bundles()].
#' `get_edsan()` remains responsible for any technical ID batching.
#'
#' @param event_ids Non-empty vector of unique EDSAN `EVTID` values.
#' @param modules EDSAN modules to retrieve. Use `"all"` (the default) for every
#'   module registered by [edsan_sources()], or provide a character vector.
#'
#' @return A named list of `edsan_event_bundle` objects in requested event order.
#'
#' @details Retrieval is fail-fast. If any requested module cannot be retrieved,
#' no partial bundle collection is returned. Each selected module is retrieved
#' once for the complete `event_ids` vector and then partitioned locally. Empty
#' normalized module results are retained in every affected bundle.
#'
#' Modules are retrieved with `process = TRUE`, so reference labels added by the
#' module processors are present in every bundle: `biol` carries `TYPEANA_LABEL`
#' from [label_biol()] and PMSI carries the labels added by [label_pmsi()].
#'
#' @examples
#' \dontrun{
#' bundles <- get_event_bundles(
#'   c("123456789", "987654321"),
#'   modules = c("doceds", "pmsi", "biol")
#' )
#' bundles[["123456789"]]
#' }
#'
#' @export
get_event_bundles <- function(event_ids, modules = "all") {
  event_ids <- .validate_event_ids(event_ids)
  modules <- .resolve_event_bundle_modules(modules)

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
#' Singular convenience wrapper around [get_event_bundles()]. It retrieves each
#' selected module for one `EVTID` and returns the bundle directly rather than
#' inside a named collection.
#'
#' @param event_id One non-missing EDSAN `EVTID`.
#' @param modules EDSAN modules to retrieve. Use `"all"` (the default) for every
#'   registered module, or provide a character vector.
#'
#' @return One `edsan_event_bundle`: a list with `event_id`, `sources`, and
#'   `created_at` entries. PMSI remains a list with `main`, `actes`, and `diag`;
#'   DOCEDS, BIOL, and VIRO remain normalized tibbles.
#'
#' @details
#' This function only forwards to [get_event_bundles()] and unwraps the single
#' bundle, so retrieval, normalization, and reference labelling behave exactly as
#' in the plural form: `biol` carries `TYPEANA_LABEL` from [label_biol()] and
#' PMSI carries the labels added by [label_pmsi()].
#'
#' Retrieval is fail-fast. If any selected module cannot be retrieved, no
#' partial bundle is returned. Empty normalized module tables are retained.
#'
#' @examples
#' \dontrun{
#' bundle <- get_event_bundle("123456789")
#' bundle <- get_event_bundle(
#'   "123456789",
#'   modules = c("doceds", "pmsi", "biol")
#' )
#' }
#'
#' @export
get_event_bundle <- function(event_id, modules = "all") {
  get_event_bundles(.validate_single_event_id(event_id), modules)[[1L]]
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

#' Print an EDSAN event bundle
#'
#' Displays the event identifier and row counts for each normalized source.
#'
#' @param x An `edsan_event_bundle`.
#' @param ... Additional arguments, currently unused.
#'
#' @return `x`, invisibly.
#'
#' @method print edsan_event_bundle
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
