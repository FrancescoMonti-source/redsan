# EDSAN data retrieval helpers
#
# This file contains a thin public wrapper (`get_edsan`) plus a set of focused
# helpers used to batch requests by time window or by ID list. The helper
# functions are intentionally small so the batching logic remains testable and
# composable.

# Time-window helpers
# These functions translate user-provided bounds into concrete batch windows.
.edsan_make_periods <- function(start_date, end_date,
                                sep = ",", by = "6 months",
                                prefix = "", suffix = "") {
  s <- lubridate::as_date(start_date)
  e <- lubridate::as_date(end_date)
  if (is.na(s) || is.na(e)) stop("start_date/end_date must be coercible to Date")
  if (s > e) {
    return(tibble::tibble(
      start = as.Date(character()),
      end   = as.Date(character()),
      period = character()
    ))
  }

  starts <- seq(from = s, to = e, by = by)
  ends <- c(starts[-1] - 1, e)

  tibble::tibble(
    start  = starts,
    end    = ends,
    period = paste0(
      prefix,
      format(starts, "%Y-%m-%d"),
      sep,
      format(ends, "%Y-%m-%d"),
      suffix
    )
  )
}

.edsan_extract_bounds <- function(x) {
  # Parse API-style date bounds into list(lo, hi).
  if (is.null(x) || length(x) == 0 || is.na(x)) return(list(lo = NULL, hi = NULL))
  x <- trimws(as.character(x))

  # range: "{YYYY-MM-DD,YYYY-MM-DD}"
  m <- stringr::str_match(x, "^\\{\\s*(\\d{4}-\\d{2}-\\d{2})\\s*,\\s*(\\d{4}-\\d{2}-\\d{2})\\s*\\}$")
  if (!is.na(m[1, 1])) {
    return(list(lo = as.Date(m[1, 2]), hi = as.Date(m[1, 3])))
  }

  # comparator: >YYYY-MM-DD or <YYYY-MM-DD
  m <- stringr::str_match(x, "^([<>])\\s*(\\d{4}-\\d{2}-\\d{2})$")
  if (!is.na(m[1, 1])) {
    d <- as.Date(m[1, 3])
    if (m[1, 2] == ">") return(list(lo = d, hi = NULL))
    if (m[1, 2] == "<") return(list(lo = NULL, hi = d))
  }

  list(lo = NULL, hi = NULL)
}

.edsan_infer_batch_window <- function(module, query, batch_key, start_date = NULL, end_date = NULL) {
  # Pick a usable time window based on module-specific rules.
  if (!is.null(start_date) && !is.null(end_date)) {
    lo <- as.Date(start_date)
    hi <- as.Date(end_date)
    if (is.na(lo) || is.na(hi) || lo > hi) stop("Invalid start_date/end_date")
    return(list(start = lo, end = hi, inferred = FALSE))
  }

  if (module == "doceds") {
    b <- .edsan_extract_bounds(query[[batch_key]])
    if (is.null(b$lo) || is.null(b$hi) || b$lo > b$hi) {
      stop("missing_time_window: doceds requires ", batch_key, " bounds")
    }
    return(list(start = b$lo, end = b$hi, inferred = TRUE))
  }

  if (module == "pmsi") {
    b_datent  <- .edsan_extract_bounds(query[["DATENT"]])
    b_datsort <- .edsan_extract_bounds(query[["DATSORT"]])

    if (batch_key == "DATENT") {
      lo <- b_datent$lo
      hi <- b_datent$hi %||% b_datsort$hi
      if (is.null(lo) || is.null(hi) || lo > hi) {
        stop("missing_time_window: pmsi DATENT batching requires bounds")
      }
      return(list(start = lo, end = hi, inferred = TRUE))
    }

    if (batch_key == "DATSORT") {
      lo <- b_datsort$lo %||% b_datent$lo
      hi <- b_datsort$hi
      if (is.null(lo) || is.null(hi) || lo > hi) {
        stop("missing_time_window: pmsi DATSORT batching requires bounds")
      }
      return(list(start = lo, end = hi, inferred = TRUE))
    }

    stop("Unsupported batch_key for pmsi")
  }

  if (module == "biol") {
    b <- .edsan_extract_bounds(query[[batch_key]])
    if (is.null(b$lo) || is.null(b$hi) || b$lo > b$hi) {
      stop("missing_time_window: biol requires ", batch_key, " bounds")
    }
    return(list(start = b$lo, end = b$hi, inferred = TRUE))
  }

  stop("infer_batch_window: module not supported")
}

# ID-batching helpers
# These functions decide when and how to split ID lists for safe API calls.

.edsan_choose_batch_ids_key <- function(query, candidates = c("ELTID", "EVTID", "PATID")) {
  # Prefer the most specific identifier available.
  present <- candidates[candidates %in% names(query) & !purrr::map_lgl(query[candidates], is.null)]
  if (length(present) == 0) return(NULL)

  counts <- purrr::map_int(present, function(k) length(.edsan_split_id_string(query[[k]])))
  spec_order <- match(present, candidates) # smaller index = more specific
  present[order(spec_order, -counts)][1]
}

.edsan_id_like <- function(x) {
  # Detect whether x likely encodes a list of IDs (vector or OR-separated string).
  if (is.null(x)) return(FALSE)
  if (length(x) > 1) return(TRUE)
  s <- as.character(x)[1]
  if (is.na(s) || !nzchar(s)) return(FALSE)
  stringr::str_detect(s, "\\s+OR\\s+") || stringr::str_detect(s, "[\\s,;]")
}

.edsan_split_id_string <- function(x) {
  # Accept vectors, or a single string like "1 2 3" or "1 OR 2 OR 3".
  x <- x %||% character()
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]
  if (length(x) == 0) return(character())

  if (length(x) > 1) return(x)

  s <- x[[1]]
  # normalize OR separators to spaces
  s <- stringr::str_replace_all(s, "\\s+OR\\s+", " ")
  # split on whitespace, commas, semicolons
  parts <- unlist(stringr::str_split(s, "[\\s,;]+"))
  parts <- parts[!is.na(parts) & nzchar(parts)]
  parts
}

.edsan_call <- function(module, query, what = c("data", "idtriplets")) {
  # Single request to the EDSAN backend (data or idtriplets).
  what <- match.arg(what)
  qry <- jsonlite::toJSON(query, auto_unbox = TRUE)

  fn <- if (what == "idtriplets") {
    d2imr::d2im_wsc.get_edsan_idtriplets_as_dataframe
  } else {
    d2imr::d2im_wsc.get_edsan_data_as_dataframe
  }

  res <- try(fn(module, qry, "edsan"), silent = TRUE)
  if (inherits(res, "try-error")) return(list(ok = FALSE, value = NULL, error = as.character(res)))
  list(ok = TRUE, value = res, error = NULL)
}

.edsan_combine <- function(module, results, what = c("data", "idtriplets")) {
  # Combine batch outputs into a single result.
  what <- match.arg(what)
  if (what == "idtriplets") {
    return(dplyr::bind_rows(purrr::compact(results)) %>%
             tidyr::unnest(cols = c(eltExt, eltId, evtId, patId)) %>%
             dplyr::select(patId, evtId, eltId) %>%
             dplyr::rename(ELTID = eltId, EVTID = evtId, PATID = patId)
    )
  }
  if (module == "doceds") return(dplyr::bind_rows(purrr::compact(results)))
  purrr::list_flatten(purrr::compact(results))
}

.edsan_normalize_id_query <- function(query, batch_ids_key) {
  # AND semantics: if batching on the most specific ID, drop redundant higher-level IDs.
  if (is.null(batch_ids_key)) return(query)
  if (batch_ids_key == "ELTID") {
    query[c("EVTID", "PATID")] <- NULL
  } else if (batch_ids_key == "EVTID") {
    query["PATID"] <- NULL
  }
  query
}

.edsan_count_out_units <- function(module, value, what = c("data", "idtriplets")) {
  # Estimate output size for adaptive splitting in ids mode.
  what <- match.arg(what)
  if (is.null(value)) return(0L)

  if (what == "idtriplets") {
    if (is.data.frame(value)) return(as.integer(nrow(value)))
    return(0L)
  }

  if (module %in% c("pmsi", "biol")) {
    if (is.list(value) && !is.data.frame(value)) return(as.integer(length(value)))
  }

  if (module == "doceds") {
    if (is.data.frame(value)) {
      if ("DOC_ID" %in% names(value)) return(as.integer(dplyr::n_distinct(value$DOC_ID)))
      return(as.integer(nrow(value)))
    }
  }

  if (is.data.frame(value)) return(as.integer(nrow(value)))
  if (is.list(value)) return(as.integer(length(value)))
  0L
}

#' Retrieve EDSAN data with adaptive batching
#'
#' Wrapper around the EDSAN API that supports automatic time- or ID-based
#' batching to stay within API limits. For time batching, provide explicit
#' bounds in `query` or via `start_date`/`end_date`. For ID batching, provide
#' an ID field containing multiple IDs (vector or OR-separated string).
#'
#' @details
#' The EDSAN backend enforces strict result size limits, so `get_edsan()` uses
#' adaptive batching to reduce failures and retry with smaller chunks.
#'
#' Decision flow (high level):
#' \itemize{
#' \item `mode = "auto"` chooses `ids` batching if the query contains a list of
#' IDs; otherwise it uses time batching.
#' \item `mode = "time"` attempts a single call first, then splits the time
#' window into `periods_by` chunks only after a limit error (when
#' `batch_on_error_only = TRUE`).
#' \item `mode = "ids"` splits the input ID list into chunks (`max_in_ids`) and
#' further subdivides when a chunk still returns too many rows (`max_out_units`).
#' \item Optional `return_audit = TRUE` yields a per-batch table with inputs,
#' outputs, and errors for debugging.
#' }
#'
#' Helper roles:
#' \describe{
#' \item{.edsan_extract_bounds}{Parses API-style date bounds from the query.}
#' \item{.edsan_infer_batch_window}{Determines a time window for batching.}
#' \item{.edsan_split_id_string}{Normalizes ID vectors or OR-separated strings.}
#' \item{.edsan_count_out_units}{Estimates output size to trigger splits.}
#' \item{.edsan_combine}{Merges batch outputs into a single result.}
#' }
#'
#' Glossary:
#' \describe{
#' \item{batch_key}{Query field used for time batching, e.g. `DATENT` or `DATEXAM`.}
#' \item{bounds}{Date range like `{YYYY-MM-DD,YYYY-MM-DD}` or comparators `>YYYY-MM-DD`.}
#' \item{limit error}{Backend error indicating too many results (quota/max rows).}
#' \item{output units}{Heuristic count of returned rows or records used to trigger splitting.}
#' \item{ids mode}{Batching strategy that splits the input ID list into chunks.}
#' \item{time mode}{Batching strategy that splits a time range into periods.}
#' }
#'
#' Pseudo-flow (simplified):
#' \preformatted{
#' get_edsan()
#'   -> decide mode (auto | time | ids)
#'   -> if time:
#'        try single call
#'        on limit error -> split into time periods -> combine
#'   -> if ids:
#'        split IDs into chunks
#'        if chunk too large -> split further
#'        optional time fallback on limit error
#'   -> return combined result (and audit if requested)
#' }
#'
#' @param module One of `doceds`, `pmsi`, or `biol`.
#' @param what One of `data` or `idtriplets`.
#' @param query Named list of API query parameters.
#' @param start_date,end_date Optional Date bounds for time batching.
#' @param periods_by Size of each time chunk (passed to `seq`, e.g. "1 month").
#' @param periods_prefix,periods_suffix Strings wrapped around each time window.
#' @param batch_key Field used for time batching (defaults by module).
#' @param mode `auto`, `time`, or `ids` batching strategy.
#' @param batch_ids_key Field used for ID batching.
#' @param max_in_ids,min_in_ids Limits for input ID chunk sizes.
#' @param max_out_units Maximum output units before further splitting.
#' @param output_count_fn Function to count output units (defaults by module).
#' @param return_audit If `TRUE`, returns a list with `data` and `audit`.
#' @param batch_on_error_only If `TRUE`, time batching starts only after a limit error.
#' @param fallback_time_on_error If `TRUE`, ID batching can fall back to time batching.
#' @return A data.frame/tibble, a list of results, or a list with audit metadata.
#' @examples
#' \dontrun{
#' # Time batching using explicit bounds
#' res <- get_edsan(
#'   module = "pmsi",
#'   what = "data",
#'   query = list(DATENT = "{2024-01-01,2024-01-31}"),
#'   periods_by = "1 week"
#' )
#'
#' # ID batching with audit output
#' out <- get_edsan(
#'   module = "biol",
#'   what = "data",
#'   query = list(PATID = "1 OR 2 OR 3"),
#'   mode = "ids",
#'   return_audit = TRUE
#' )
#' out$audit
#'
#' # Doceds example with explicit RECDATE bounds
#' doc <- get_edsan(
#'   module = "doceds",
#'   what = "data",
#'   query = list(RECDATE = "{2024-01-01,2024-01-31}")
#' )
#'
#' # Minimal ids-mode example with a vector of IDs
#' ids_out <- get_edsan(
#'   module = "pmsi",
#'   what = "idtriplets",
#'   query = list(PATID = c("10", "11", "12")),
#'   mode = "ids"
#' )
#' }
#' @export
get_edsan <- function(
    module = c("doceds", "pmsi", "biol"),
    what = c("data", "idtriplets"),
    query = list(),
    # time-window batching (default)
    start_date = NULL,
    end_date = NULL,
    periods_by = "1 month",
    periods_prefix = "{",
    periods_suffix = "}",
    batch_key = NULL,
    # retrieval mode
    mode = c("auto", "time", "ids"),
    # id-batching options
    batch_ids_key = NULL,
    max_in_ids = 3500,
    min_in_ids = 50,
    max_out_units = 40000,
    output_count_fn = NULL,
    return_audit = FALSE,
    batch_on_error_only = TRUE,
    fallback_time_on_error = TRUE
) {
  # ---- Argument normalization ----
  module <- match.arg(module)
  what <- match.arg(what)
  mode <- match.arg(mode)
  output_count_fn <- output_count_fn %||% function(x) .edsan_count_out_units(module, x, what)

  # ---- Mode selection (auto/time/ids) ----
  # AUTO: decide whether this is time-batching or id-batching.
  if (mode == "auto") {
    # If user didn't specify which ID key to batch, try to infer it.
    if (is.null(batch_ids_key)) {
      batch_ids_key <- .edsan_choose_batch_ids_key(query)
    }

    # If an ID key is present and looks like a list, default to ids mode.
    if (!is.null(batch_ids_key) && .edsan_id_like(query[[batch_ids_key]])) {
      mode <- "ids"
    } else {
      mode <- "time"
    }
  }

  # ---- Time batching path ----
  if (mode == "time") {
    if (is.null(batch_key)) {
      batch_key <- switch(module, doceds = "RECDATE", pmsi = "DATENT", biol = "DATEXAM")
    }

    window <- try(.edsan_infer_batch_window(module, query, batch_key, start_date, end_date), silent = TRUE)

    # If no time window is provided, we can still try a single unbatched call.
    # This may succeed if the result set stays under API limits.
    if (inherits(window, "try-error")) {
      tr <- .edsan_call(module, query, what)
      if (!tr$ok) {
        stop(paste0(
          "missing_time_window: no explicit time bounds for ", module,
          " and single-call attempt failed. Provide ", batch_key,
          " bounds or use ids batching. Underlying error: ",
          paste(tr$error, collapse = " ")
        ))
      }
      return(.edsan_combine(module, list(tr$value), what))
    }

    # If we do have a window, first attempt a single call (cheaper).
    # Only batch if it fails due to limits (or if batch_on_error_only=FALSE).
    if (isTRUE(batch_on_error_only)) {
      tr0 <- .edsan_call(module, query, what)
      if (tr0$ok) return(.edsan_combine(module, list(tr0$value), what))
      if (!.edsan_is_limit_error(tr0$error)) {
        stop(paste0(
          "single_call_failed: ", module, " request failed for non-limit reason. ",
          "Underlying error: ", paste(tr0$error, collapse = " ")
        ))
      }
    }

    query_periods <- .edsan_make_periods(
      window$start, window$end,
      prefix = periods_prefix,
      suffix = periods_suffix,
      by = periods_by
    )

    raw_batches <- purrr::map(query_periods$period, function(period) {
      q <- query
      q[[batch_key]] <- period
      tr <- .edsan_call(module, q, what)
      if (!tr$ok) return(NULL)
      tr$value
    })

    return(.edsan_combine(module, raw_batches, what))
  }

  # ---- ID batching path ----
  # mode == "ids": adaptive split on input size and/or output size
  # AND semantics across different ID fields: drop redundant higher-level IDs.
  query <- .edsan_normalize_id_query(query, batch_ids_key)
  if (is.null(batch_ids_key) || is.null(query[[batch_ids_key]])) {
    stop("ids mode requires batch_ids_key and query[[batch_ids_key]]")
  }

  ids_all <- .edsan_split_id_string(query[[batch_ids_key]])
  if (length(ids_all) == 0) stop("No ids found in query[[batch_ids_key]]")

  initial_chunks <- split(ids_all, ceiling(seq_along(ids_all) / max_in_ids))

  # Accumulators for results and audit log.
  results <- vector("list", 0)
  audit <- vector("list", 0)

  .process_chunk <- function(chunk_ids) {
    n_in <- length(chunk_ids)

    q <- query
    # IMPORTANT: API requires OR-separated IDs in a single string
    q[[batch_ids_key]] <- paste(chunk_ids, collapse = " OR ")

    tr <- .edsan_call(module, q, what)

    err_is_limit <- if (!tr$ok) .edsan_is_limit_error(tr$error) else FALSE

    if (!tr$ok) {
      if (n_in <= min_in_ids) {
        # Last resort: only if this looks like a LIMIT error, try time-splitting inside this ID chunk.
        if (isTRUE(fallback_time_on_error) && isTRUE(err_is_limit)) {
          if (is.null(batch_key)) {
            batch_key <- switch(module, doceds = "RECDATE", pmsi = "DATENT", biol = "DATEXAM")
          }
          win <- try(.edsan_infer_batch_window(module, query, batch_key, start_date, end_date), silent = TRUE)
          if (!inherits(win, "try-error")) {
            per <- .edsan_make_periods(win$start, win$end, prefix = periods_prefix, suffix = periods_suffix, by = periods_by)
            sub_results <- purrr::map(per$period, function(period) {
              qq <- q
              qq[[batch_key]] <- period
              trp <- .edsan_call(module, qq, what)
              if (!trp$ok) return(NULL)
              trp$value
            })
            sub_combined <- .edsan_combine(module, sub_results, what)
            # If we got anything, accept it and record audit; otherwise fall through to failure logging.
            if ((is.data.frame(sub_combined) && nrow(sub_combined) > 0) || (is.list(sub_combined) && length(sub_combined) > 0)) {
              results[[length(results) + 1]] <<- sub_combined
              audit[[length(audit) + 1]] <<- tibble::tibble(
                module = module,
                batch_ids_key = batch_ids_key,
                n_in = n_in,
                ok = TRUE,
                n_out = output_count_fn(sub_combined),
                reason = "ok_time_fallback",
                error = NA_character_
              )
              return(invisible(NULL))
            }
          }
        }

        audit[[length(audit) + 1]] <<- tibble::tibble(
          module = module,
          batch_ids_key = batch_ids_key,
          n_in = n_in,
          ok = FALSE,
          n_out = NA_integer_,
          reason = if (err_is_limit) "limit_error" else "backend_error",
          error = paste(tr$error, collapse = " ")
        )
        return(invisible(NULL))
      }
      mid <- floor(n_in / 2)
      .process_chunk(chunk_ids[seq_len(mid)])
      .process_chunk(chunk_ids[(mid + 1):n_in])
      return(invisible(NULL))
    }

    n_out <- output_count_fn(tr$value)

    if (!is.na(n_out) && n_out > max_out_units && n_in > min_in_ids) {
      mid <- floor(n_in / 2)
      .process_chunk(chunk_ids[seq_len(mid)])
      .process_chunk(chunk_ids[(mid + 1):n_in])
      return(invisible(NULL))
    }

    results[[length(results) + 1]] <<- tr$value
    audit[[length(audit) + 1]] <<- tibble::tibble(
      module = module,
      batch_ids_key = batch_ids_key,
      n_in = n_in,
      ok = TRUE,
      n_out = n_out,
      reason = "ok",
      error = NA_character_
    )

    invisible(NULL)
  }

  for (ch in initial_chunks) .process_chunk(ch)

  combined <- .edsan_combine(module, results, what)

  if (!return_audit) return(combined)

  list(data = combined, audit = dplyr::bind_rows(audit))
}
