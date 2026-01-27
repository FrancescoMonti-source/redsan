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
                                prefix = "", suffix = "",
                                end_inclusive = TRUE,
                                overlap_days = 0L) {
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
  overlap_days <- as.integer(overlap_days %||% 0L)
  if (overlap_days < 0L) stop("overlap_days must be >= 0")

  base_ends <- if (isTRUE(end_inclusive)) {
    c(starts[-1] - 1, e)
  } else {
    c(starts[-1], e)
  }
  ends <- pmin(base_ends + overlap_days, e)

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
  # Parse API-style date bounds into list(lo, hi)
  if (is.null(x) || length(x) == 0 || is.na(x)) return(list(lo = NULL, hi = NULL))
  x <- trimws(as.character(x))

  # range: "{YYYY-MM-DD,YYYY-MM-DD}"
  m <- stringr::str_match(x, "^\\{\\s*(\\d{4}-\\d{2}-\\d{2})\\s*,\\s*(\\d{4}-\\d{2}-\\d{2})\\s*\\}$")
  if (!is.na(m[1, 1])) {
    return(list(lo = as.Date(m[1, 2]), hi = as.Date(m[1, 3])))
  }

  m <- stringr::str_match(x, "^\\s*(\\d{4}-\\d{2}-\\d{2})\\s*,\\s*(\\d{4}-\\d{2}-\\d{2})\\s*$")
  if (!is.na(m[1, 1])) {
    return(list(lo = as.Date(m[1, 2]), hi = as.Date(m[1, 3])))
  }

  m <- stringr::str_match(
    x,
    "^\\s*[^0-9]*?(\\d{4}-\\d{2}-\\d{2})\\s*,\\s*(\\d{4}-\\d{2}-\\d{2})[^0-9]*\\s*$"
  )
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

.edsan_normalize_date_query <- function(query, date_keys, prefix = "{", suffix = "}") {
  if (length(date_keys) == 0) return(query)

  for (key in date_keys) {
    val <- query[[key]]
    if (is.null(val) || length(val) == 0 || all(is.na(val))) next

    val_chr <- as.character(val)

    if (length(val_chr) >= 2 && all(stringr::str_detect(val_chr[1:2], "^\\d{4}-\\d{2}-\\d{2}$"))) {
      query[[key]] <- paste0(prefix, val_chr[[1]], ",", val_chr[[2]], suffix)
      next
    }

    val1 <- trimws(val_chr[[1]])
    if (stringr::str_detect(val1, "^\\d{4}-\\d{2}-\\d{2}\\s*,\\s*\\d{4}-\\d{2}-\\d{2}$")) {
      query[[key]] <- paste0(prefix, val1, suffix)
    }
  }

  query
}

.edsan_infer_batch_window <- function(module, query, batch_key, start_date = NULL, end_date = NULL) {
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

.edsan_time_backoff_seq <- function(initial = "6 months") {
  ladder <- c("6 months", "3 months", "1 month", "2 weeks", "1 week", "1 day")
  i <- match(initial, ladder)
  if (is.na(i)) i <- 1L
  ladder[i:length(ladder)]
}

.edsan_time_batch_adaptive <- function(
    module, what, query,
    batch_key,
    start_date, end_date,
    periods_prefix = "{", periods_suffix = "}",
    periods_end_inclusive = TRUE,
    periods_overlap_days = 0L,
    initial_by = "6 months",
    max_batches = 1000L,
    verbose = FALSE,
    progress_every = NULL,
    chunk_idx = NA_integer_,
    n_chunks = NA_integer_,
    batch_ids_key = NA_character_,
    n_in = NA_integer_,
    count_out_fn = NULL,
    return_audit = FALSE
) {
  by_seq <- .edsan_time_backoff_seq(initial_by)
  last_limit_error <- NULL

  id_part <- if (!is.na(chunk_idx) && !is.na(n_chunks)) {
    paste0("[", batch_ids_key %||% "ids", " ", chunk_idx, "/", n_chunks,
           if (!is.na(n_in)) paste0(" n_in=", n_in) else "", "] ")
  } else {
    ""
  }

  results <- list()
  audit <- list()
  current_start <- lubridate::as_date(start_date)
  final_end <- lubridate::as_date(end_date)

  for (by in by_seq) {
    if (is.na(current_start) || is.na(final_end) || current_start > final_end) {
      return(list(ok = TRUE, by = by, results = results, by_history = by_seq[1:match(by, by_seq)],
                  audit = dplyr::bind_rows(audit)))
    }

    periods <- .edsan_make_periods(
      current_start, final_end,
      by = by,
      prefix = periods_prefix,
      suffix = periods_suffix,
      end_inclusive = periods_end_inclusive,
      overlap_days = periods_overlap_days
    )

    if (nrow(periods) > as.integer(max_batches)) {
      stop("time_batch_too_many_batches: ", nrow(periods),
           " batches requested at granularity '", by,
           "'. Narrow the time window or increase period size.")
    }

    progress_every <- if (isTRUE(verbose)) 1L else as.integer(progress_every %||% 25L)

    for (i in seq_len(nrow(periods))) {
      per <- periods$period[[i]]

      if (isTRUE(verbose) && (i == 1L || i == nrow(periods) || (i %% progress_every) == 0L)) {
        message(id_part, "[call] ", by, " ", i, "/", nrow(periods),
                " [", format(periods$start[[i]]), "..", format(periods$end[[i]]), "]")
      }

      q <- query
      q[[batch_key]] <- per

      t0 <- proc.time()[[3]]
      tr <- .edsan_call(module, q, what)
      dt_ms <- as.integer(round((proc.time()[[3]] - t0) * 1000))

      n_out <- NA_integer_
      if (tr$ok && is.function(count_out_fn)) {
        n_out <- tryCatch(as.integer(count_out_fn(tr$value)), error = function(e) NA_integer_)
      }

      if (isTRUE(return_audit)) {
        audit[[length(audit) + 1]] <- tibble::tibble(
          module = module,
          what = what,
          batch_key = batch_key,
          batch_ids_key = batch_ids_key %||% NA_character_,
          chunk_idx = chunk_idx,
          n_in = n_in,
          time_by = by,
          time_idx = i,
          time_n = nrow(periods),
          start = as.Date(periods$start[[i]]),
          end = as.Date(periods$end[[i]]),
          period = per,
          ok = tr$ok,
          n_out = n_out,
          dt_ms = dt_ms,
          error = if (tr$ok) NA_character_ else paste(tr$error, collapse = " "),
          kind = if (tr$ok) "ok" else if (.edsan_is_limit_error(tr$error)) "limit" else "error"
        )
      }

      if (isTRUE(verbose)) {
        if (tr$ok) {
          if (!is.na(n_out)) {
            message(id_part, "[ok]   ", by, " ", i, "/", nrow(periods),
                    " [", format(periods$start[[i]]), "..", format(periods$end[[i]]), "]",
                    " | n_out=", n_out, " | ", dt_ms, "ms")
          } else {
            message(id_part, "[ok]   ", by, " ", i, "/", nrow(periods),
                    " [", format(periods$start[[i]]), "..", format(periods$end[[i]]), "]",
                    " | ", dt_ms, "ms")
          }
        } else {
          message(id_part, "[err]  ", by, " ", i, "/", nrow(periods),
                  " [", format(periods$start[[i]]), "..", format(periods$end[[i]]), "]",
                  " | ", if (.edsan_is_limit_error(tr$error)) "LIMIT" else "ERROR",
                  " | ", dt_ms, "ms",
                  " | ", paste(tr$error, collapse = " "))
        }
      }

      if (!tr$ok) {
        if (.edsan_is_limit_error(tr$error)) {
          last_limit_error <- paste(tr$error, collapse = " ")
          current_start <- periods$start[[i]]
          if (isTRUE(verbose)) {
            message(id_part, "[backoff] LIMIT at ", by, " ", i, "/", nrow(periods),
                    " [", format(periods$start[[i]]), "..", format(periods$end[[i]]), "]",
                    " -> switching smaller granularity",
                    " | remaining_from=", format(current_start))
          }
          break
        }

        stop(paste0(
          "time_batch_failed_nonlimit: module=", module,
          ", batch_key=", batch_key,
          ", by=", by,
          ", period=", per,
          ". Underlying error: ",
          paste(tr$error, collapse = " ")
        ))
      }

      results[[length(results) + 1]] <- tr$value

      if (i == nrow(periods)) {
        return(list(ok = TRUE, by = by, results = results, by_history = by_seq[1:match(by, by_seq)],
                    audit = dplyr::bind_rows(audit)))
      }
    }

    if (!is.na(current_start) && current_start <= final_end) {
      next
    }

    return(list(ok = TRUE, by = by, results = results, by_history = by_seq[1:match(by, by_seq)],
                audit = dplyr::bind_rows(audit)))
  }

  stop(paste0(
    "time_batch_exhausted: even 1-day batches hit limit. ",
    "Last limit error: ", last_limit_error %||% "unknown"
  ))
}


# ID-batching helpers
# These functions decide when and how to split ID lists for safe API calls.

.edsan_choose_batch_ids_key <- function(query, candidates = c("ELTID", "EVTID", "PATID")) {
  present <- candidates[candidates %in% names(query)]
  if (length(present) == 0) return(NULL)

  n_ids <- purrr::map_int(present, function(k) {
    v <- query[[k]]
    if (is.null(v)) return(0L)
    length(.edsan_split_id_string(v))
  })

  present <- present[n_ids > 0]
  n_ids   <- n_ids[n_ids > 0]
  if (length(present) == 0) return(NULL)

  spec_order <- match(present, candidates)
  present[order(spec_order, -n_ids)][1]
}

.edsan_split_id_string <- function(x) {
  x <- x %||% character()
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]
  if (length(x) == 0) return(character())

  if (length(x) > 1) return(x)

  s <- x[[1]]
  s <- stringr::str_replace_all(s, "\\s+OR\\s+", " ")
  parts <- unlist(stringr::str_split(s, "[\\s,;]+"))
  parts <- parts[!is.na(parts) & nzchar(parts)]
  parts
}

.edsan_call <- function(module, query, what = c("data", "idtriplets")) {
  what <- match.arg(what)
  qry <- jsonlite::toJSON(query, auto_unbox = TRUE)

  fn <- if (what == "idtriplets") {
    d2imr::d2im_wsc.get_edsan_idtriplets_as_dataframe
  } else {
    d2imr::d2im_wsc.get_edsan_data_as_dataframe
  }

  res <- try(fn(module, qry, "edsan"), silent = TRUE)
  if (inherits(res, "try-error")) return(list(ok = FALSE, value = NULL, error = as.character(res)))
  if (is.list(res) && "error" %in% names(res)) {
    err_val <- res[["error"]]
    if (!is.null(err_val)) {
      err_msg <- as.character(err_val)
      err_msg <- err_msg[!is.na(err_msg) & nzchar(err_msg)]
      if (length(err_msg) > 0) {
        return(list(ok = FALSE, value = NULL, error = err_msg))
      }
    }
  }
  list(ok = TRUE, value = res, error = NULL)
}

.edsan_combine <- function(module, results, what = c("data", "idtriplets")) {
  what <- match.arg(what)

  coerce_doceds_df <- function(result) {
    if (is.null(result)) return(NULL)
    if (is.data.frame(result)) return(result)
    if (is.list(result)) {
      if (!is.null(result$data) && is.data.frame(result$data)) return(result$data)
      if (!is.null(result$result) && is.data.frame(result$result)) return(result$result)
      if (!is.null(result$value) && is.data.frame(result$value)) return(result$value)
      if (length(result) > 0 && all(purrr::map_lgl(result, ~ is.data.frame(.x) || is.list(.x)))) {
        attempt <- tryCatch(dplyr::bind_rows(result), error = function(e) NULL)
        if (is.data.frame(attempt)) return(attempt)
      }
      attempt <- tryCatch(tibble::as_tibble(result), error = function(e) NULL)
      if (is.data.frame(attempt)) return(attempt)
    }
    NULL
  }

  if (what == "idtriplets") {
    rows <- purrr::compact(results)
    if (length(rows) == 0) {
      return(tibble::tibble(ELTID = character(), EVTID = character(), PATID = character()))
    }

    df <- dplyr::bind_rows(rows)

    if (all(c("ELTID", "EVTID", "PATID") %in% names(df))) {
      df <- df[, c("ELTID", "EVTID", "PATID"), drop = FALSE]
      return(dplyr::distinct(df))
    }

    nested_cols <- c("eltId", "evtId", "patId")
    if (all(nested_cols %in% names(df))) {
      out <- df
      if ("eltExt" %in% names(out)) {
        out <- tidyr::unnest(out, cols = c("eltExt", "eltId", "evtId", "patId"))
      } else {
        out <- tidyr::unnest(out, cols = c("eltId", "evtId", "patId"))
      }
      out <- dplyr::rename_with(out, ~ c("ELTID", "EVTID", "PATID"), .cols = c("eltId", "evtId", "patId"))
      out <- out[, c("ELTID", "EVTID", "PATID"), drop = FALSE]
      out <- dplyr::distinct(out)
      return(out)
    }

    warning("idtriplets combine: unexpected shape returned by backend; returning bound rows as-is.")
    return(df)
  }

  if (module == "doceds") {
    coerced <- purrr::map(results, coerce_doceds_df)
    dropped <- sum(purrr::map_lgl(results, ~ !is.null(.x)) & purrr::map_lgl(coerced, is.null))
    if (dropped > 0) warning("Skipped ", dropped, " doceds batch result(s) that were not data frames.")

    return(dplyr::distinct(dplyr::bind_rows(purrr::compact(coerced))))

  }

  purrr::list_flatten(purrr::compact(results))
}

.edsan_normalize_id_query <- function(query, batch_ids_key) {
  if (is.null(batch_ids_key)) return(query)
  if (batch_ids_key == "ELTID") {
    query[c("EVTID", "PATID")] <- NULL
  } else if (batch_ids_key == "EVTID") {
    query["PATID"] <- NULL
  }
  query
}

.edsan_count_out_units <- function(module, value, what = c("data", "idtriplets")) {
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
#' ... (roxygen omitted here for brevity in canvas; keep your original block)
#'
#' @export
get_edsan <- function(
    module = c("doceds", "pmsi", "biol"),
    what = c("data", "idtriplets"),
    query = list(),
    start_date = NULL,
    end_date = NULL,
    periods_by = "6 months",
    periods_prefix = "{",
    periods_suffix = "}",
    periods_end_inclusive = TRUE,
    periods_overlap_days = 0L,
    batch_key = NULL,
    batch_ids_key = NULL,
    max_in_ids = 3500,
    max_time_batches = 1000,
    return_audit = FALSE,
    batch_on_error_only = FALSE,
    verbose = FALSE
) {
  module <- match.arg(module)
  what <- match.arg(what)
  date_keys <- c("RECDATE", "DATENT", "DATSORT", "DATEXAM")
  present_dates <- intersect(names(query), date_keys)
  query <- .edsan_normalize_date_query(query, present_dates, periods_prefix, periods_suffix)

  date_keys <- c("RECDATE", "DATENT", "DATSORT", "DATEXAM")
  present_dates <- intersect(names(query), date_keys)

  if (module == "doceds") {
    bad <- intersect(present_dates, c("DATENT", "DATSORT", "DATEXAM"))
    if (length(bad) > 0) {
      stop("doceds module only supports RECDATE as date key; unsupported key(s): ",
           paste(bad, collapse = ", "), ". Please use RECDATE.")
    }
  }

  if (module == "pmsi") {
    bad <- intersect(present_dates, c("RECDATE", "DATEXAM"))
    if (length(bad) > 0) {
      stop("pmsi module only supports DATENT and DATSORT as date keys; unsupported key(s): ",
           paste(bad, collapse = ", "), ". Please use DATENT or DATSORT.")
    }
  }

  if (module == "biol") {
    bad <- intersect(present_dates, c("RECDATE", "DATENT", "DATSORT"))
    if (length(bad) > 0) {
      stop("biol module only supports DATEXAM as date key; unsupported key(s): ",
           paste(bad, collapse = ", "), ". Please use DATEXAM.")
    }
  }

  if (is.null(batch_key)) {
    batch_key <- switch(module, doceds = "RECDATE", pmsi = "DATENT", biol = "DATEXAM")
  }

  if (is.null(batch_ids_key)) {
    batch_ids_key <- .edsan_choose_batch_ids_key(query, candidates = c("ELTID", "EVTID", "PATID"))
  }

  query <- .edsan_normalize_id_query(query, batch_ids_key)

  ids_all <- character()
  if (!is.null(batch_ids_key) && !is.null(query[[batch_ids_key]])) {
    ids_all <- .edsan_split_id_string(query[[batch_ids_key]])
  }

  id_chunks <- if (length(ids_all) == 0) {
    list(NULL)
  } else {
    split(ids_all, ceiling(seq_along(ids_all) / max_in_ids))
  }

  if (isTRUE(verbose)) {
    window0 <- try(.edsan_infer_batch_window(module, query, batch_key, start_date, end_date), silent = TRUE)
    est_n_time <- NA_integer_
    est_total_calls <- NA_integer_
    if (!inherits(window0, "try-error")) {
      est_n_time <- tryCatch({
        nrow(.edsan_make_periods(window0$start, window0$end,
                                 by = periods_by,
                                 prefix = periods_prefix,
                                 suffix = periods_suffix,
                                 end_inclusive = periods_end_inclusive,
                                 overlap_days = as.integer(periods_overlap_days %||% 0L)))
      }, error = function(e) NA_integer_)
      if (!is.na(est_n_time)) {
        est_total_calls <- as.integer(length(id_chunks) * est_n_time)
      }
    }

    message("[plan] module=", module,
            " | what=", what,
            " | batch_key=", batch_key,
            if (!is.null(batch_ids_key) && length(ids_all) > 0) paste0(" | batch_ids_key=", batch_ids_key, " n_ids=", length(ids_all), " id_chunks=", length(id_chunks)) else " | no_ids",
            " | periods_by=", periods_by,
            if (!is.na(est_n_time)) paste0(" | time_chunks=", est_n_time) else "",
            if (!is.na(est_total_calls)) paste0(" | est_calls=", est_total_calls) else "")
  }

  audit <- vector("list", 0)
  results <- vector("list", 0)

  current_by <- periods_by

  for (chunk_idx in seq_along(id_chunks)) {
    chunk_ids <- id_chunks[[chunk_idx]]

    q <- query
    n_in <- NA_integer_
    if (!is.null(chunk_ids)) {
      n_in <- length(chunk_ids)
      q[[batch_ids_key]] <- paste(chunk_ids, collapse = " OR ")
    }

    window <- try(.edsan_infer_batch_window(module, q, batch_key, start_date, end_date), silent = TRUE)

    if (inherits(window, "try-error")) {
      if (isTRUE(verbose)) {
        message("[single] chunk ", chunk_idx, "/", length(id_chunks),
                " | no time bounds found for batch_key=", batch_key,
                " -> single call")
      }
      tr <- .edsan_call(module, q, what)

      if (isTRUE(return_audit)) {
        audit[[length(audit) + 1]] <- tibble::tibble(
          module = module,
          what = what,
          batch_key = batch_key,
          batch_ids_key = batch_ids_key %||% NA_character_,
          chunk_idx = chunk_idx,
          n_in = n_in,
          strategy = "single",
          time_by = NA_character_,
          period = NA_character_,
          ok = tr$ok,
          error = if (tr$ok) NA_character_ else paste(tr$error, collapse = " ")
        )
      }

      if (!tr$ok) {
        stop(paste0(
          "missing_time_window: no usable time bounds for ", module,
          " (batch_key=", batch_key, ") and single-call failed. Underlying error: ",
          paste(tr$error, collapse = " ")
        ))
      }

      results[[length(results) + 1]] <- tr$value
      next
    }

    if (isTRUE(batch_on_error_only)) {
      tr0 <- .edsan_call(module, q, what)

      if (isTRUE(return_audit)) {
        audit[[length(audit) + 1]] <- tibble::tibble(
          module = module,
          what = what,
          batch_key = batch_key,
          batch_ids_key = batch_ids_key %||% NA_character_,
          chunk_idx = chunk_idx,
          n_in = n_in,
          strategy = "probe",
          time_by = NA_character_,
          period = NA_character_,
          ok = tr0$ok,
          error = if (tr0$ok) NA_character_ else paste(tr0$error, collapse = " ")
        )
      }

      if (tr0$ok) {
        results[[length(results) + 1]] <- tr0$value
        next
      }

      if (!.edsan_is_limit_error(tr0$error)) {
        stop(paste0(
          "single_call_failed: ", module, " request failed for non-limit reason. ",
          "Underlying error: ", paste(tr0$error, collapse = " ")
        ))
      }
    }

    tb <- .edsan_time_batch_adaptive(
      module = module,
      what = what,
      query = q,
      batch_key = batch_key,
      start_date = window$start,
      end_date = window$end,
      periods_prefix = periods_prefix,
      periods_suffix = periods_suffix,
      periods_end_inclusive = periods_end_inclusive,
      periods_overlap_days = as.integer(periods_overlap_days %||% 0L),
      initial_by = current_by,
      max_batches = max_time_batches,
      verbose = verbose,
      chunk_idx = as.integer(chunk_idx),
      n_chunks = as.integer(length(id_chunks)),
      batch_ids_key = batch_ids_key %||% NA_character_,
      n_in = as.integer(n_in),
      count_out_fn = function(x) .edsan_count_out_units(module, x, what),
      return_audit = return_audit
    )

    if (isTRUE(return_audit) && !is.null(tb$audit) && is.data.frame(tb$audit) && nrow(tb$audit) > 0) {
      audit[[length(audit) + 1]] <- tb$audit
    }

    if (isTRUE(return_audit)) {
      audit[[length(audit) + 1]] <- tibble::tibble(
        module = module,
        what = what,
        batch_key = batch_key,
        batch_ids_key = batch_ids_key %||% NA_character_,
        chunk_idx = chunk_idx,
        n_in = n_in,
        strategy = "time",
        time_by = tb$by,
        period = NA_character_,
        ok = TRUE,
        error = NA_character_
      )
    }

    results[[length(results) + 1]] <- .edsan_combine(module, tb$results, what)

    if (!identical(tb$by, current_by)) {
      if (isTRUE(verbose)) {
        message("[plan] downgraded time granularity to '", tb$by, "' for remaining chunks")
      }
      current_by <- tb$by
    }
  }

  combined <- .edsan_combine(module, results, what)

  if (!return_audit) return(combined)

  list(data = combined, audit = dplyr::bind_rows(audit))
}
