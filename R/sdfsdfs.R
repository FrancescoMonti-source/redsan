.edsan_is_limit_error <- function(err) {
  if (is.null(err)) return(FALSE)
  err <- paste(err, collapse = " ")
  stringr::str_detect(
    stringr::str_to_lower(err),
    "(4000|40000|40 000|too many|limit|quota|max results|max rows)"
  )
}


`%||%` <- function(x, y) if (!is.null(x)) x else y

.pmsi_has_time <- function(x) {
  x <- as.character(x %||% "")
  stringr::str_detect(x, "\\d{2}:\\d{2}")
}

.pmsi_parse_datetime <- function(x, tz = "Europe/Paris") {
  # Robust parsing for both date-only and datetime strings.
  # Returns POSIXct (NA where parsing fails).
  if (is.null(x)) return(x)
  if (inherits(x, "POSIXt")) return(x)
  x <- as.character(x)
  lubridate::parse_date_time(
    x,
    orders = c(
      "Y-m-d H:M:S", "Y-m-d H:M", "Y-m-d",
      "d/m/Y H:M:S", "d/m/Y H:M", "d/m/Y",
      "Ymd HMS", "Ymd HM", "Ymd"
    ),
    quiet = TRUE,
    tz = tz
  )
}

.pmsi_time_hms <- function(dt, raw) {
  # Return hms time if raw had an explicit time component, else NA.
  out <- rep(NA_character_, length(dt))
  ok <- !is.na(dt) & .pmsi_has_time(raw)
  out[ok] <- format(dt[ok], "%H:%M:%S")
  hms::as_hms(out)
}

# -----------------------------
# BIOL processing API
# -----------------------------

.biol_prepare <- function(data) {
  # data: list of lab exams; each element has metadata + RESULTATS (data.frame)
  purrr::imap(data, function(entry, biol_id) {
    if (is.null(entry) || !is.list(entry)) return(NULL)
    results <- entry$RESULTATS
    if (is.null(results) || (is.data.frame(results) && nrow(results) == 0)) return(NULL)

    meta <- tibble::tibble(
      PATID = entry$PATID,
      EVTID = entry$EVTID,
      ELTID = entry$ELTID,
      BIOL_ID = biol_id,
      DATEXAM = entry$DATEXAM,
      SEJUM = entry$SEJUM,
      SEJUF = entry$SEJUF,
      PATBD = entry$PATBD,
      PATAGE = entry$PATAGE,
      PATSEX = entry$PATSEX,
      CSTE_LABO = entry$CSTE_LABO
    )

    # Ensure results is a tibble
    res_tbl <- tibble::as_tibble(results)

    # Repeat meta for each result row, then bind columns
    out <- dplyr::bind_cols(meta[rep(1, nrow(res_tbl)), , drop = FALSE], res_tbl)

    out
  }) %>% purrr::compact()
}

.biol_results <- function(data) {
  # Accept either raw list (API chunks) or already prepared list of tibbles
  if (is.list(data) && !is.data.frame(data)) {
    rows <- .biol_prepare(data)
    if (length(rows) == 0) return(tibble::tibble())
    df <- dplyr::bind_rows(rows)
  } else {
    df <- tibble::as_tibble(data)
  }

  # Parse DATEXAM and compute HEURE_DATEXAM (only if time was explicit in raw string)
  if ("DATEXAM" %in% names(df)) {
    raw <- as.character(df$DATEXAM)
    df$DATEXAM <- .pmsi_parse_datetime(raw)
    df$HEURE_DATEXAM <- .pmsi_time_hms(df$DATEXAM, raw)
  }

  df
}

process_biol <- function(data) {
  .biol_results(data)
}

# -----------------------------
# PMSI processing API
# -----------------------------

# Pure core: flatten + parse DATENT/DATSORT + compute HEURE_*
.pmsi_prepare <- function(data) {
  clean_data <- lapply(data, function(x) {
    flat <- unlist(x, recursive = TRUE)
    flat <- lapply(flat, function(v) {
      if (is.null(v) || length(v) == 0) {
        NA_character_
      } else if (length(v) > 1) {
        paste(as.character(v), collapse = ";")
      } else {
        as.character(v)
      }
    })
    tibble::as_tibble(flat)
  }) %>% dplyr::bind_rows()

  if ("DATENT" %in% names(clean_data)) {
    raw <- clean_data$DATENT
    clean_data$DATENT <- .pmsi_parse_datetime(raw)
    clean_data$HEURE_DATENT <- .pmsi_time_hms(clean_data$DATENT, raw)
  } else {
    clean_data$HEURE_DATENT <- hms::as_hms(rep(NA_character_, nrow(clean_data)))
  }

  if ("DATSORT" %in% names(clean_data)) {
    raw <- clean_data$DATSORT
    clean_data$DATSORT <- .pmsi_parse_datetime(raw)
    clean_data$HEURE_DATSORT <- .pmsi_time_hms(clean_data$DATSORT, raw)
  } else {
    clean_data$HEURE_DATSORT <- hms::as_hms(rep(NA_character_, nrow(clean_data)))
  }

  clean_data
}

# Main (stay-level) table
.pmsi_main <- function(data) {
  cols <- c(
    "PATID", "EVTID", "ELTID",
    "DATENT", "HEURE_DATENT",
    "DATSORT", "HEURE_DATSORT",
    "PATBD", "PATAGE", "PATSEX",
    "SEJDUR", "MODEENT", "MODESORT",
    "PMSISTATUT", "SEJUM", "SEJUF",
    "GHM", "SEVERITE", "SRC"
  )
  data %>% dplyr::select(dplyr::any_of(cols)) %>% dplyr::distinct()
}

# Actes (long format)
.pmsi_actes <- function(data) {
  # assume DATENT/DATSORT already POSIXct and HEURE_* already set by pmsi_prepare
  data %>%
    dplyr::select(
      PATID, EVTID, ELTID, PATBD, PATAGE, PATSEX,
      DATENT, HEURE_DATENT, DATSORT, HEURE_DATSORT,
      SEJDUR, SEJUM, SEJUF,
      dplyr::contains("ACTE"), dplyr::contains("UFPRO"), dplyr::contains("UFDEM")
    ) %>%
    tidyr::pivot_longer(
      cols = c(
        dplyr::contains("CODEACTE"),
        dplyr::contains("DATEACTE"),
        dplyr::contains("UFPRO"),
        dplyr::contains("UFDEM"),
        dplyr::contains("NOMENCLATURE")
      ),
      names_to = c(".value"),
      names_pattern = "(CODEACTE|DATEACTE|UFPRO|UFDEM|NOMENCLATURE).*",
      values_drop_na = TRUE
    ) %>%
    dplyr::distinct()
}

# Diagnoses (derived from DALL)
.pmsi_diag <- function(data) {
  data %>%
    dplyr::select(dplyr::ends_with("ID"), DATENT, HEURE_DATENT, DATSORT, HEURE_DATSORT, DALL) %>%
    tidyr::separate_rows(DALL, sep = " ") %>%
    dplyr::filter(stringr::str_detect(DALL, "\\d")) %>%
    dplyr::mutate(
      diag      = stringr::str_extract(DALL, ":(.+)", group = 1),
      type_diag = stringr::str_sub(DALL, 1, 2)
    ) %>%
    dplyr::select(-DALL)
}

# Orchestrator (pure): returns all 3 tables
process_pmsi <- function(data) {
  cleaned <- .pmsi_prepare(data)
  list(
    main  = .pmsi_main(cleaned),
    actes = .pmsi_actes(cleaned),
    diag  = .pmsi_diag(cleaned)
  )
}


# Helpers
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
  # Parse API-style date bounds into list(lo, hi)
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
  # Explicit orchestration always wins
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

# get edsan data

.edsan_choose_batch_ids_key <- function(query, candidates = c("ELTID", "EVTID", "PATID")) {
  # Prefer the most specific identifier available.
  present <- candidates[candidates %in% names(query) & !purrr::map_lgl(query[candidates], is.null)]
  if (length(present) == 0) return(NULL)

  counts <- purrr::map_int(present, function(k) length(.edsan_split_id_string(query[[k]])))
  spec_order <- match(present, candidates) # smaller index = more specific
  present[order(spec_order, -counts)][1]
}

.edsan_id_like <- function(x) {
  # detect whether x likely encodes a list of IDs (vector or OR-separated string)
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
  what <- match.arg(what)
  if (what == "idtriplets") {
    return(dplyr::bind_rows(purrr::compact(results)) %>%
             unnest(cols = c(eltExt, eltId, evtId, patId)) %>%
             select(patId,evtId,eltId) %>%
             rename(ELTID = eltId,EVTID = evtId,PATID = patId)
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
  module <- match.arg(module)
  what <- match.arg(what)
  mode <- match.arg(mode)
  output_count_fn <- output_count_fn %||% function(x) .edsan_count_out_units(module, x, what)

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

  # mode == "ids": adaptive split on input size and/or output size
  # AND semantics across different ID fields: drop redundant higher-level IDs.
  query <- .edsan_normalize_id_query(query, batch_ids_key)
  if (is.null(batch_ids_key) || is.null(query[[batch_ids_key]])) {
    stop("ids mode requires batch_ids_key and query[[batch_ids_key]]")
  }

  ids_all <- .edsan_split_id_string(query[[batch_ids_key]])
  if (length(ids_all) == 0) stop("No ids found in query[[batch_ids_key]]")

  initial_chunks <- split(ids_all, ceiling(seq_along(ids_all) / max_in_ids))

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



