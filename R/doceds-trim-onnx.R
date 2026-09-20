#' Trim DOCEDS documents using DrBERT ONNX model
#'
#' Evaluates hospital document lines using the trained `edsan-doc-trimmer` DrBERT
#' model to strip administrative boilerplate (headers, footers, signatures, and
#' transport vouchers) while preserving clinical narrative and exact grounding
#' coordinates `[start, end]`.
#'
#' @param data A character vector of texts, or a data frame / tibble containing text.
#' @param text_col Column name containing the text if `data` is a data frame.
#'   Defaults to `NULL`, which automatically checks `"RECTXT"`, `"text"`, `"raw_text"`,
#'   `"content"`, or `"document"`.
#' @param python_exe Path to Python executable with `onnxruntime` and `tokenizers` installed.
#'   Defaults to auto-detecting the `edsan-doc-trimmer` virtual environment or `REDSAN_PYTHON_PATH`.
#' @param model_dir Path to the directory containing `model.onnx`, `tokenizer.json`, and
#'   `trim_batch_service.py`. If `NULL`, automatically resolved via [.edsan_get_trimmer_dir()].
#'
#' @return If `data` is a character vector, returns a character vector of trimmed texts.
#'   If `data` is a data frame, returns `data` augmented with `RECTXT_TRIMMED` (or
#'   `<text_col>_TRIMMED`), `TRIM_REDUCTION_PCT`, `TRIM_IS_BT`, and `TRIM_PRESERVED_INTERVALS`.
#'
#' @examples
#' \dontrun{
#' # On a data frame
#' data <- trim_doceds_onnx(bundle$sources$doceds)
#'
#' # Directly on a character vector
#' clean_text <- trim_doceds_onnx(bundle$sources$doceds$RECTXT)
#'
#' # On an entire event bundle
#' clean_bundle <- trim_doceds_onnx(bundle)
#'
#' # On a list of event bundles (e.g. denut cohort)
#' clean_bundles <- trim_doceds_onnx(denut)
#' }
#'
#' @export
trim_doceds_onnx <- function(
  data,
  text_col = NULL,
  python_exe = .edsan_get_python_exe(),
  model_dir = NULL
) {
  # 1. Single bundle support
  if (inherits(data, "edsan_event_bundle")) {
    if (!is.null(data$sources$doceds) && nrow(data$sources$doceds) > 0) {
      data$sources$doceds <- trim_doceds_onnx(
        data = data$sources$doceds,
        text_col = text_col,
        python_exe = python_exe,
        model_dir = model_dir
      )
    }
    return(data)
  }

  # 2. List of bundles support (High-performance Batch-and-Split)
  if (is.list(data) && !is.data.frame(data) && length(data) > 0 && inherits(data[[1L]], "edsan_event_bundle")) {
    doc_list <- vector("list", length(data))
    for (i in seq_along(data)) {
      d <- data[[i]]$sources$doceds
      if (!is.null(d) && nrow(d) > 0) {
        d$.bundle_idx <- i
        doc_list[[i]] <- d
      }
    }
    all_docs <- do.call(rbind, doc_list[!vapply(doc_list, is.null, logical(1))])
    if (is.null(all_docs) || nrow(all_docs) == 0) {
      return(data)
    }

    # Run ONE single batched GPU forward pass across the entire cohort!
    trimmed_all <- trim_doceds_onnx(
      data = all_docs,
      text_col = text_col,
      python_exe = python_exe,
      model_dir = model_dir
    )

    # Split back into individual bundles
    split_docs <- split(trimmed_all, trimmed_all$.bundle_idx)
    for (idx_str in names(split_docs)) {
      i <- as.integer(idx_str)
      sub_df <- split_docs[[idx_str]]
      sub_df$.bundle_idx <- NULL
      data[[i]]$sources$doceds <- sub_df
    }
    return(data)
  }

  is_char_input <- is.character(data)

  if (is_char_input) {
    df <- data.frame(
      doc_id = paste0("doc_", seq_along(data)),
      RECTXT = data,
      stringsAsFactors = FALSE
    )
    col_to_use <- "RECTXT"
  } else if (is.data.frame(data)) {
    df <- data
    if (is.null(text_col)) {
      candidates <- c("RECTXT", "text", "raw_text", "content", "document")
      found <- candidates[candidates %in% names(df)]
      if (length(found) > 0) {
        col_to_use <- found[1L]
      } else {
        stop(
          sprintf(
            "Could not auto-detect text column in data. Candidates searched: %s. Please specify 'text_col'.",
            paste(candidates, collapse = ", ")
          ),
          call. = FALSE
        )
      }
    } else {
      if (!text_col %in% names(df)) {
        stop(sprintf("Column '%s' not found in data.", text_col), call. = FALSE)
      }
      col_to_use <- text_col
    }
  } else {
    stop(
      "trim_doceds_onnx() requires a character vector, data frame, tibble, edsan_event_bundle, or list of bundles.",
      call. = FALSE
    )
  }

  if (is.null(model_dir)) {
    model_dir <- .edsan_get_trimmer_dir()
  }

  service_script <- file.path(model_dir, "trim_batch_service.py")
  if (!file.exists(service_script)) {
    stop(
      sprintf("Trimming service script not found at: %s", service_script),
      call. = FALSE
    )
  }

  if (!nzchar(python_exe) || !file.exists(python_exe)) {
    stop(
      "Valid Python executable not found. Please set REDSAN_PYTHON_PATH in your .Renviron.",
      call. = FALSE
    )
  }

  # Build JSON payload
  id_col <- intersect(c("ELTID", "doc_id", "ID"), names(df))[1L]
  ids <- if (!is.na(id_col)) {
    as.character(df[[id_col]])
  } else {
    paste0("doc_", seq_len(nrow(df)))
  }

  raw_texts <- df[[col_to_use]]
  payload <- data.frame(
    id = ids,
    text = ifelse(is.na(raw_texts), "", as.character(raw_texts)),
    stringsAsFactors = FALSE
  )

  tmp_in <- tempfile(fileext = ".json")
  tmp_out <- tempfile(fileext = ".json")
  on.exit(unlink(c(tmp_in, tmp_out)), add = TRUE)

  writeLines(
    jsonlite::toJSON(payload, auto_unbox = TRUE),
    tmp_in,
    useBytes = TRUE
  )

  # Execute Python ONNX inference worker
  res <- processx::run(
    command = python_exe,
    args = c(
      service_script,
      "--input",
      tmp_in,
      "--output",
      tmp_out,
      "--onnx_dir",
      model_dir
    ),
    echo_cmd = FALSE,
    error_on_status = TRUE
  )

  output_data <- jsonlite::fromJSON(tmp_out, simplifyVector = FALSE)

  trimmed_texts <- character(nrow(df))
  reduc_pcts <- numeric(nrow(df))
  is_bts <- logical(nrow(df))
  intervals_list <- vector("list", nrow(df))

  for (i in seq_along(output_data)) {
    item <- output_data[[i]]
    trimmed_texts[i] <- if (!is.null(item$trimmed_text)) {
      item$trimmed_text
    } else {
      ""
    }
    reduc_pcts[i] <- if (!is.null(item$reduction_pct)) {
      as.numeric(item$reduction_pct)
    } else {
      0.0
    }
    is_bts[i] <- isTRUE(item$is_bt)

    if (length(item$preserved_intervals) > 0) {
      intervals_list[[i]] <- do.call(
        rbind,
        lapply(
          item$preserved_intervals,
          as.data.frame,
          stringsAsFactors = FALSE
        )
      )
    } else {
      intervals_list[[i]] <- data.frame(
        start = integer(),
        end = integer(),
        family = character(),
        text = character(),
        stringsAsFactors = FALSE
      )
    }
  }

  if (is_char_input) {
    return(trimmed_texts)
  }

  out_col <- if (col_to_use == "RECTXT") {
    "RECTXT_TRIMMED"
  } else {
    paste0(col_to_use, "_TRIMMED")
  }
  data[[out_col]] <- trimmed_texts
  data$TRIM_REDUCTION_PCT <- reduc_pcts
  data$TRIM_IS_BT <- is_bts
  data$TRIM_PRESERVED_INTERVALS <- intervals_list

  data
}

#' Install edsan-doc-trimmer Model for Air-Gapped HDW Environments
#'
#' Extracts and registers a downloaded `edsan-doc-trimmer-v1.0.0.zip` file into
#' the persistent user cache directory.
#'
#' @param zip_path Path to the `edsan-doc-trimmer-v1.0.0.zip` file.
#' @param dest_dir Destination directory. Defaults to `edsan_trimmer_cache_dir()`.
#'
#' @return The path to the installed model directory (invisibly).
#' @export
edsan_install_trimmer <- function(
  zip_path,
  dest_dir = edsan_trimmer_cache_dir()
) {
  if (!file.exists(zip_path)) {
    stop(sprintf("Zip file does not exist: %s", zip_path), call. = FALSE)
  }
  dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)
  utils::unzip(zip_path, exdir = dest_dir)
  message(sprintf(
    "edsan-doc-trimmer model successfully installed to: %s",
    dest_dir
  ))
  invisible(dest_dir)
}

#' Standard user cache directory for edsan-doc-trimmer
#'
#' @return Path string to the cache folder.
#' @export
edsan_trimmer_cache_dir <- function() {
  file.path(tools::R_user_dir("edsan_doc_trimmer", "cache"), "v1")
}

#' Resolve edsan-doc-trimmer Python Executable
#'
#' @noRd
.edsan_get_python_exe <- function() {
  env_path <- Sys.getenv("REDSAN_PYTHON_PATH", "")
  if (nzchar(env_path) && file.exists(env_path)) {
    return(normalizePath(env_path))
  }
  candidates <- c(
    file.path(
      Sys.getenv("USERPROFILE"),
      "Documents",
      "Git",
      "edsan-doc-trimmer",
      ".venv",
      "Scripts",
      "python.exe"
    ),
    file.path(
      Sys.getenv("HOME"),
      "Documents",
      "Git",
      "edsan-doc-trimmer",
      ".venv",
      "Scripts",
      "python.exe"
    ),
    file.path("..", "edsan-doc-trimmer", ".venv", "Scripts", "python.exe"),
    Sys.which("python")
  )
  for (cand in candidates) {
    if (nzchar(cand) && file.exists(cand)) {
      return(normalizePath(cand))
    }
  }
  ""
}

#' Resolve edsan-doc-trimmer Model Directory
#'
#' Locates the trimmer model through:
#' 1. `Sys.getenv("EDSAN_TRIMMER_PATH")`
#' 2. Local user cache directory (`edsan_trimmer_cache_dir()`)
#' 3. Automatic download from GitHub Releases (when online)
#'
#' @noRd
.edsan_get_trimmer_dir <- function() {
  # 1. Environment variable override
  env_path <- Sys.getenv("EDSAN_TRIMMER_PATH", "")
  if (!nzchar(env_path)) {
    env_path <- Sys.getenv("REDSAN_TRIMMER_PATH", "")
  }
  if (nzchar(env_path) && file.exists(file.path(env_path, "model.onnx"))) {
    return(normalizePath(env_path))
  }

  # 2. Check local user cache directory
  cache_dir <- edsan_trimmer_cache_dir()
  if (file.exists(file.path(cache_dir, "model.onnx"))) {
    return(normalizePath(cache_dir))
  }

  # 3. Check for internet connectivity to auto-download from GitHub Releases
  can_download <- FALSE
  tryCatch(
    {
      con <- suppressWarnings(url("https://github.com", open = "rb"))
      close(con)
      can_download <- TRUE
    },
    error = function(e) {}
  )

  if (can_download) {
    message(
      "edsan-doc-trimmer model not found locally. Downloading release v1.0.0 from GitHub (392 MB)..."
    )
    dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
    release_url <- "https://github.com/FrancescoMonti-source/edsan-doc-trimmer/releases/download/v1.0.0/edsan-doc-trimmer-v1.0.0.zip"
    tmp_zip <- tempfile(fileext = ".zip")
    on.exit(unlink(tmp_zip), add = TRUE)
    utils::download.file(release_url, tmp_zip, mode = "wb")
    utils::unzip(tmp_zip, exdir = cache_dir)
    message(sprintf("Installed successfully to: %s", cache_dir))
    return(normalizePath(cache_dir))
  }

  # 4. Air-gapped environment fallback
  stop(
    "edsan-doc-trimmer model not found.\n",
    "Air-gapped hospital HDW environment detected (no internet connection).\n",
    "Please install the model by running:\n",
    "  edsan_install_trimmer('path/to/edsan-doc-trimmer-v1.0.0.zip')\n",
    "or set EDSAN_TRIMMER_PATH in your .Renviron.",
    call. = FALSE
  )
}
