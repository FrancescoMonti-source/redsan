#' Trim DOCEDS documents using DrBERT ONNX model
#'
#' Evaluates hospital document lines using the trained `edsan-doc-trimmer` DrBERT
#' model to strip administrative boilerplate (headers, footers, signatures, and
#' transport vouchers) while preserving clinical narrative and exact grounding
#' coordinates `[start, end]`.
#'
#' @param data A data frame or tibble containing at least `RECTXT` (e.g. `bundle$sources$doceds`).
#' @param python_exe Path to Python executable with `onnxruntime` and `transformers` installed.
#'   Defaults to `Sys.getenv("REDSAN_PYTHON_PATH", Sys.which("python"))`.
#' @param model_dir Path to the directory containing `model.onnx`, `tokenizer.json`, and
#'   `trim_batch_service.py`. If `NULL`, automatically resolved via [.edsan_get_trimmer_dir()].
#'
#' @return The input data frame augmented with:
#'   * `RECTXT_TRIMMED`: clean clinical narrative text.
#'   * `TRIM_REDUCTION_PCT`: percentage of prompt characters/tokens removed.
#'   * `TRIM_IS_BT`: boolean indicating whether the document was an administrative transport voucher.
#'   * `TRIM_PRESERVED_INTERVALS`: list of data frames with `start`, `end`, `family`, and `text`
#'     representing exact character coordinates in raw `RECTXT`.
#'
#' @examples
#' \dontrun{
#' data <- trim_doceds_onnx(bundle$sources$doceds)
#' cat(data$RECTXT_TRIMMED[[1]])
#' }
#'
#' @export
trim_doceds_onnx <- function(
  data,
  python_exe = .edsan_get_python_exe(),
  model_dir = NULL
) {
  if (!is.data.frame(data)) {
    stop("trim_doceds_onnx() requires a data frame or tibble.", call. = FALSE)
  }
  if (!"RECTXT" %in% names(data)) {
    stop(
      "trim_doceds_onnx() requires a 'RECTXT' column in data.",
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
  id_col <- intersect(c("ELTID", "doc_id", "ID"), names(data))[1L]
  ids <- if (!is.na(id_col)) {
    as.character(data[[id_col]])
  } else {
    paste0("doc_", seq_len(nrow(data)))
  }

  payload <- data.frame(
    id = ids,
    text = ifelse(is.na(data$RECTXT), "", as.character(data$RECTXT)),
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

  trimmed_texts <- character(nrow(data))
  reduc_pcts <- numeric(nrow(data))
  is_bts <- logical(nrow(data))
  intervals_list <- vector("list", nrow(data))

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

  data$RECTXT_TRIMMED <- trimmed_texts
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
    file.path(Sys.getenv("USERPROFILE"), "Documents", "Git", "edsan-doc-trimmer", ".venv", "Scripts", "python.exe"),
    file.path(Sys.getenv("HOME"), "Documents", "Git", "edsan-doc-trimmer", ".venv", "Scripts", "python.exe"),
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
