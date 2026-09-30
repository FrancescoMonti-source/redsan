#' Trim DOCEDS documents with a versioned external worker
#'
#' Sends DOCEDS text to a compatible `edsan-doc-trimmer` runtime artifact and
#' maps its results back to the original warehouse objects. Worker results must
#' preserve request identity and provide intervals grounded exactly in the
#' original text. The returned trimmed text must contain the ordered interval
#' content exactly, apart from worker-selected whitespace.
#'
#' @param data A character vector of texts, a data frame / tibble containing text,
#'   a single `edsan_event_bundle`, or a list of event bundles.
#' @param text_col Column name containing the text if `data` is a data frame or bundle.
#'   Defaults to `NULL`, which automatically checks `"RECTXT"`, `"text"`, `"raw_text"`,
#'   `"content"`, or `"document"`.
#' @param python_exe Path to Python executable with `onnxruntime` and `tokenizers` installed.
#'   Defaults to auto-detecting the `edsan-doc-trimmer` virtual environment or `REDSAN_PYTHON_PATH`.
#' @param model_dir Path to the versioned runtime artifact containing `model.onnx`,
#'   `tokenizer.json`, `trim_batch_service.py`, and `artifact.json`. If `NULL`,
#'   the artifact is discovered as described in [edsan_trimmer_cache_dir()].
#'
#' @return Depending on the input shape:
#'   \itemize{
#'     \item **Character vector**: Returns trimmed texts with a
#'       `TRIM_EXECUTION_PROVIDER` attribute when the worker reports its provider.
#'     \item **Data frame / tibble**: Returns the input table augmented with
#'       `<text_col>_TRIMMED`, `TRIM_REDUCTION_PCT`,
#'       `TRIM_PRESERVED_INTERVALS` (character JSON string), and
#'       `TRIM_EXECUTION_PROVIDER`.
#'     \item **Single bundle (`edsan_event_bundle`)**: Returns the bundle with its `sources$doceds`
#'       table augmented.
#'     \item **List of bundles**: Evaluates all document texts in a single batch
#'       and returns the list of bundles with each `sources$doceds` augmented, leaving all original
#'       schema structures and attributes intact.
#'   }
#'
#' Each top-level call that runs the worker prints one line,
#' `edsan-doc-trimmer <version> (<path>)`, naming the artifact that ran. The line
#' is printed once per call, not per chunk or per bundle, and not at all when the
#' input is empty and no worker is started. [doceds_onnx_spec()] returns the same
#' identification programmatically.
#'
#' The runtime artifact owns model selection and inference policy. `redsan`
#' validates the artifact and response protocol, but does not reproduce those
#' policies in R. The worker reads `EDSAN_TRIMMER_DEVICE`, which defaults to
#' `auto` and accepts `cpu`, `cuda`, `openvino`, `dml`, or
#' `migraphx`. It records the selected ONNX Runtime provider in
#' `TRIM_EXECUTION_PROVIDER` on tables or as the same-named attribute on
#' character-vector results. Older compatible artifacts that omit the provider
#' yield `NA`.
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
    .doceds_onnx_validate_bundle(data, text_col = text_col)
    data$sources$doceds <- trim_doceds_onnx(
      data = data$sources$doceds,
      text_col = text_col,
      python_exe = python_exe,
      model_dir = model_dir
    )
    return(data)
  }

  # 2. List of bundles support (Cohort Batching without mutating data frames)
  if (is.list(data) && !is.data.frame(data)) {
    if (length(data) == 0L) {
      return(data)
    }
    if (!all(vapply(data, inherits, logical(1), what = "edsan_event_bundle"))) {
      stop(
        "Every cohort element must be an edsan_event_bundle.",
        call. = FALSE
      )
    }
    invisible(lapply(data, .doceds_onnx_validate_bundle, text_col = text_col))

    bundle_map <- vector("list", length(data))
    all_texts <- character()

    for (i in seq_along(data)) {
      doc <- data[[i]]$sources$doceds
      col <- .doceds_onnx_text_col(doc, text_col)
      if (nrow(doc) == 0L) {
        data[[i]]$sources$doceds <- .doceds_onnx_add_output_columns(doc, col)
      } else {
        bundle_map[[i]] <- seq.int(
          length(all_texts) + 1L,
          length(all_texts) + nrow(doc)
        )
        all_texts <- c(all_texts, as.character(doc[[col]]))
      }
    }

    if (length(all_texts) == 0L) {
      return(data)
    }

    # Delegate the batched texts to trim_doceds_onnx on a simple standard dataframe
    flat_df <- data.frame(
      doc_id = paste0("doc_", seq_along(all_texts)),
      RECTXT = all_texts,
      stringsAsFactors = FALSE
    )

    trimmed_flat <- trim_doceds_onnx(
      data = flat_df,
      text_col = "RECTXT",
      python_exe = python_exe,
      model_dir = model_dir
    )

    # Directly assign results back to each bundle without altering any other columns or attributes
    col_to_assign <- if (!is.null(text_col)) {
      paste0(text_col, "_TRIMMED")
    } else {
      "RECTXT_TRIMMED"
    }
    for (i in seq_along(data)) {
      indices <- bundle_map[[i]]
      if (!is.null(indices) && length(indices) > 0) {
        data[[i]]$sources$doceds[[
          col_to_assign
        ]] <- trimmed_flat$RECTXT_TRIMMED[indices]
        data[[
          i
        ]]$sources$doceds$TRIM_REDUCTION_PCT <- trimmed_flat$TRIM_REDUCTION_PCT[
          indices
        ]
        data[[
          i
        ]]$sources$doceds$TRIM_PRESERVED_INTERVALS <- trimmed_flat$TRIM_PRESERVED_INTERVALS[
          indices
        ]
        data[[
          i
        ]]$sources$doceds$TRIM_EXECUTION_PROVIDER <- trimmed_flat$TRIM_EXECUTION_PROVIDER[
          indices
        ]
      }
    }
    return(data)
  }

  is_char_input <- is.character(data)

  if (is_char_input) {
    if (length(data) == 0L) {
      return(character(0))
    }
    df <- data.frame(
      doc_id = paste0("doc_", seq_along(data)),
      RECTXT = data,
      stringsAsFactors = FALSE
    )
    col_to_use <- "RECTXT"
  } else if (is.data.frame(data)) {
    df <- data
    col_to_use <- .doceds_onnx_text_col(df, text_col)
    if (nrow(data) == 0L) {
      return(.doceds_onnx_add_output_columns(data, col_to_use))
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
  artifact_version <- .doceds_onnx_validate_artifact(model_dir)
  .doceds_onnx_announce_artifact(artifact_version, model_dir)

  service_script <- file.path(model_dir, "trim_batch_service.py")

  if (!nzchar(python_exe) || !file.exists(python_exe)) {
    stop(
      paste0(
        "\n================================================================================\n",
        "[redsan] Valid Python executable not found!\n",
        "================================================================================\n",
        "redsan requires Python with 'onnxruntime' and 'tokenizers' installed to run the\n",
        "DrBERT document trimmer.\n\n",
        "HOW TO FIX:\n",
        "  1. Set the path to Python in your current R session:\n",
        "     Sys.setenv(REDSAN_PYTHON_PATH = \"/path/to/python\")\n\n",
        "  2. Or make it permanent by adding this line to your ~/.Renviron:\n",
        "     REDSAN_PYTHON_PATH=/path/to/python\n\n",
        "  3. In an air-gapped hospital HDW, ask your system administrator for the path\n",
        "     to the shared Python virtual environment.\n",
        "================================================================================\n"
      ),
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
  if (anyNA(ids) || any(!nzchar(ids)) || anyDuplicated(ids)) {
    stop("Document identifiers must be non-empty and unique.", call. = FALSE)
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
  worker_result <- tryCatch(
    .doceds_onnx_run_worker(
      python_exe = python_exe,
      service_script = service_script,
      input_path = tmp_in,
      output_path = tmp_out,
      model_dir = model_dir
    ),
    error = function(e) {
      err_msg <- e$message
      if (!is.null(e$stderr) && nzchar(e$stderr)) {
        err_msg <- paste0(err_msg, "\n--- Worker Stderr ---\n", e$stderr)
      }
      troubleshoot_hint <- ""
      if (!is.null(e$stderr) && grepl("No module named", e$stderr)) {
        troubleshoot_hint <- paste0(
          "\nPOSSIBLE CAUSE: Missing required Python packages.\n",
          "Please verify that 'onnxruntime', 'tokenizers', and 'numpy' are installed in the\n",
          "active Python environment:\n",
          "  pip install onnxruntime tokenizers numpy\n"
        )
      }
      stop(
        paste0(
          "\n================================================================================\n",
          "[redsan] Error running Python ONNX trimmer process:\n",
          "================================================================================\n",
          err_msg, "\n",
          troubleshoot_hint,
          "================================================================================\n"
        ),
        call. = FALSE
      )
    }
  )
  .doceds_onnx_emit_worker_notices(worker_result$stderr)

  output_data <- jsonlite::fromJSON(tmp_out, simplifyVector = FALSE)
  output_ids <- vapply(
    output_data,
    function(item) if (is.null(item$id)) "" else as.character(item$id),
    character(1)
  )
  if (
    length(output_ids) != length(ids) ||
      any(!nzchar(output_ids)) ||
      anyDuplicated(output_ids) ||
      !setequal(output_ids, ids)
  ) {
    stop(
      "Trimmer worker result identifiers do not match the request.",
      call. = FALSE
    )
  }
  output_data <- output_data[match(ids, output_ids)]

  trimmed_texts <- character(nrow(df))
  reduc_pcts <- numeric(nrow(df))
  intervals_list <- vector("list", nrow(df))
  execution_providers <- rep(NA_character_, nrow(df))

  for (i in seq_along(output_data)) {
    item <- output_data[[i]]
    .doceds_onnx_validate_result(item, ids[[i]], payload$text[[i]])
    trimmed_texts[i] <- item$trimmed_text
    reduc_pcts[i] <- as.numeric(item$reduction_pct)
    if (!is.null(item$execution_provider)) {
      execution_providers[i] <- item$execution_provider
    }

    if (length(item$preserved_intervals) > 0) {
      intervals_list[[i]] <- as.character(jsonlite::toJSON(
        item$preserved_intervals,
        auto_unbox = TRUE
      ))
    } else {
      intervals_list[[i]] <- "[]"
    }
  }

  known_providers <- unique(execution_providers[!is.na(execution_providers)])
  if (
    length(known_providers) > 1L ||
      (any(!is.na(execution_providers)) && anyNA(execution_providers))
  ) {
    stop(
      "Trimmer worker returned inconsistent execution provider provenance.",
      call. = FALSE
    )
  }
  execution_provider <- if (length(known_providers) == 1L) {
    known_providers[[1L]]
  } else {
    NA_character_
  }

  if (is_char_input) {
    names(trimmed_texts) <- names(data)
    if (!is.na(execution_provider)) {
      attr(trimmed_texts, "TRIM_EXECUTION_PROVIDER") <- execution_provider
    }
    return(trimmed_texts)
  }

  out_col <- if (col_to_use == "RECTXT") {
    "RECTXT_TRIMMED"
  } else {
    paste0(col_to_use, "_TRIMMED")
  }
  data[[out_col]] <- trimmed_texts
  data$TRIM_REDUCTION_PCT <- reduc_pcts
  data$TRIM_PRESERVED_INTERVALS <- as.character(intervals_list)
  data$TRIM_EXECUTION_PROVIDER <- rep(execution_provider, nrow(data))

  data
}

#' Validate one result returned by the external trimmer worker
#'
#' @noRd
.doceds_onnx_validate_result <- function(item, document_id, source_text) {
  scalar_character <- function(x) {
    is.character(x) && length(x) == 1L && !is.na(x)
  }
  scalar_number <- function(x) {
    is.numeric(x) && length(x) == 1L && is.finite(x)
  }
  valid_interval <- function(interval) {
    if (
      !is.list(interval) ||
        !scalar_number(interval$start) ||
        !scalar_number(interval$end) ||
        !scalar_character(interval$family) ||
        !scalar_character(interval$text)
    ) {
      return(FALSE)
    }
    start <- interval$start
    end <- interval$end
    start == as.integer(start) &&
      end == as.integer(end) &&
      start >= 1L &&
      end >= start &&
      end <= nchar(source_text) &&
    identical(
      enc2utf8(substr(source_text, start, end)),
      enc2utf8(interval$text)
    )
  }
  intervals <- item$preserved_intervals
  intervals_valid <- is.list(intervals) &&
    all(vapply(intervals, valid_interval, logical(1)))
  if (intervals_valid && length(intervals) > 1L) {
    starts <- vapply(intervals, function(interval) interval$start, numeric(1))
    ends <- vapply(intervals, function(interval) interval$end, numeric(1))
    intervals_valid <- all(starts[-1L] > ends[-length(ends)])
  }
  interval_text <- if (intervals_valid && length(intervals) > 0L) {
    paste(
      vapply(intervals, function(interval) interval$text, character(1)),
      collapse = ""
    )
  } else {
    ""
  }
  without_whitespace <- function(text) {
    gsub("[[:space:]]", "", enc2utf8(text))
  }
  trimmed_text_grounded <- scalar_character(item$trimmed_text) &&
    identical(
      without_whitespace(item$trimmed_text),
      without_whitespace(interval_text)
    )
  legacy_fields_absent <- !"is_bt" %in% names(item)
  execution_provider_valid <- is.null(item$execution_provider) ||
    (scalar_character(item$execution_provider) && nzchar(item$execution_provider))
  valid <- is.list(item) &&
    legacy_fields_absent &&
    execution_provider_valid &&
    scalar_character(item$id) &&
    identical(item$id, document_id) &&
    trimmed_text_grounded &&
    scalar_number(item$reduction_pct) &&
    intervals_valid
  if (!valid) {
    stop(
      sprintf(
        "Trimmer worker returned an invalid result for document %s.",
        document_id
      ),
      call. = FALSE
    )
  }
  invisible(item)
}

#' Resolve the text column used by the worker protocol
#'
#' @noRd
.doceds_onnx_text_col <- function(data, text_col = NULL) {
  if (!is.null(text_col)) {
    if (!text_col %in% names(data)) {
      stop(sprintf("Column '%s' not found in data.", text_col), call. = FALSE)
    }
    return(text_col)
  }
  candidates <- c("RECTXT", "text", "raw_text", "content", "document")
  found <- candidates[candidates %in% names(data)]
  if (length(found) == 0L) {
    stop(
      sprintf(
        "Could not auto-detect text column in data. Candidates searched: %s. Please specify 'text_col'.",
        paste(candidates, collapse = ", ")
      ),
      call. = FALSE
    )
  }
  found[[1L]]
}

#' Add the stable worker output schema to an empty DOCEDS table
#'
#' @noRd
.doceds_onnx_add_output_columns <- function(data, text_col) {
  data[[paste0(text_col, "_TRIMMED")]] <- character(0)
  data$TRIM_REDUCTION_PCT <- numeric(0)
  data$TRIM_PRESERVED_INTERVALS <- character(0)
  data$TRIM_EXECUTION_PROVIDER <- character(0)
  data
}

#' Validate the DOCEDS contract carried by an event bundle
#'
#' @noRd
.doceds_onnx_validate_bundle <- function(bundle, text_col = NULL) {
  if (!inherits(bundle, "edsan_event_bundle")) {
    stop("Every cohort element must be an edsan_event_bundle.", call. = FALSE)
  }
  doceds <- bundle$sources$doceds
  if (!is.data.frame(doceds)) {
    stop("Each event bundle must contain a DOCEDS data frame.", call. = FALSE)
  }
  required <- unique(c("RECTXT", "RECTYPE", text_col))
  if (!all(required %in% names(doceds))) {
    stop(
      sprintf(
        "DOCEDS table must contain columns: %s.",
        paste(required, collapse = ", ")
      ),
      call. = FALSE
    )
  }
  invisible(bundle)
}

#' Surface only worker diagnostics using the documented runtime marker
#'
#' @noRd
.doceds_onnx_emit_worker_notices <- function(stderr) {
  if (is.null(stderr) || !nzchar(stderr)) {
    return(invisible(NULL))
  }
  lines <- strsplit(stderr, "\\r?\\n", perl = TRUE)[[1L]]
  pattern <- "^EDSAN_TRIMMER_NOTICE:(INFO|WARNING):(.*)$"
  for (line in lines) {
    match <- regmatches(line, regexec(pattern, line, perl = TRUE))[[1L]]
    if (length(match) == 0L || !nzchar(match[[3L]])) {
      next
    }
    notice <- paste0("[edsan-doc-trimmer] ", match[[3L]])
    if (identical(match[[2L]], "WARNING")) {
      warning(notice, call. = FALSE)
    } else {
      message(notice)
    }
  }
  invisible(NULL)
}

#' Run the external edsan-doc-trimmer worker
#'
#' @noRd
.doceds_onnx_run_worker <- function(
  python_exe,
  service_script,
  input_path,
  output_path,
  model_dir
) {
  processx::run(
    command = python_exe,
    args = c(
      service_script,
      "--input",
      input_path,
      "--output",
      output_path,
      "--onnx_dir",
      model_dir
    ),
    echo_cmd = FALSE,
    error_on_status = TRUE
  )
}

#' Install an edsan-doc-trimmer artifact
#'
#' Extracts a compatible versioned `edsan-doc-trimmer` archive into the trimmer
#' cache. The archive is validated before anything in the cache is changed. It
#' must contain `model.onnx`, `tokenizer.json`, `trim_batch_service.py`, and an
#' `artifact.json` manifest accepted by this version of `redsan`. The current
#' contract requires `artifact_version >= 1.2.0` and
#' `worker_contract = "model-only-v1"`.
#'
#' @details
#' The artifact is installed in `<cache root>/<artifact_version>`, where the
#' cache root is returned by [edsan_trimmer_cache_dir()] and `<artifact_version>`
#' is the manifest's `artifact_version`, verbatim (for example `1.3.0`). Versions
#' coexist: installing 1.3.0 leaves 1.2.0 in place. The destination is not
#' configurable; the folder name is how discovery recognizes an installation.
#'
#' Installing a release makes it the one that runs, provided it is the highest
#' installed version and neither `EDSAN_TRIMMER_PATH` nor `EDSAN_TRIMMER_VERSION`
#' selects something else (see [edsan_trimmer_cache_dir()] for the discovery
#' order). The success message states the version, the path, and whether the
#' installed artifact is now the selected one. [edsan_trimmer_versions()] lists
#' what is installed.
#'
#' Reinstalling an archive whose content digest (see [doceds_onnx_spec()])
#' matches the installed version is a no-op that only reports the situation.
#' An archive with the same version but a different digest is an error unless
#' `overwrite = TRUE`, which replaces that version's folder.
#'
#' Extraction and validation happen in a staging directory inside the cache
#' root. If the archive is invalid, the cache is left unchanged. If publishing
#' an overwriting install fails, `redsan` attempts to restore the previous
#' installation of that version.
#'
#' @param zip_path Path to a compatible versioned `edsan-doc-trimmer` archive.
#' @param overwrite Logical. Replace an installed artifact of the same version
#'   whose content differs. Defaults to `FALSE`.
#'
#' @return The path to the installed artifact directory (invisibly).
#'
#' @examples
#' \dontrun{
#' redsan::edsan_install_trimmer(
#'   "C:/path/to/edsan-doc-trimmer-v1.3.0.zip"
#' )
#'
#' # Versions coexist; list them and see which one is selected.
#' redsan::edsan_trimmer_versions()
#'
#' # Replace an installed version whose archive was rebuilt.
#' redsan::edsan_install_trimmer(
#'   "C:/path/to/edsan-doc-trimmer-v1.3.0.zip",
#'   overwrite = TRUE
#' )
#' }
#'
#' @seealso [edsan_trimmer_cache_dir()], [edsan_trimmer_versions()],
#'   [trim_doceds_onnx()], [doceds_onnx_spec()]
#' @export
edsan_install_trimmer <- function(zip_path, overwrite = FALSE) {
  if (missing(zip_path) || !is.character(zip_path) || !nzchar(zip_path)) {
    stop(
      paste0(
        "\n================================================================================\n",
        "[redsan] Please provide the path to a compatible edsan-doc-trimmer archive.\n",
        "================================================================================\n",
        "Usage:\n",
        "  edsan_install_trimmer(\"path/to/edsan-doc-trimmer-runtime.zip\")\n\n",
        "Where to get the compatible archive:\n",
        "  1. Hospital internal GitLab: Project Overview or Deploy > Releases\n",
        "  2. Shared HDW storage (e.g. /data/shared/models/edsan-doc-trimmer/)\n",
        "  3. Workstation with internet access via direct download, then transfer via USB/SFTP.\n",
        "================================================================================\n"
      ),
      call. = FALSE
    )
  }
  if (!isTRUE(overwrite) && !isFALSE(overwrite)) {
    stop("`overwrite` must be TRUE or FALSE.", call. = FALSE)
  }

  if (dir.exists(zip_path)) {
    stop(
      paste0(
        "\n================================================================================\n",
        "[redsan] Specified path is a directory, not a zip archive:\n",
        sprintf("  %s\n", zip_path),
        "================================================================================\n",
        "If the trimmer model is already extracted in this directory, you do not need to install it.\n",
        "Simply point redsan to it:\n",
        sprintf("  Sys.setenv(EDSAN_TRIMMER_PATH = \"%s\")\n\n", normalizePath(zip_path)),
        "Or make it permanent by adding this line to ~/.Renviron:\n",
        sprintf("  EDSAN_TRIMMER_PATH=%s\n\n", normalizePath(zip_path)),
        "To install into user cache, provide a compatible versioned archive.\n",
        "================================================================================\n"
      ),
      call. = FALSE
    )
  }

  if (!file.exists(zip_path)) {
    stop(
      sprintf(
        paste0(
          "\n================================================================================\n",
          "[redsan] Model zip file not found at: %s\n",
          "================================================================================\n",
          "Please verify the file path.\n",
          "If you have not downloaded it yet:\n",
          "  1. Download a compatible versioned edsan-doc-trimmer archive from your\n",
          "     hospital's internal GitLab under Deploy > Releases.\n",
          "  2. Or ask your HDW platform administrator for the shared model archive.\n",
          "================================================================================\n"
        ),
        zip_path
      ),
      call. = FALSE
    )
  }

  cache_root <- normalizePath(edsan_trimmer_cache_dir(), mustWork = FALSE)
  if (file.exists(cache_root) && !dir.exists(cache_root)) {
    stop("Trimmer cache root exists and is not a directory.", call. = FALSE)
  }
  dir.create(cache_root, recursive = TRUE, showWarnings = FALSE)
  staging_dir <- tempfile("trimmer-stage-", tmpdir = cache_root)
  dir.create(staging_dir)
  on.exit(unlink(staging_dir, recursive = TRUE), add = TRUE)

  utils::unzip(zip_path, exdir = staging_dir)
  artifact_dir <- .doceds_onnx_artifact_root(staging_dir)
  if (is.null(artifact_dir)) {
    stop(
      paste0(
        "\n================================================================================\n",
        "[redsan] Invalid trimmer archive: 'model.onnx' not found after extraction!\n",
        "================================================================================\n",
        "The archive does not contain a runnable model artifact.\n",
        "================================================================================\n"
      ),
      call. = FALSE
    )
  }
  version <- .doceds_onnx_validate_artifact(artifact_dir)
  # Only installs need a folder-safe version: the name is how discovery finds
  # them. Artifacts named by model_dir or EDSAN_TRIMMER_PATH never need one.
  if (!grepl(.DOCEDS_ONNX_VERSION_PATTERN, version)) {
    stop(
      sprintf(
        paste0(
          "Invalid trimmer archive; artifact_version '%s' must be dotted ",
          "numbers such as 1.3.0, because it names the install folder."
        ),
        version
      ),
      call. = FALSE
    )
  }
  dest_dir <- file.path(cache_root, version)
  if (file.exists(dest_dir) && !dir.exists(dest_dir)) {
    stop("Trimmer destination exists and is not a directory.", call. = FALSE)
  }

  # A folder that fails validation is not an installation (discovery skips
  # it), so replacing it needs no overwrite and is reported as a repair.
  existing_valid <- dir.exists(dest_dir) && !is.null(tryCatch(
    .doceds_onnx_validate_artifact(dest_dir),
    error = function(e) NULL
  ))
  if (dir.exists(dest_dir) && !existing_valid) {
    message(
      sprintf(
        "[redsan] %s exists but is not a valid installation; replacing it.",
        normalizePath(dest_dir, mustWork = FALSE)
      )
    )
  }

  if (existing_valid) {
    if (.doceds_onnx_same_artifact(artifact_dir, dest_dir)) {
      message(
        sprintf(
          "[redsan] edsan-doc-trimmer %s is already installed with identical content; nothing changed.\n",
          version
        ),
        sprintf("Location: %s\n", normalizePath(dest_dir, mustWork = FALSE)),
        .doceds_onnx_selection_note(dest_dir)
      )
      return(invisible(dest_dir))
    }
    if (!isTRUE(overwrite)) {
      stop(
        sprintf(
          paste0(
            "edsan-doc-trimmer %s is already installed at %s with different content.\n",
            "Use `overwrite = TRUE` to replace it, or install an archive with a new artifact_version."
          ),
          version,
          normalizePath(dest_dir, mustWork = FALSE)
        ),
        call. = FALSE
      )
    }
  }

  backup_dir <- NULL
  published <- FALSE
  on.exit({
    if (!published && !is.null(backup_dir) && dir.exists(backup_dir)) {
      if (dir.exists(dest_dir)) {
        unlink(dest_dir, recursive = TRUE)
      }
      file.rename(backup_dir, dest_dir)
    }
  }, add = TRUE)

  if (dir.exists(dest_dir)) {
    backup_dir <- tempfile("trimmer-backup-", tmpdir = cache_root)
    if (!file.rename(dest_dir, backup_dir)) {
      stop("Could not preserve the existing trimmer installation.", call. = FALSE)
    }
  }
  if (!.doceds_onnx_publish_artifact(artifact_dir, dest_dir)) {
    stop("Could not publish the validated trimmer artifact.", call. = FALSE)
  }
  published <- TRUE
  if (!is.null(backup_dir) && dir.exists(backup_dir)) {
    unlink(backup_dir, recursive = TRUE)
  }

  installed_norm <- normalizePath(dest_dir, mustWork = FALSE)

  message(
    paste0(
      "\n================================================================================\n",
      "[redsan] edsan-doc-trimmer model successfully installed!\n",
      "================================================================================\n",
      sprintf("Version:  %s\n", version),
      sprintf("Location: %s\n", installed_norm),
      .doceds_onnx_selection_note(dest_dir),
      "\n",
      "How to verify it works:\n",
      "  library(redsan)\n",
      "  # Quick smoke test:\n",
      "  trim_doceds_onnx(\n",
      "    \"Consultation du 12/03/2024. Patient vu pour fievre.\\nEmail: pierre.dupont@gmail.com\"\n",
      "  )\n",
      "  # Expected:\n",
      "  # \"Consultation du 12/03/2024. Patient vu pour fievre.\"\n",
      "  # Or trim an entire cohort bundle:\n",
      "  clean_bundle <- trim_doceds_onnx(bundle)\n\n",
      "Air-gapped hospital HDW note:\n",
      "  Runtime discovery and execution use only this local artifact.\n",
      "================================================================================\n"
    )
  )

  invisible(dest_dir)
}

#' Say whether an installed artifact is the one discovery selects
#'
#' @noRd
.doceds_onnx_selection_note <- function(installed_dir) {
  selected <- tryCatch(
    suppressWarnings(.edsan_resolve_trimmer(quiet = TRUE)),
    error = function(e) e
  )
  installed <- normalizePath(installed_dir, mustWork = FALSE)
  if (inherits(selected, "error")) {
    # Name the cause, e.g. a stale EDSAN_TRIMMER_PATH or an unknown pin.
    reason <- strsplit(conditionMessage(selected), "\n", fixed = TRUE)[[1L]][[1L]]
    return(sprintf("Selected:  no, discovery fails: %s\n", reason))
  }
  if (identical(selected$path, installed)) {
    return("Selected:  yes, this is the artifact trim_doceds_onnx() will use.\n")
  }
  sprintf(
    "Selected:  no, %s is selected instead (%s).\n",
    if (is.na(selected$version)) selected$path else selected$version,
    selected$source
  )
}

#' TRUE when two artifact directories carry identical content digests
#'
#' @noRd
.doceds_onnx_same_artifact <- function(new_dir, installed_dir) {
  required <- file.path(installed_dir, .DOCEDS_ONNX_ARTIFACT_FILES)
  if (!all(file.exists(required))) {
    return(FALSE)
  }
  identical(
    .doceds_onnx_artifact_digest(new_dir),
    .doceds_onnx_artifact_digest(installed_dir)
  )
}

#' Print which artifact is about to run
#'
#' @noRd
.doceds_onnx_announce_artifact <- function(version, model_dir) {
  message(
    sprintf(
      "edsan-doc-trimmer %s (%s)",
      version,
      normalizePath(model_dir, mustWork = FALSE)
    )
  )
}

#' Publish a validated artifact from staging
#'
#' Kept behind a package-local seam so rollback behavior can be tested without
#' relying on platform-specific filesystem permissions.
#'
#' @noRd
.doceds_onnx_publish_artifact <- function(artifact_dir, dest_dir) {
  file.rename(artifact_dir, dest_dir)
}

#' Find the model root inside an extracted trimmer archive
#'
#' @noRd
.doceds_onnx_artifact_root <- function(staging_dir) {
  candidates <- unique(c(staging_dir, list.dirs(staging_dir, recursive = TRUE)))
  candidates <- candidates[file.exists(file.path(candidates, "model.onnx"))]
  if (length(candidates) != 1L) {
    return(NULL)
  }
  candidates[[1L]]
}

#' Validate a versioned edsan-doc-trimmer runtime artifact
#'
#' Returns the manifest's `artifact_version` invisibly.
#'
#' @noRd
.doceds_onnx_validate_artifact <- function(artifact_dir) {
  required <- c(
    "model.onnx",
    "tokenizer.json",
    "trim_batch_service.py",
    "artifact.json"
  )
  missing <- required[!file.exists(file.path(artifact_dir, required))]
  if (length(missing) > 0L) {
    stop(
      sprintf(
        "Invalid trimmer archive; missing required files: %s.",
        paste(missing, collapse = ", ")
      ),
      call. = FALSE
    )
  }
  manifest <- tryCatch(
    jsonlite::fromJSON(file.path(artifact_dir, "artifact.json")),
    error = function(e) NULL
  )
  if (!is.list(manifest)) {
    manifest <- NULL
  }
  version <- if (is.null(manifest)) NULL else manifest$artifact_version
  worker_contract <- if (is.null(manifest)) NULL else manifest$worker_contract
  parsed_version <- tryCatch(
    numeric_version(version),
    error = function(e) NULL
  )
  if (
    is.null(manifest) ||
      !is.character(version) ||
      length(version) != 1L ||
      !nzchar(version) ||
      is.null(parsed_version) ||
      parsed_version < numeric_version("1.2.0") ||
      !identical(worker_contract, "model-only-v1")
  ) {
    stop(
      paste0(
        "Invalid trimmer archive; redsan requires artifact_version >= 1.2.0 ",
        "and worker_contract 'model-only-v1'."
      ),
      call. = FALSE
    )
  }
  invisible(version)
}

# The four files the archive is required to carry are exactly the four that
# decide what comes back: the weights, the tokenizer that feeds them, the worker
# that routes a document around them, and the manifest that names the contract.
# Digesting the artifact means digesting these, not the manifest alone - a
# manifest is maintained by hand and can stay put while the model moves.
.DOCEDS_ONNX_ARTIFACT_FILES <- c(
  "model.onnx",
  "tokenizer.json",
  "trim_batch_service.py",
  "artifact.json"
)

# `model.onnx` is 442 MB, and a caller that builds one catalog per stay would
# hash it once per stay. Cache on the artifact's own identity - path, sizes and
# modification times - so installing a different archive produces a different
# key rather than a stale answer.
.doceds_onnx_digest_cache <- new.env(parent = emptyenv())

#' @noRd
.doceds_onnx_artifact_digest <- function(artifact_dir) {
  paths <- file.path(artifact_dir, .DOCEDS_ONNX_ARTIFACT_FILES)
  info <- file.info(paths)
  key <- paste(
    normalizePath(artifact_dir, winslash = "/", mustWork = FALSE),
    paste(info$size, as.numeric(info$mtime), sep = "@", collapse = ";"),
    sep = "|"
  )
  cached <- .doceds_onnx_digest_cache[[key]]
  if (!is.null(cached)) {
    return(cached)
  }
  parts <- vapply(
    paths,
    function(path) digest::digest(file = path, algo = "sha256"),
    character(1),
    USE.NAMES = FALSE
  )
  value <- digest::digest(
    charToRaw(enc2utf8(paste(
      .DOCEDS_ONNX_ARTIFACT_FILES,
      parts,
      sep = "=",
      collapse = "\n"
    ))),
    algo = "sha256",
    serialize = FALSE
  )
  .doceds_onnx_digest_cache[[key]] <- value
  value
}

#' Which trimmer artifact produced a DrBERT-trimmed text
#'
#' Identifies the runtime artifact used by [trim_doceds_onnx()].
#' A caller can record this specification alongside trimmed text to identify
#' what produced it.
#'
#' `digest` is the field to compare between two runs. It is derived from the
#' weights, the tokenizer, the worker script and the manifest themselves, so an
#' artifact that changed changed it whether or not anybody edited
#' `artifact_version`. The version and the contract are reported beside it
#' because they are what a human reads, not because they can be trusted to move.
#'
#' @param model_dir Path to an installed runtime artifact. Defaults to the
#'   artifact [trim_doceds_onnx()] would use (see [edsan_trimmer_cache_dir()]).
#'
#' @return A list describing the artifact: `package`, `version`, `path` (the
#'   normalized artifact directory that was inspected), `digest`,
#'   `digest_algorithm`, `digest_schema`, and the manifest's `artifact_name`,
#'   `artifact_version`, `worker_contract` and `model_type`.
#' @seealso [trim_doceds_onnx()]
#' @export
doceds_onnx_spec <- function(model_dir = NULL) {
  if (is.null(model_dir)) {
    model_dir <- .edsan_get_trimmer_dir()
  }
  .doceds_onnx_validate_artifact(model_dir)
  manifest <- jsonlite::fromJSON(file.path(model_dir, "artifact.json"))
  field <- function(name) {
    value <- manifest[[name]]
    if (is.null(value) || !is.character(value) || length(value) != 1L) {
      NA_character_
    } else {
      value
    }
  }
  list(
    package = "redsan",
    version = as.character(utils::packageVersion("redsan")),
    path = normalizePath(model_dir, mustWork = FALSE),
    digest = .doceds_onnx_artifact_digest(model_dir),
    digest_algorithm = "sha256",
    digest_schema = "doceds-onnx-artifact-v1",
    artifact_name = field("artifact_name"),
    artifact_version = field("artifact_version"),
    worker_contract = field("worker_contract"),
    model_type = field("model_type")
  )
}

#' Locate the edsan-doc-trimmer cache
#'
#' Returns the root of the user-cache directory where [edsan_install_trimmer()]
#' installs trimmer artifacts. This function reports the path; it does not
#' create the directory or inspect what is installed. Use
#' [edsan_trimmer_versions()] to list installations.
#'
#' @section Cache layout:
#' The root is `tools::R_user_dir("edsan_doc_trimmer", "cache")`, which honors
#' `R_USER_CACHE_DIR` on every platform (for example, the root is
#' `<R_USER_CACHE_DIR>/R/edsan_doc_trimmer`). Each installation is a direct
#' child folder named exactly after the `artifact_version` of the
#' `artifact.json` manifest it contains:
#'
#' ```
#' <root>/
#'   1.2.0/   model.onnx  tokenizer.json  trim_batch_service.py  artifact.json
#'   1.3.0/   model.onnx  tokenizer.json  trim_batch_service.py  artifact.json
#' ```
#'
#' Versions coexist. A folder is an installation only if its name is a dotted
#' numeric version (`1.3.0`), it passes artifact validation, and its manifest's
#' `artifact_version` equals its name. Anything else (for example `v1.3`, or a
#' folder whose manifest disagrees with its name) is ignored. Staging folders
#' used during installation are also ignored.
#'
#' @section Discovery order:
#' When `model_dir` is not supplied to [trim_doceds_onnx()] or
#' [doceds_onnx_spec()], `redsan` selects one artifact, in this order:
#'
#' 1. The directory named by `EDSAN_TRIMMER_PATH` (or a direct path to its
#'    `model.onnx`). The legacy `REDSAN_TRIMMER_PATH` variable is used only when
#'    `EDSAN_TRIMMER_PATH` is unset. This is the only way to select an artifact
#'    outside the cache, such as a development checkout's export. A path that
#'    does not contain `model.onnx` is an error; it never falls through.
#' 2. The installed version named by `EDSAN_TRIMMER_VERSION`. An unknown
#'    version is an error that lists the installed versions.
#' 3. The highest valid installed version (compared as a numeric version).
#' 4. The legacy single-slot install `<root>/v1`, used with a message asking you
#'    to reinstall into the versioned layout.
#' 5. Otherwise an error with installation instructions.
#'
#' There is no automatic download and no implicit fallback to a development
#' checkout. Every [trim_doceds_onnx()] call that runs the worker prints the
#' version and path it used, and [doceds_onnx_spec()] reports them.
#'
#' @return A character scalar containing the cache root path (with
#'   platform-specific separators).
#'
#' @examples
#' edsan_trimmer_cache_dir()
#'
#' @seealso [edsan_install_trimmer()], [edsan_trimmer_versions()],
#'   [trim_doceds_onnx()], [doceds_onnx_spec()]
#' @export
edsan_trimmer_cache_dir <- function() {
  tools::R_user_dir("edsan_doc_trimmer", "cache")
}

#' List installed edsan-doc-trimmer versions
#'
#' Lists the valid artifacts installed in the trimmer cache (see
#' [edsan_trimmer_cache_dir()] for what counts as an installation) and marks the
#' one that [trim_doceds_onnx()] would use.
#'
#' `selected` follows the full discovery order, including
#' `EDSAN_TRIMMER_PATH` and `EDSAN_TRIMMER_VERSION`. When discovery selects an
#' artifact outside the cache, or falls back to the legacy `v1` slot, no row is
#' selected. An unresolvable selection never raises an error here.
#'
#' @return A data frame with one row per installed version, highest first, and
#'   columns `version` (character), `path` (character), and `selected`
#'   (logical). It has zero rows when nothing is installed.
#'
#' @examples
#' edsan_trimmer_versions()
#'
#' @seealso [edsan_install_trimmer()], [edsan_trimmer_cache_dir()],
#'   [doceds_onnx_spec()]
#' @export
edsan_trimmer_versions <- function() {
  installed <- .doceds_onnx_installed_versions()
  selected <- tryCatch(
    suppressWarnings(.edsan_resolve_trimmer(quiet = TRUE)$path),
    error = function(e) NA_character_
  )
  installed$selected <- installed$path %in% selected
  installed
}

# An installation folder is named after a dotted numeric artifact version.
.DOCEDS_ONNX_VERSION_PATTERN <- "^[0-9]+(\\.[0-9]+)*$"

#' Valid installations in the versioned cache, highest version first
#'
#' A folder counts only if its name is a dotted numeric version, it passes
#' artifact validation, and its manifest version equals its name.
#'
#' @noRd
.doceds_onnx_installed_versions <- function(root = edsan_trimmer_cache_dir()) {
  found <- if (dir.exists(root)) {
    list.dirs(root, recursive = FALSE, full.names = FALSE)
  } else {
    character()
  }
  found <- found[grepl(.DOCEDS_ONNX_VERSION_PATTERN, found)]
  valid <- vapply(
    found,
    function(name) {
      version <- tryCatch(
        .doceds_onnx_validate_artifact(file.path(root, name)),
        error = function(e) NA_character_
      )
      identical(version, name)
    },
    logical(1),
    USE.NAMES = FALSE
  )
  versions <- found[valid]
  versions <- versions[order(numeric_version(versions), decreasing = TRUE)]
  data.frame(
    version = versions,
    path = vapply(
      file.path(root, versions),
      normalizePath,
      character(1),
      mustWork = FALSE,
      USE.NAMES = FALSE
    ),
    stringsAsFactors = FALSE
  )
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
      "bin",
      "python"
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
    file.path("..", "edsan-doc-trimmer", ".venv", "bin", "python"),
    file.path(".venv", "bin", "python"),
    file.path(".venv", "Scripts", "python.exe"),
    file.path("venv", "bin", "python"),
    file.path("venv", "Scripts", "python.exe"),
    Sys.which("python3"),
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
#' Applies the discovery order documented in [edsan_trimmer_cache_dir()] and
#' returns the selected artifact's path only.
#'
#' @noRd
.edsan_get_trimmer_dir <- function() {
  .edsan_resolve_trimmer()$path
}

#' Select a trimmer artifact
#'
#' Order: `EDSAN_TRIMMER_PATH` (legacy `REDSAN_TRIMMER_PATH`), the
#' `EDSAN_TRIMMER_VERSION` pin, the highest valid installed version, the legacy
#' `<root>/v1` slot, then an error. Returns the normalized `path`, the manifest
#' `version` (`NA` when the manifest cannot be read), and a human-readable
#' `source`. `quiet` suppresses the legacy-slot message for callers that only
#' inspect the selection.
#'
#' @noRd
.edsan_resolve_trimmer <- function(quiet = FALSE) {
  manifest_version <- function(path) {
    version <- tryCatch(
      jsonlite::fromJSON(file.path(path, "artifact.json"))$artifact_version,
      error = function(e) NULL
    )
    if (is.character(version) && length(version) == 1L) version else NA_character_
  }

  # 1. Environment variable override
  env_name <- "EDSAN_TRIMMER_PATH"
  env_path <- Sys.getenv(env_name, "")
  if (!nzchar(env_path)) {
    env_name <- "REDSAN_TRIMMER_PATH"
    env_path <- Sys.getenv(env_name, "")
  }
  if (nzchar(env_path)) {
    # If pointed directly to model.onnx file, take parent folder
    if (file.exists(env_path) && !dir.exists(env_path) && tolower(basename(env_path)) == "model.onnx") {
      env_path <- dirname(env_path)
    }
    # An explicit override that points nowhere is an error: falling through
    # would silently run a different artifact than the one the user named.
    if (!file.exists(file.path(env_path, "model.onnx"))) {
      stop(
        sprintf(
          paste0(
            "%s is set to '%s', but 'model.onnx' was not found in that folder.\n",
            "Fix the path, or unset %s to use the installed trimmer versions."
          ),
          env_name,
          env_path,
          env_name
        ),
        call. = FALSE
      )
    }
    path <- normalizePath(env_path)
    return(list(
      path = path,
      version = manifest_version(path),
      source = env_name
    ))
  }

  cache_dir <- edsan_trimmer_cache_dir()
  installed <- .doceds_onnx_installed_versions(cache_dir)

  # 2. Explicit version pin
  pin <- trimws(Sys.getenv("EDSAN_TRIMMER_VERSION", ""))
  if (nzchar(pin)) {
    row <- match(pin, installed$version)
    if (is.na(row)) {
      stop(
        sprintf(
          paste0(
            "EDSAN_TRIMMER_VERSION is '%s', but that version is not installed in %s.\n",
            "Installed versions: %s.\n",
            "Install it with edsan_install_trimmer(), or unset EDSAN_TRIMMER_VERSION."
          ),
          pin,
          cache_dir,
          if (nrow(installed) > 0L) paste(installed$version, collapse = ", ") else "none"
        ),
        call. = FALSE
      )
    }
    return(list(
      path = installed$path[[row]],
      version = installed$version[[row]],
      source = "EDSAN_TRIMMER_VERSION"
    ))
  }

  # 3. Highest valid installed version
  if (nrow(installed) > 0L) {
    return(list(
      path = installed$path[[1L]],
      version = installed$version[[1L]],
      source = "highest installed version"
    ))
  }

  # 4. Legacy single-slot install
  legacy <- file.path(cache_dir, "v1")
  legacy_version <- tryCatch(
    .doceds_onnx_validate_artifact(legacy),
    error = function(e) NULL
  )
  if (!is.null(legacy_version)) {
    path <- normalizePath(legacy)
    if (!isTRUE(quiet)) {
      message(
        sprintf(
          paste0(
            "[redsan] Using the legacy trimmer install at %s (artifact_version %s).\n",
            "Reinstall the archive with edsan_install_trimmer() to move it into the versioned cache."
          ),
          path,
          legacy_version
        )
      )
    }
    return(list(path = path, version = legacy_version, source = "legacy v1 slot"))
  }

  # 5. Nothing usable: local-only instructions for air-gapped HDW environments
  stop(
    paste0(
      "\n================================================================================\n",
      "[redsan] edsan-doc-trimmer model not found!\n",
      "================================================================================\n",
      "No valid installed artifact was found in the cache or named by an environment variable.\n\n",
      "HOW TO FIX:\n\n",
      "Option 1: Install model via zip file (Recommended for individual users)\n",
      "  1. Obtain a compatible versioned edsan-doc-trimmer archive from:\n",
      "     - Hospital internal GitLab: Project Overview or Deploy > Releases\n",
      "     - Or copy from your internet-connected workstation / shared HDW drive.\n",
      "  2. In R, run:\n",
      "     redsan::edsan_install_trimmer(\"path/to/edsan-doc-trimmer-runtime.zip\")\n\n",
      "Option 2: Point to a shared HDW directory (Recommended for multi-user servers)\n",
      "  If an admin has placed the model in a shared server directory:\n",
      "  1. Set the environment variable in your session:\n",
      "     Sys.setenv(EDSAN_TRIMMER_PATH = \"/data/shared/models/edsan-doc-trimmer/current\")\n",
      "  2. Or make it permanent by adding this line to your ~/.Renviron:\n",
      "     EDSAN_TRIMMER_PATH=/data/shared/models/edsan-doc-trimmer/current\n\n",
      "Cache directory checked:\n",
      "  ", cache_dir, "\n",
      "================================================================================\n"
    ),
    call. = FALSE
  )
}
