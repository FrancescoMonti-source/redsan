#' Trim DOCEDS documents with a versioned external worker
#'
#' Sends DOCEDS text to a compatible `edsan-doc-trimmer` runtime artifact and
#' maps its results back to the original warehouse objects. Worker results must
#' preserve request identity and provide intervals grounded exactly in the
#' original text.
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
#'   automatically resolved via `.edsan_get_trimmer_dir()`.
#'
#' @return Depending on the input shape:
#'   \itemize{
#'     \item **Character vector**: Returns a character vector of trimmed texts.
#'     \item **Data frame / tibble**: Returns the input table augmented with `<text_col>_TRIMMED`
#'       (defaults to `RECTXT_TRIMMED`), `TRIM_REDUCTION_PCT`, `TRIM_IS_BT`, and
#'       `TRIM_PRESERVED_INTERVALS` (character JSON string adhering to warehouse list-column contracts).
#'     \item **Single bundle (`edsan_event_bundle`)**: Returns the bundle with its `sources$doceds`
#'       table augmented.
#'     \item **List of bundles**: Evaluates all document texts in a single batch
#'       and returns the list of bundles with each `sources$doceds` augmented, leaving all original
#'       schema structures and attributes intact.
#'   }
#'
#' The runtime artifact owns model selection and inference policy. `redsan`
#' validates the artifact and response protocol, but does not reproduce those
#' policies in R.
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
    all_rectypes <- character()

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
        all_rectypes <- c(
          all_rectypes,
          ifelse(is.na(doc$RECTYPE), "", as.character(doc$RECTYPE))
        )
      }
    }

    if (length(all_texts) == 0L) {
      return(data)
    }

    # Delegate the batched texts to trim_doceds_onnx on a simple standard dataframe
    flat_df <- data.frame(
      doc_id = paste0("doc_", seq_along(all_texts)),
      RECTXT = all_texts,
      RECTYPE = all_rectypes,
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
        data[[i]]$sources$doceds$TRIM_IS_BT <- trimmed_flat$TRIM_IS_BT[indices]
        data[[
          i
        ]]$sources$doceds$TRIM_PRESERVED_INTERVALS <- trimmed_flat$TRIM_PRESERVED_INTERVALS[
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
  .doceds_onnx_validate_artifact(model_dir)

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
    rectype = if ("RECTYPE" %in% names(df)) {
      ifelse(is.na(df$RECTYPE), "", as.character(df$RECTYPE))
    } else {
      rep("", nrow(df))
    },
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
  tryCatch(
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
  is_bts <- logical(nrow(df))
  intervals_list <- vector("list", nrow(df))

  for (i in seq_along(output_data)) {
    item <- output_data[[i]]
    .doceds_onnx_validate_result(item, ids[[i]], payload$text[[i]])
    trimmed_texts[i] <- item$trimmed_text
    reduc_pcts[i] <- as.numeric(item$reduction_pct)
    is_bts[i] <- item$is_bt

    if (length(item$preserved_intervals) > 0) {
      intervals_list[[i]] <- as.character(jsonlite::toJSON(
        item$preserved_intervals,
        auto_unbox = TRUE
      ))
    } else {
      intervals_list[[i]] <- "[]"
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
  data$TRIM_PRESERVED_INTERVALS <- as.character(intervals_list)

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
  scalar_logical <- function(x) {
    is.logical(x) && length(x) == 1L && !is.na(x)
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
      identical(substr(source_text, start, end), interval$text)
  }
  intervals <- item$preserved_intervals
  intervals_valid <- is.list(intervals) &&
    all(vapply(intervals, valid_interval, logical(1)))
  if (intervals_valid && length(intervals) > 1L) {
    starts <- vapply(intervals, function(interval) interval$start, numeric(1))
    ends <- vapply(intervals, function(interval) interval$end, numeric(1))
    intervals_valid <- all(starts[-1L] > ends[-length(ends)])
  }
  valid <- is.list(item) &&
    scalar_character(item$id) &&
    identical(item$id, document_id) &&
    scalar_character(item$trimmed_text) &&
    scalar_number(item$reduction_pct) &&
    scalar_logical(item$is_bt) &&
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
  data$TRIM_IS_BT <- logical(0)
  data$TRIM_PRESERVED_INTERVALS <- character(0)
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

#' Install edsan-doc-trimmer Model for Air-Gapped HDW Environments
#'
#' Extracts and registers a compatible versioned `edsan-doc-trimmer` archive into
#' the persistent user cache directory or a custom destination directory. The
#' archive is validated in staging and must include the model, tokenizer, worker,
#' and an `artifact.json` manifest declaring `artifact_version`.
#'
#' @param zip_path Path to a compatible versioned `edsan-doc-trimmer` archive.
#' @param dest_dir Destination directory. Defaults to `edsan_trimmer_cache_dir()`.
#'
#' @return The path to the installed model directory (invisibly).
#' @export
edsan_install_trimmer <- function(
  zip_path,
  dest_dir = edsan_trimmer_cache_dir()
) {
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

  if (file.exists(dest_dir) && !dir.exists(dest_dir)) {
    stop("Trimmer destination exists and is not a directory.", call. = FALSE)
  }
  dest_dir <- normalizePath(dest_dir, mustWork = FALSE)
  parent_dir <- dirname(dest_dir)
  dir.create(parent_dir, recursive = TRUE, showWarnings = FALSE)
  staging_dir <- tempfile("trimmer-stage-", tmpdir = parent_dir)
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
  .doceds_onnx_validate_artifact(artifact_dir)

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
    backup_dir <- tempfile("trimmer-backup-", tmpdir = parent_dir)
    if (!file.rename(dest_dir, backup_dir)) {
      stop("Could not preserve the existing trimmer installation.", call. = FALSE)
    }
  }
  if (!file.rename(artifact_dir, dest_dir)) {
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
      sprintf("Location: %s\n\n", installed_norm),
      "How to verify it works:\n",
      "  library(redsan)\n",
      "  # Quick test on a single text:\n",
      "  clean_text <- trim_doceds_onnx(\"Consultation du 12/03/2024. Patient vu pour fievre.\")\n",
      "  # Or trim an entire cohort bundle:\n",
      "  clean_bundle <- trim_doceds_onnx(bundle)\n\n",
      "Air-gapped hospital HDW note:\n",
      "  Runtime discovery and execution use only this local artifact.\n",
      "================================================================================\n"
    )
  )

  invisible(dest_dir)
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
      parsed_version < numeric_version("1.1.0") ||
      !identical(worker_contract, "rectype-aware-v1")
  ) {
    stop(
      paste0(
        "Invalid trimmer archive; redsan requires artifact_version >= 1.1.0 ",
        "and worker_contract 'rectype-aware-v1'."
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
#' The counterpart of [doceds_trim_spec()] for [trim_doceds_onnx()]. A caller
#' that records a trimmed text needs to be able to say afterwards what produced
#' it, and the two trimmers answer that question from different material: the
#' heuristic one from the text of its own rules, this one from the runtime
#' artifact it hands the documents to.
#'
#' `digest` is the field to compare between two runs. It is derived from the
#' weights, the tokenizer, the worker script and the manifest themselves, so an
#' artifact that changed changed it whether or not anybody edited
#' `artifact_version`. The version and the contract are reported beside it
#' because they are what a human reads, not because they can be trusted to move.
#'
#' @param model_dir Path to an installed runtime artifact. Defaults to the
#'   artifact [trim_doceds_onnx()] would use.
#'
#' @return A list describing the artifact: `package`, `version`, `digest`,
#'   `digest_algorithm`, `digest_schema`, and the manifest's `artifact_name`,
#'   `artifact_version`, `worker_contract` and `model_type`.
#' @seealso [doceds_trim_spec()], [trim_doceds_onnx()]
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
    digest = .doceds_onnx_artifact_digest(model_dir),
    digest_algorithm = "sha256",
    digest_schema = "doceds-onnx-artifact-v1",
    artifact_name = field("artifact_name"),
    artifact_version = field("artifact_version"),
    worker_contract = field("worker_contract"),
    model_type = field("model_type")
  )
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
#' Locates the trimmer model through:
#' 1. `Sys.getenv("EDSAN_TRIMMER_PATH")` or `Sys.getenv("REDSAN_TRIMMER_PATH")`
#' 2. Local user cache directory (`edsan_trimmer_cache_dir()`)
#' 3. Common repository / development relative paths
#' 4. Automatic download from GitHub Releases (when online)
#'
#' @noRd
.edsan_get_trimmer_dir <- function() {
  # 1. Environment variable override
  env_path <- Sys.getenv("EDSAN_TRIMMER_PATH", "")
  if (!nzchar(env_path)) {
    env_path <- Sys.getenv("REDSAN_TRIMMER_PATH", "")
  }
  if (nzchar(env_path)) {
    # If pointed directly to model.onnx file, take parent folder
    if (file.exists(env_path) && !dir.exists(env_path) && tolower(basename(env_path)) == "model.onnx") {
      env_path <- dirname(env_path)
    }
    if (file.exists(file.path(env_path, "model.onnx"))) {
      return(normalizePath(env_path))
    } else {
      warning(
        sprintf(
          "EDSAN_TRIMMER_PATH is set to '%s', but 'model.onnx' was not found in that folder.",
          env_path
        ),
        call. = FALSE
      )
    }
  }


  # 2. Check local user cache directory
  cache_dir <- edsan_trimmer_cache_dir()
  if (file.exists(file.path(cache_dir, "model.onnx"))) {
    return(normalizePath(cache_dir))
  }

  # 3. Check common development / repo paths
  repo_candidates <- c(
    file.path("..", "edsan-doc-trimmer", "artifacts", "active_learning", "onnx_export"),
    file.path(".", "artifacts", "active_learning", "onnx_export"),
    file.path(
      Sys.getenv("USERPROFILE"),
      "Documents",
      "Git",
      "edsan-doc-trimmer",
      "artifacts",
      "active_learning",
      "onnx_export"
    ),
    file.path(
      Sys.getenv("HOME"),
      "Documents",
      "Git",
      "edsan-doc-trimmer",
      "artifacts",
      "active_learning",
      "onnx_export"
    )
  )
  for (cand in repo_candidates) {
    if (nzchar(cand) && file.exists(file.path(cand, "model.onnx"))) {
      return(normalizePath(cand))
    }
  }

  # 4. Local-only fallback for air-gapped HDW environments
  stop(
    paste0(
      "\n================================================================================\n",
      "[redsan] edsan-doc-trimmer model not found!\n",
      "================================================================================\n",
      "The DrBERT ONNX model files were not found in the cache or environment path.\n\n",
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
