trimmer_fixture <- function(result_mutation = character()) {
  model_dir <- tempfile("trimmer_model_")
  dir.create(model_dir)
  writeLines("dummy model", file.path(model_dir, "model.onnx"))
  writeLines("{}", file.path(model_dir, "tokenizer.json"))
  writeLines(
    '{"artifact_version":"1.1.0","worker_contract":"rectype-aware-v1"}',
    file.path(model_dir, "artifact.json")
  )

  worker <- c(
    "args <- commandArgs(trailingOnly = TRUE)",
    "arg <- function(name) args[[match(name, args) + 1L]]",
    "documents <- jsonlite::fromJSON(arg('--input'), simplifyVector = FALSE)",
    "trim_one <- function(document) {",
    "  rectype <- if (is.null(document$rectype)) '' else document$rectype",
    "  is_bt <- grepl('FORMCHECKBOX', document$text, fixed = TRUE) &&",
    "    (identical(rectype, 'BT') || startsWith(rectype, 'ORDON'))",
    "  intervals <- if (is_bt || !nzchar(document$text)) list() else list(list(",
    "    start = 1L, end = nchar(document$text), family = 'model', text = document$text",
    "  ))",
    "  list(",
    "    id = document$id,",
    "    trimmed_text = if (is_bt) '' else document$text,",
    "    reduction_pct = if (is_bt) 100 else 0,",
    "    is_bt = is_bt,",
    "    preserved_intervals = intervals",
    "  )",
    "}",
    "results <- rev(lapply(documents, trim_one))",
    result_mutation,
    "writeLines(jsonlite::toJSON(results, auto_unbox = TRUE), arg('--output'))"
  )
  writeLines(worker, file.path(model_dir, "trim_batch_service.py"))

  list(
    model_dir = model_dir,
    runner = file.path(
      R.home("bin"),
      if (.Platform$OS.type == "windows") "Rscript.exe" else "Rscript"
    )
  )
}

test_that("transport vouchers require FORMCHECKBOX and a transport RECTYPE", {
  fixture <- trimmer_fixture()
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  doceds <- data.frame(
    ELTID = c("clinical-form", "bt", "ordon", "bt-without-marker"),
    RECTYPE = c("CRH2AB", "BT", "ORDON7", "BT"),
    RECTXT = c(
      "FORMCHECKBOX\nClinical narrative",
      "FORMCHECKBOX\nTransport form",
      "FORMCHECKBOX\nPrescription transport form",
      "Clinical narrative"
    ),
    stringsAsFactors = FALSE
  )

  result <- trim_doceds_onnx(
    doceds,
    python_exe = fixture$runner,
    model_dir = fixture$model_dir
  )

  expect_identical(result$ELTID, doceds$ELTID)
  expect_identical(
    result$RECTXT_TRIMMED,
    c("FORMCHECKBOX\nClinical narrative", "", "", "Clinical narrative")
  )
  expect_identical(result$TRIM_IS_BT, c(FALSE, TRUE, TRUE, FALSE))
})

test_that("preserved intervals are always valid scalar JSON", {
  fixture <- trimmer_fixture()
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  result <- trim_doceds_onnx(
    data.frame(
      ELTID = c("clinical", "transport"),
      RECTYPE = c("CRH2AB", "BT"),
      RECTXT = c("Clinical narrative", "FORMCHECKBOX"),
      stringsAsFactors = FALSE
    ),
    python_exe = fixture$runner,
    model_dir = fixture$model_dir
  )

  expect_true(all(vapply(
    result$TRIM_PRESERVED_INTERVALS,
    jsonlite::validate,
    logical(1)
  )))
  expect_identical(result$TRIM_PRESERVED_INTERVALS[[2L]], "[]")
})

test_that("malformed worker output is rejected", {
  fixture <- trimmer_fixture("results[[1L]]$trimmed_text <- NULL")
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  expect_error(
    trim_doceds_onnx(
      data.frame(
        ELTID = "doc-1",
        RECTYPE = "CRH2AB",
        RECTXT = "Clinical narrative",
        stringsAsFactors = FALSE
      ),
      python_exe = fixture$runner,
      model_dir = fixture$model_dir
    ),
    "Trimmer worker returned an invalid result for document doc-1"
  )
})

test_that("grounding intervals must match the original RECTXT", {
  fixture <- trimmer_fixture(
    "results[[1L]]$preserved_intervals[[1L]]$text <- 'Different text'"
  )
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  expect_error(
    trim_doceds_onnx(
      data.frame(
        ELTID = "doc-1",
        RECTYPE = "CRH2AB",
        RECTXT = "Clinical narrative",
        stringsAsFactors = FALSE
      ),
      python_exe = fixture$runner,
      model_dir = fixture$model_dir
    ),
    "invalid result for document doc-1"
  )
})

test_that("trimmed text must be reconstructed from grounded intervals", {
  fixture <- trimmer_fixture("results[[1L]]$trimmed_text <- 'Ungrounded text'")
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  expect_error(
    trim_doceds_onnx(
      "Clinical narrative",
      python_exe = fixture$runner,
      model_dir = fixture$model_dir
    ),
    "invalid result"
  )
})

test_that("worker result identities must match the request", {
  fixture <- trimmer_fixture("results[[1L]]$id <- 'unexpected'")
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  expect_error(
    trim_doceds_onnx(
      data.frame(
        ELTID = "doc-1",
        RECTYPE = "CRH2AB",
        RECTXT = "Clinical narrative",
        stringsAsFactors = FALSE
      ),
      python_exe = fixture$runner,
      model_dir = fixture$model_dir
    ),
    "result identifiers do not match the request"
  )
})

test_that("short and duplicate worker responses are rejected", {
  mutations <- c(
    "results <- results[-length(results)]",
    "results <- c(results, results)"
  )

  for (mutation in mutations) {
    fixture <- trimmer_fixture(mutation)
    on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)
    expect_error(
      trim_doceds_onnx(
        data.frame(
          ELTID = "doc-1",
          RECTYPE = "CRH2AB",
          RECTXT = "Clinical narrative",
          stringsAsFactors = FALSE
        ),
        python_exe = fixture$runner,
        model_dir = fixture$model_dir
      ),
      "result identifiers do not match the request"
    )
  }
})

test_that("missing RECTYPE never activates the transport shortcut", {
  fixture <- trimmer_fixture()
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  result <- trim_doceds_onnx(
    data.frame(
      ELTID = "doc-1",
      RECTXT = "FORMCHECKBOX\nClinical narrative",
      stringsAsFactors = FALSE
    ),
    python_exe = fixture$runner,
    model_dir = fixture$model_dir
  )

  expect_identical(result$RECTXT_TRIMMED, "FORMCHECKBOX\nClinical narrative")
  expect_identical(result$TRIM_IS_BT, FALSE)
})

test_that("blank RECTYPE never activates the transport shortcut", {
  fixture <- trimmer_fixture()
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  result <- trim_doceds_onnx(
    data.frame(
      ELTID = "doc-1",
      RECTYPE = "",
      RECTXT = "FORMCHECKBOX\nClinical narrative",
      stringsAsFactors = FALSE
    ),
    python_exe = fixture$runner,
    model_dir = fixture$model_dir
  )

  expect_identical(result$TRIM_IS_BT, FALSE)
})

test_that("cohort batching preserves bundle and DOCEDS structure", {
  fixture <- trimmer_fixture()
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  first_doceds <- structure(
    data.frame(
      ELTID = "first",
      RECTYPE = "BT",
      RECTXT = "FORMCHECKBOX",
      keep = 11L,
      stringsAsFactors = FALSE
    ),
    source_note = "first table"
  )
  second_doceds <- data.frame(
    ELTID = "second",
    RECTYPE = "CRH2AB",
    RECTXT = "Clinical narrative",
    keep = 22L,
    stringsAsFactors = FALSE
  )
  cohort <- list(
    structure(
      list(event_id = "evt-1", sources = list(doceds = first_doceds)),
      class = "edsan_event_bundle",
      bundle_note = "first bundle"
    ),
    structure(
      list(event_id = "evt-2", sources = list(doceds = second_doceds)),
      class = "edsan_event_bundle"
    )
  )

  result <- trim_doceds_onnx(
    cohort,
    python_exe = fixture$runner,
    model_dir = fixture$model_dir
  )

  expect_s3_class(result[[1L]], "edsan_event_bundle")
  expect_identical(attr(result[[1L]], "bundle_note"), "first bundle")
  expect_identical(attr(result[[1L]]$sources$doceds, "source_note"), "first table")
  expect_identical(result[[1L]]$sources$doceds$keep, 11L)
  expect_identical(result[[2L]]$sources$doceds$keep, 22L)
  expect_identical(result[[1L]]$sources$doceds$RECTXT_TRIMMED, "")
  expect_identical(result[[1L]]$sources$doceds$TRIM_IS_BT, TRUE)
  expect_identical(
    result[[2L]]$sources$doceds$RECTXT_TRIMMED,
    "Clinical narrative"
  )
})

test_that("single and empty bundles preserve the bundle contract", {
  fixture <- trimmer_fixture()
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  single <- structure(
    list(sources = list(doceds = data.frame(
      ELTID = "single",
      RECTYPE = "CRH2AB",
      RECTXT = "Clinical narrative",
      stringsAsFactors = FALSE
    ))),
    class = "edsan_event_bundle"
  )
  empty <- structure(
    list(sources = list(doceds = data.frame(
      ELTID = character(),
      RECTYPE = character(),
      RECTXT = character(),
      stringsAsFactors = FALSE
    ))),
    class = "edsan_event_bundle"
  )

  single_result <- trim_doceds_onnx(
    single,
    python_exe = fixture$runner,
    model_dir = fixture$model_dir
  )
  empty_result <- trim_doceds_onnx(
    empty,
    python_exe = fixture$runner,
    model_dir = fixture$model_dir
  )

  expect_s3_class(single_result, "edsan_event_bundle")
  expect_identical(
    single_result$sources$doceds$RECTXT_TRIMMED,
    "Clinical narrative"
  )
  expect_identical(empty_result, empty)
})

test_that("bundle inputs reject malformed DOCEDS contracts", {
  fixture <- trimmer_fixture()
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  valid <- structure(
    list(sources = list(doceds = data.frame(
      ELTID = "valid",
      RECTYPE = "CRH2AB",
      RECTXT = "Clinical narrative",
      stringsAsFactors = FALSE
    ))),
    class = "edsan_event_bundle"
  )
  missing_rectype <- structure(
    list(sources = list(doceds = data.frame(
      ELTID = "invalid",
      RECTXT = "Clinical narrative",
      stringsAsFactors = FALSE
    ))),
    class = "edsan_event_bundle"
  )

  expect_error(
    trim_doceds_onnx(
      list(valid, list(not = "a bundle")),
      python_exe = fixture$runner,
      model_dir = fixture$model_dir
    ),
    "Every cohort element must be an edsan_event_bundle"
  )
  expect_error(
    trim_doceds_onnx(
      missing_rectype,
      python_exe = fixture$runner,
      model_dir = fixture$model_dir
    ),
    "DOCEDS table must contain columns: RECTXT, RECTYPE"
  )
  expect_error(
    trim_doceds_onnx(
      list(valid),
      text_col = "MODEL_TEXT",
      python_exe = fixture$runner,
      model_dir = fixture$model_dir
    ),
    "DOCEDS table must contain columns: RECTXT, RECTYPE, MODEL_TEXT"
  )
  expect_error(
    trim_doceds_onnx(data.frame(), text_col = "MODEL_TEXT"),
    "Column 'MODEL_TEXT' not found in data"
  )
})

test_that("edsan_install_trimmer validates inputs with helpful messages", {
  # Missing or empty zip_path
  expect_error(
    edsan_install_trimmer(),
    "Please provide the path to a compatible edsan-doc-trimmer archive"
  )
  expect_error(
    edsan_install_trimmer(""),
    "Please provide the path to a compatible edsan-doc-trimmer archive"
  )

  # Nonexistent file
  expect_error(
    edsan_install_trimmer("nonexistent_model_file.zip"),
    "Model zip file not found at"
  )
})

test_that("edsan_install_trimmer extracts and prints guidance", {
  tmp_zip_dir <- tempfile("trimmer_test_src_")
  dir.create(tmp_zip_dir)
  on.exit(unlink(tmp_zip_dir, recursive = TRUE), add = TRUE)

  # Create dummy model files
  fake_model <- file.path(tmp_zip_dir, "model.onnx")
  fake_tokenizer <- file.path(tmp_zip_dir, "tokenizer.json")
  fake_script <- file.path(tmp_zip_dir, "trim_batch_service.py")
  fake_manifest <- file.path(tmp_zip_dir, "artifact.json")
  writeLines("dummy model", fake_model)
  writeLines("{}", fake_tokenizer)
  writeLines("dummy script", fake_script)
  writeLines(
    '{"artifact_version":"1.1.0","worker_contract":"rectype-aware-v1"}',
    fake_manifest
  )

  # Zip them
  zip_file <- tempfile(fileext = ".zip")
  on.exit(unlink(zip_file), add = TRUE)
  utils::zip(
    zip_file,
    files = c(fake_model, fake_tokenizer, fake_script, fake_manifest),
    flags = "-j"
  )

  # Install to temp destination
  target_cache <- tempfile("trimmer_test_dest_")
  on.exit(unlink(target_cache, recursive = TRUE), add = TRUE)

  msg <- testthat::capture_messages({
    res <- edsan_install_trimmer(zip_file, dest_dir = target_cache)
  })

  expect_true(file.exists(file.path(target_cache, "model.onnx")))
  expect_true(any(grepl("edsan-doc-trimmer model successfully installed", msg)))
  expect_true(any(grepl("How to verify it works", msg)))
})

test_that("edsan_install_trimmer preserves an existing install on invalid input", {
  tmp_zip_dir <- tempfile("incomplete_trimmer_")
  dir.create(tmp_zip_dir)
  on.exit(unlink(tmp_zip_dir, recursive = TRUE), add = TRUE)
  writeLines("dummy model", file.path(tmp_zip_dir, "model.onnx"))

  zip_file <- tempfile(fileext = ".zip")
  on.exit(unlink(zip_file), add = TRUE)
  utils::zip(
    zip_file,
    files = file.path(tmp_zip_dir, "model.onnx"),
    flags = "-j"
  )

  target_cache <- tempfile("existing_trimmer_")
  dir.create(target_cache)
  writeLines("keep me", file.path(target_cache, "existing.txt"))
  on.exit(unlink(target_cache, recursive = TRUE), add = TRUE)

  expect_error(
    edsan_install_trimmer(zip_file, dest_dir = target_cache),
    "missing required files: tokenizer.json, trim_batch_service.py, artifact.json"
  )
  expect_identical(
    readLines(file.path(target_cache, "existing.txt"), warn = FALSE),
    "keep me"
  )
})

test_that("edsan_install_trimmer rejects incompatible worker contracts", {
  tmp_zip_dir <- tempfile("incompatible_trimmer_")
  dir.create(tmp_zip_dir)
  on.exit(unlink(tmp_zip_dir, recursive = TRUE), add = TRUE)
  writeLines("dummy model", file.path(tmp_zip_dir, "model.onnx"))
  writeLines("{}", file.path(tmp_zip_dir, "tokenizer.json"))
  writeLines("dummy script", file.path(tmp_zip_dir, "trim_batch_service.py"))
  writeLines(
    '{"artifact_version":"1.0.0","worker_contract":"text-only-v1"}',
    file.path(tmp_zip_dir, "artifact.json")
  )

  zip_file <- tempfile(fileext = ".zip")
  on.exit(unlink(zip_file), add = TRUE)
  utils::zip(
    zip_file,
    files = list.files(tmp_zip_dir, full.names = TRUE),
    flags = "-j"
  )

  expect_error(
    edsan_install_trimmer(zip_file, dest_dir = tempfile("target_cache_")),
    "requires artifact_version >= 1.1.0 and worker_contract 'rectype-aware-v1'"
  )
})

test_that("edsan_trimmer_cache_dir returns valid path string", {
  cache_dir <- edsan_trimmer_cache_dir()
  expect_true(is.character(cache_dir))
  expect_true(nzchar(cache_dir))
  expect_true(grepl("edsan_doc_trimmer", cache_dir))
})

test_that("edsan_install_trimmer rejects directory paths with guidance", {
  tmp_dir <- tempfile("not_a_zip_")
  dir.create(tmp_dir)
  on.exit(unlink(tmp_dir, recursive = TRUE), add = TRUE)

  expect_error(
    edsan_install_trimmer(tmp_dir),
    "Specified path is a directory, not a zip archive"
  )
})

test_that("edsan_install_trimmer detects zip missing model.onnx", {
  tmp_zip_dir <- tempfile("wrong_zip_src_")
  dir.create(tmp_zip_dir)
  on.exit(unlink(tmp_zip_dir, recursive = TRUE), add = TRUE)

  # Zip without model.onnx
  fake_readme <- file.path(tmp_zip_dir, "README.md")
  writeLines("not a model", fake_readme)
  zip_file <- tempfile(fileext = ".zip")
  on.exit(unlink(zip_file), add = TRUE)
  utils::zip(zip_file, files = fake_readme, flags = "-j")

  target_cache <- tempfile("target_cache_")
  on.exit(unlink(target_cache, recursive = TRUE), add = TRUE)

  expect_error(
    edsan_install_trimmer(zip_file, dest_dir = target_cache),
    "Invalid trimmer archive: 'model.onnx' not found after extraction"
  )
})

test_that(".edsan_get_trimmer_dir unwraps direct model.onnx file path", {
  tmp_model_dir <- tempfile("model_dir_")
  dir.create(tmp_model_dir)
  on.exit(unlink(tmp_model_dir, recursive = TRUE), add = TRUE)

  model_file <- file.path(tmp_model_dir, "model.onnx")
  writeLines("dummy", model_file)

  # User sets path directly to model.onnx file instead of directory
  old_path <- Sys.getenv("EDSAN_TRIMMER_PATH", unset = NA_character_)
  on.exit({
    if (is.na(old_path)) {
      Sys.unsetenv("EDSAN_TRIMMER_PATH")
    } else {
      Sys.setenv(EDSAN_TRIMMER_PATH = old_path)
    }
  }, add = TRUE)
  Sys.setenv(EDSAN_TRIMMER_PATH = model_file)

  resolved <- redsan:::.edsan_get_trimmer_dir()
  expect_identical(normalizePath(resolved), normalizePath(tmp_model_dir))
})

test_that("trim_doceds_onnx handles empty inputs gracefully without spawning process", {
  # Character vector of length 0
  res_char <- trim_doceds_onnx(character(0))
  expect_identical(res_char, character(0))

  # Data frame with 0 rows
  empty_df <- data.frame(RECTXT = character(0), stringsAsFactors = FALSE)
  res_df <- trim_doceds_onnx(empty_df)
  expect_equal(nrow(res_df), 0L)
  expect_true("RECTXT_TRIMMED" %in% names(res_df))
  expect_true("TRIM_REDUCTION_PCT" %in% names(res_df))
  expect_true("TRIM_IS_BT" %in% names(res_df))
  expect_true("TRIM_PRESERVED_INTERVALS" %in% names(res_df))
})
