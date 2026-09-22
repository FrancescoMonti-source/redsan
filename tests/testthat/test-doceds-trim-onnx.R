trimmer_protocol_fixture <- function(result_mutation = character()) {
  model_dir <- tempfile("trimmer_model_")
  dir.create(model_dir)
  writeLines("dummy model", file.path(model_dir, "model.onnx"))
  writeLines("{}", file.path(model_dir, "tokenizer.json"))
  writeLines(
    '{"artifact_version":"1.2.0","worker_contract":"model-only-v1"}',
    file.path(model_dir, "artifact.json")
  )

  worker <- c(
    "args <- commandArgs(trailingOnly = TRUE)",
    "arg <- function(name) args[[match(name, args) + 1L]]",
    "documents <- jsonlite::fromJSON(arg('--input'), simplifyVector = FALSE)",
    "trim_one <- function(document) {",
    "  stopifnot(identical(sort(names(document)), c('id', 'text')))",
    "  intervals <- if (!nzchar(document$text)) list() else list(list(",
    "    start = 1L, end = nchar(document$text), family = 'fixture', text = document$text",
    "  ))",
    "  list(",
    "    id = document$id,",
    "    trimmed_text = document$text,",
    "    reduction_pct = 0,",
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

trimmer_archive_fixture <- function(
  nested = FALSE,
  manifest = '{"artifact_version":"1.2.0","worker_contract":"model-only-v1"}'
) {
  source_dir <- tempfile("trimmer_archive_")
  artifact_dir <- if (nested) {
    file.path(source_dir, "release", "runtime")
  } else {
    source_dir
  }
  dir.create(artifact_dir, recursive = TRUE)
  writeLines("dummy model", file.path(artifact_dir, "model.onnx"))
  writeLines("{}", file.path(artifact_dir, "tokenizer.json"))
  writeLines("dummy script", file.path(artifact_dir, "trim_batch_service.py"))
  writeLines(manifest, file.path(artifact_dir, "artifact.json"))

  zip_file <- tempfile(fileext = ".zip")
  old_dir <- setwd(source_dir)
  on.exit({
    setwd(old_dir)
    unlink(source_dir, recursive = TRUE)
  }, add = TRUE)
  files <- if (nested) {
    list.files("release", recursive = TRUE, full.names = TRUE)
  } else {
    list.files(".", recursive = TRUE, full.names = TRUE)
  }
  utils::zip(zip_file, files = files)
  zip_file
}

test_that("RECTYPE stays in DOCEDS but is absent from the worker protocol", {
  fixture <- trimmer_protocol_fixture()
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  doceds <- data.frame(
    ELTID = c("first", "second"),
    RECTYPE = c("protocol-false", "protocol-true"),
    RECTXT = c("First text", "Second text"),
    stringsAsFactors = FALSE
  )

  result <- trim_doceds_onnx(
    doceds,
    python_exe = fixture$runner,
    model_dir = fixture$model_dir
  )

  expect_identical(result$ELTID, doceds$ELTID)
  expect_identical(result$RECTYPE, doceds$RECTYPE)
  expect_identical(
    result$RECTXT_TRIMMED,
    doceds$RECTXT
  )
  expect_false("TRIM_IS_BT" %in% names(result))
})

test_that("named character-vector inputs preserve names", {
  fixture <- trimmer_protocol_fixture()
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  input <- c(first = "Clinical narrative", second = "Other narrative")
  result <- trim_doceds_onnx(
    input,
    python_exe = fixture$runner,
    model_dir = fixture$model_dir
  )

  expect_identical(names(result), names(input))
  expect_identical(unname(result), unname(input))
})

test_that("preserved intervals are always valid scalar JSON", {
  fixture <- trimmer_protocol_fixture()
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  result <- trim_doceds_onnx(
    data.frame(
      ELTID = c("clinical", "transport"),
      RECTYPE = c("protocol-false", "protocol-false"),
      RECTXT = c("Clinical narrative", ""),
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
  fixture <- trimmer_protocol_fixture("results[[1L]]$trimmed_text <- NULL")
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

test_that("legacy worker response fields are rejected", {
  fixture <- trimmer_protocol_fixture("results[[1L]]$is_bt <- FALSE")
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  expect_error(
    trim_doceds_onnx(
      "Clinical narrative",
      python_exe = fixture$runner,
      model_dir = fixture$model_dir
    ),
    "Trimmer worker returned an invalid result for document doc_1"
  )
})

test_that("grounding intervals must match the original RECTXT", {
  fixture <- trimmer_protocol_fixture(
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

test_that("grounding compares French text independently of native encoding", {
  utf8_text <- enc2utf8("Le patient présente une dyspnée aiguë.")
  native_text <- iconv(utf8_text, from = "UTF-8", to = "latin1")
  item <- list(
    id = "doc-1",
    trimmed_text = utf8_text,
    reduction_pct = 0,
    preserved_intervals = list(list(
      start = 1,
      end = nchar(native_text),
      family = "clinical",
      text = utf8_text
    ))
  )

  expect_invisible(
    .doceds_onnx_validate_result(item, "doc-1", native_text)
  )
})

test_that("trimmed text assembly may choose whitespace between grounded intervals", {
  fixture <- trimmer_protocol_fixture(
    c(
      "results[[1L]]$preserved_intervals <- list(",
      "  list(start = 1L, end = 5L, family = 'fixture', text = 'First'),",
      "  list(start = 7L, end = 12L, family = 'fixture', text = 'Second')",
      ")",
      "results[[1L]]$trimmed_text <- 'First\\n\\nSecond'"
    )
  )
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  expect_identical(
    trim_doceds_onnx(
      "First Second",
      python_exe = fixture$runner,
      model_dir = fixture$model_dir
    ),
    "First\n\nSecond"
  )
})

test_that("trimmed text cannot introduce content outside grounded intervals", {
  fixture <- trimmer_protocol_fixture(
    "results[[1L]]$trimmed_text <- 'Unrelated text'"
  )
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  expect_error(
    trim_doceds_onnx(
      "Clinical narrative",
      python_exe = fixture$runner,
      model_dir = fixture$model_dir
    ),
    "invalid result for document doc_1"
  )
})

test_that("worker result identities must match the request", {
  fixture <- trimmer_protocol_fixture("results[[1L]]$id <- 'unexpected'")
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
    fixture <- trimmer_protocol_fixture(mutation)
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

test_that("data frames without RECTYPE use the model-only protocol", {
  fixture <- trimmer_protocol_fixture()
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  result <- trim_doceds_onnx(
    data.frame(
      ELTID = c("missing"),
      RECTXT = c("Clinical narrative"),
      stringsAsFactors = FALSE
    ),
    python_exe = fixture$runner,
    model_dir = fixture$model_dir
  )

  expect_identical(result$RECTXT_TRIMMED, "Clinical narrative")
  expect_false("TRIM_IS_BT" %in% names(result))
})

test_that("cohort batching preserves bundle and DOCEDS structure", {
  fixture <- trimmer_protocol_fixture()
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  first_doceds <- structure(
    data.frame(
      ELTID = "first",
      RECTYPE = "protocol-true",
      RECTXT = "First text",
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
  empty_doceds <- structure(
    data.frame(
      ELTID = character(),
      RECTYPE = character(),
      RECTXT = character(),
      keep = integer(),
      stringsAsFactors = FALSE
    ),
    source_note = "empty table"
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
    ),
    structure(
      list(event_id = "evt-3", sources = list(doceds = empty_doceds)),
      class = "edsan_event_bundle",
      bundle_note = "empty bundle"
    )
  )
  names(cohort) <- c("first", "second", "empty")
  attr(cohort, "cohort_note") <- "keep cohort attributes"

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
  expect_identical(result[[1L]]$sources$doceds$RECTXT_TRIMMED, "First text")
  expect_false("TRIM_IS_BT" %in% names(result[[1L]]$sources$doceds))
  expect_identical(
    result[[2L]]$sources$doceds$RECTXT_TRIMMED,
    "Clinical narrative"
  )
  expect_identical(names(result), names(cohort))
  expect_identical(attr(result, "cohort_note"), "keep cohort attributes")
  expect_identical(attr(result[[3L]], "bundle_note"), "empty bundle")
  expect_identical(
    attr(result[[3L]]$sources$doceds, "source_note"),
    "empty table"
  )
  expect_identical(
    names(result[[3L]]$sources$doceds),
    c(
      names(empty_doceds),
      "RECTXT_TRIMMED",
      "TRIM_REDUCTION_PCT",
      "TRIM_PRESERVED_INTERVALS"
    )
  )
})

test_that("single and empty bundles preserve the bundle contract", {
  fixture <- trimmer_protocol_fixture()
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
  expect_s3_class(empty_result, "edsan_event_bundle")
  expect_identical(
    names(empty_result$sources$doceds),
    c(
      names(empty$sources$doceds),
      "RECTXT_TRIMMED",
      "TRIM_REDUCTION_PCT",
      "TRIM_PRESERVED_INTERVALS"
    )
  )
  expect_identical(empty_result$sources$doceds$RECTXT_TRIMMED, character())
})

test_that("bundle inputs reject malformed DOCEDS contracts", {
  fixture <- trimmer_protocol_fixture()
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
    '{"artifact_version":"1.2.0","worker_contract":"model-only-v1"}',
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
    '{"artifact_version":"1.2.0","worker_contract":"rectype-aware-v1"}',
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
    "requires artifact_version >= 1.2.0 and worker_contract 'model-only-v1'"
  )
})

test_that("edsan_install_trimmer rejects malformed manifests", {
  zip_file <- trimmer_archive_fixture(manifest = "{not-json")
  on.exit(unlink(zip_file), add = TRUE)

  expect_error(
    edsan_install_trimmer(zip_file, dest_dir = tempfile("target_cache_")),
    "requires artifact_version >= 1.2.0 and worker_contract 'model-only-v1'"
  )
})

test_that("edsan_install_trimmer accepts one nested artifact root", {
  zip_file <- trimmer_archive_fixture(nested = TRUE)
  on.exit(unlink(zip_file), add = TRUE)
  target_cache <- tempfile("nested_target_cache_")
  on.exit(unlink(target_cache, recursive = TRUE), add = TRUE)

  suppressMessages(edsan_install_trimmer(zip_file, dest_dir = target_cache))

  expect_true(file.exists(file.path(target_cache, "model.onnx")))
  expect_true(file.exists(file.path(target_cache, "artifact.json")))
  expect_false(dir.exists(file.path(target_cache, "release")))
})

test_that("edsan_install_trimmer restores an existing install when publish fails", {
  zip_file <- trimmer_archive_fixture()
  on.exit(unlink(zip_file), add = TRUE)
  target_cache <- tempfile("rollback_target_cache_")
  dir.create(target_cache)
  writeLines("keep me", file.path(target_cache, "existing.txt"))
  on.exit(unlink(target_cache, recursive = TRUE), add = TRUE)
  testthat::local_mocked_bindings(
    .doceds_onnx_publish_artifact = function(...) FALSE,
    .package = "redsan"
  )

  expect_error(
    edsan_install_trimmer(zip_file, dest_dir = target_cache),
    "Could not publish the validated trimmer artifact"
  )
  expect_identical(
    readLines(file.path(target_cache, "existing.txt"), warn = FALSE),
    "keep me"
  )
  expect_false(file.exists(file.path(target_cache, "model.onnx")))
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
  empty_df <- structure(
    data.frame(RECTXT = character(0), keep = integer(), stringsAsFactors = FALSE),
    class = c("custom_doceds", "data.frame"),
    source_note = "preserve me"
  )
  res_df <- trim_doceds_onnx(
    empty_df,
    python_exe = "does-not-exist",
    model_dir = "does-not-exist"
  )
  expect_equal(nrow(res_df), 0L)
  expect_true("RECTXT_TRIMMED" %in% names(res_df))
  expect_true("TRIM_REDUCTION_PCT" %in% names(res_df))
  expect_false("TRIM_IS_BT" %in% names(res_df))
  expect_true("TRIM_PRESERVED_INTERVALS" %in% names(res_df))
  expect_s3_class(res_df, "custom_doceds")
  expect_identical(attr(res_df, "source_note"), "preserve me")
  expect_identical(res_df$keep, integer())

  empty_bundle <- structure(
    list(sources = list(doceds = data.frame(
      ELTID = character(),
      RECTYPE = character(),
      RECTXT = character(),
      stringsAsFactors = FALSE
    ))),
    class = "edsan_event_bundle"
  )
  res_cohort <- trim_doceds_onnx(
    list(named = empty_bundle),
    python_exe = "does-not-exist",
    model_dir = "does-not-exist"
  )
  expect_identical(names(res_cohort), "named")
  expect_identical(
    names(res_cohort[[1L]]$sources$doceds),
    c(
      "ELTID", "RECTYPE", "RECTXT", "RECTXT_TRIMMED",
      "TRIM_REDUCTION_PCT", "TRIM_PRESERVED_INTERVALS"
    )
  )
})


test_that("the artifact spec identifies what produced a trimmed text", {
  model_dir <- trimmer_protocol_fixture()$model_dir
  spec <- doceds_onnx_spec(model_dir)

  expect_identical(spec$package, "redsan")
  expect_identical(spec$version, as.character(utils::packageVersion("redsan")))
  expect_match(spec$digest, "^[0-9a-f]{64}$")
  expect_identical(spec$digest_algorithm, "sha256")
  expect_identical(spec$digest_schema, "doceds-onnx-artifact-v1")
  expect_identical(spec$artifact_version, "1.1.0")
  expect_identical(spec$worker_contract, "rectype-aware-v1")

  # A manifest that names nothing still produces a spec, because the digest is
  # the field that answers the question. Absent prose reads as absent.
  expect_identical(spec$artifact_name, NA_character_)
})

test_that("the digest follows the artifact and not the manifest", {
  model_dir <- trimmer_protocol_fixture()$model_dir
  before <- doceds_onnx_spec(model_dir)$digest

  # Same artifact, asked twice: the cache must not be the reason two runs agree,
  # so re-reading an untouched directory has to give the same answer as hashing
  # it fresh would.
  expect_identical(doceds_onnx_spec(model_dir)$digest, before)

  # The weights moved and nobody edited `artifact_version`. This is the case the
  # spec exists for: the version still reads 1.1.0 and the digest does not.
  writeLines("different weights", file.path(model_dir, "model.onnx"))
  after <- doceds_onnx_spec(model_dir)
  expect_identical(after$artifact_version, "1.1.0")
  expect_false(identical(after$digest, before))

  # The worker script decides which documents are deleted whole, so it is part
  # of what produced the text.
  worker_changed <- trimmer_protocol_fixture()$model_dir
  baseline <- doceds_onnx_spec(worker_changed)$digest
  cat("\n# routing changed\n", file = file.path(worker_changed, "trim_batch_service.py"), append = TRUE)
  expect_false(identical(doceds_onnx_spec(worker_changed)$digest, baseline))
})

test_that("an invalid artifact has no spec rather than an unverifiable one", {
  model_dir <- trimmer_protocol_fixture()$model_dir
  file.remove(file.path(model_dir, "artifact.json"))

  expect_error(doceds_onnx_spec(model_dir), "missing required files")
})
