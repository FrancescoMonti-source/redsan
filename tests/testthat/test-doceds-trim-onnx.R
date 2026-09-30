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
    "    execution_provider = 'CPUExecutionProvider',",
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

write_trimmer_artifact <- function(
  dir,
  version = "1.2.0",
  model = "dummy model",
  manifest = NULL
) {
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  if (is.null(manifest)) {
    manifest <- sprintf(
      '{"artifact_version":"%s","worker_contract":"model-only-v1"}',
      version
    )
  }
  writeLines(model, file.path(dir, "model.onnx"))
  writeLines("{}", file.path(dir, "tokenizer.json"))
  writeLines("dummy script", file.path(dir, "trim_batch_service.py"))
  writeLines(manifest, file.path(dir, "artifact.json"))
  invisible(dir)
}

trimmer_archive_fixture <- function(
  nested = FALSE,
  manifest = NULL,
  version = "1.2.0",
  model = "dummy model"
) {
  source_dir <- tempfile("trimmer_archive_")
  artifact_dir <- if (nested) {
    file.path(source_dir, "release", "runtime")
  } else {
    source_dir
  }
  write_trimmer_artifact(artifact_dir, version, model, manifest)

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

# Redirect the trimmer cache to a temp dir through R_USER_CACHE_DIR and clear
# every variable that could steer discovery. Returns the cache root.
local_trimmer_cache <- function(env = parent.frame()) {
  base <- withr::local_tempdir(.local_envir = env)
  withr::local_envvar(
    R_USER_CACHE_DIR = base,
    EDSAN_TRIMMER_PATH = NA,
    REDSAN_TRIMMER_PATH = NA,
    EDSAN_TRIMMER_VERSION = NA,
    .local_envir = env
  )
  edsan_trimmer_cache_dir()
}

install_trimmer_versions <- function(versions) {
  root <- edsan_trimmer_cache_dir()
  for (version in versions) {
    write_trimmer_artifact(file.path(root, version), version)
  }
  invisible(root)
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
  expect_identical(unname(as.vector(result)), unname(as.vector(input)))
  expect_identical(
    attr(result, "TRIM_EXECUTION_PROVIDER"),
    "CPUExecutionProvider"
  )
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

  result <- trim_doceds_onnx(
    "First Second",
    python_exe = fixture$runner,
    model_dir = fixture$model_dir
  )
  expect_identical(unname(as.vector(result)), "First\n\nSecond")
  expect_identical(
    attr(result, "TRIM_EXECUTION_PROVIDER"),
    "CPUExecutionProvider"
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
  expect_identical(
    result[[1L]]$sources$doceds$TRIM_EXECUTION_PROVIDER,
    "CPUExecutionProvider"
  )
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
      "TRIM_PRESERVED_INTERVALS",
      "TRIM_EXECUTION_PROVIDER"
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
      "TRIM_PRESERVED_INTERVALS",
      "TRIM_EXECUTION_PROVIDER"
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
  local_trimmer_cache()

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

test_that("edsan_install_trimmer no longer accepts a destination", {
  expect_false("dest_dir" %in% names(formals(edsan_install_trimmer)))
  expect_identical(formals(edsan_install_trimmer)$overwrite, FALSE)
})

test_that("edsan_install_trimmer installs into a folder named after the version", {
  cache <- local_trimmer_cache()
  zip_file <- trimmer_archive_fixture(version = "1.3.0")
  on.exit(unlink(zip_file), add = TRUE)

  msg <- testthat::capture_messages({
    res <- edsan_install_trimmer(zip_file)
  })

  target <- file.path(cache, "1.3.0")
  expect_identical(normalizePath(res), normalizePath(target))
  expect_true(file.exists(file.path(target, "model.onnx")))
  expect_true(file.exists(file.path(target, "artifact.json")))
  expect_identical(list.dirs(cache, recursive = FALSE, full.names = FALSE), "1.3.0")

  msg <- paste(msg, collapse = "")
  expect_match(msg, "edsan-doc-trimmer model successfully installed")
  expect_match(msg, "Version:  1.3.0", fixed = TRUE)
  expect_match(msg, "Location: ", fixed = TRUE)
  expect_match(msg, "Selected:  yes", fixed = TRUE)
  expect_match(msg, "How to verify it works", fixed = TRUE)
  expect_match(msg, "Quick smoke test", fixed = TRUE)
  expect_match(msg, "pierre.dupont@gmail.com", fixed = TRUE)
  expect_match(msg, "# Expected:", fixed = TRUE)

  versions <- edsan_trimmer_versions()
  expect_identical(versions$version, "1.3.0")
  expect_identical(versions$selected, TRUE)
  expect_identical(normalizePath(versions$path), normalizePath(target))
})

test_that("reinstalling the same archive is a no-op with a message", {
  cache <- local_trimmer_cache()
  zip_file <- trimmer_archive_fixture(version = "1.3.0")
  on.exit(unlink(zip_file), add = TRUE)
  suppressMessages(edsan_install_trimmer(zip_file))
  marker <- file.path(cache, "1.3.0", "marker.txt")
  writeLines("still here", marker)

  msg <- testthat::capture_messages(edsan_install_trimmer(zip_file))

  expect_match(paste(msg, collapse = ""), "already installed with identical content")
  expect_true(file.exists(marker))
  expect_identical(list.dirs(cache, recursive = FALSE, full.names = FALSE), "1.3.0")
})

test_that("the same version with different content errors unless overwrite = TRUE", {
  cache <- local_trimmer_cache()
  original <- trimmer_archive_fixture(version = "1.3.0", model = "original weights")
  rebuilt <- trimmer_archive_fixture(version = "1.3.0", model = "rebuilt weights")
  on.exit(unlink(c(original, rebuilt)), add = TRUE)
  suppressMessages(edsan_install_trimmer(original))
  installed_model <- file.path(cache, "1.3.0", "model.onnx")

  expect_error(
    edsan_install_trimmer(rebuilt),
    "1.3.0 is already installed .* different content.*overwrite = TRUE"
  )
  expect_identical(readLines(installed_model, warn = FALSE), "original weights")

  suppressMessages(edsan_install_trimmer(rebuilt, overwrite = TRUE))
  expect_identical(readLines(installed_model, warn = FALSE), "rebuilt weights")
  expect_identical(list.dirs(cache, recursive = FALSE, full.names = FALSE), "1.3.0")
})

test_that("installing a version leaves other versions in place", {
  cache <- local_trimmer_cache()
  old <- trimmer_archive_fixture(version = "1.2.0")
  new <- trimmer_archive_fixture(version = "1.3.0")
  on.exit(unlink(c(old, new)), add = TRUE)
  suppressMessages(edsan_install_trimmer(old))
  suppressMessages(edsan_install_trimmer(new))

  expect_setequal(
    list.dirs(cache, recursive = FALSE, full.names = FALSE),
    c("1.2.0", "1.3.0")
  )
})

test_that("the success message says when another artifact stays selected", {
  local_trimmer_cache()
  new <- trimmer_archive_fixture(version = "1.3.0")
  old <- trimmer_archive_fixture(version = "1.2.0")
  on.exit(unlink(c(old, new)), add = TRUE)
  suppressMessages(edsan_install_trimmer(new))

  msg <- testthat::capture_messages(edsan_install_trimmer(old))

  expect_match(
    paste(msg, collapse = ""),
    "Selected:  no, 1.3.0 is selected instead (highest installed version)",
    fixed = TRUE
  )
})

test_that("edsan_install_trimmer preserves an existing install on invalid input", {
  cache <- local_trimmer_cache()
  good <- trimmer_archive_fixture(version = "1.2.0")
  on.exit(unlink(good), add = TRUE)
  suppressMessages(edsan_install_trimmer(good))

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

  expect_error(
    edsan_install_trimmer(zip_file),
    "missing required files: tokenizer.json, trim_batch_service.py, artifact.json"
  )
  expect_identical(edsan_trimmer_versions()$version, "1.2.0")
  expect_identical(list.dirs(cache, recursive = FALSE, full.names = FALSE), "1.2.0")
})

test_that("edsan_install_trimmer rejects incompatible worker contracts", {
  local_trimmer_cache()
  zip_file <- trimmer_archive_fixture(
    manifest = '{"artifact_version":"1.2.0","worker_contract":"rectype-aware-v1"}'
  )
  on.exit(unlink(zip_file), add = TRUE)

  expect_error(
    edsan_install_trimmer(zip_file),
    "requires artifact_version >= 1.2.0 and worker_contract 'model-only-v1'"
  )
})

test_that("edsan_install_trimmer rejects malformed manifests", {
  local_trimmer_cache()
  zip_file <- trimmer_archive_fixture(manifest = "{not-json")
  on.exit(unlink(zip_file), add = TRUE)

  expect_error(
    edsan_install_trimmer(zip_file),
    "requires artifact_version >= 1.2.0 and worker_contract 'model-only-v1'"
  )
})

test_that("edsan_install_trimmer rejects versions that cannot name a folder", {
  cache <- local_trimmer_cache()
  zip_file <- trimmer_archive_fixture(version = "1.3-0")
  on.exit(unlink(zip_file), add = TRUE)

  expect_error(
    edsan_install_trimmer(zip_file),
    "artifact_version '1.3-0' must be dotted numbers"
  )
  expect_identical(list.dirs(cache, recursive = FALSE, full.names = FALSE), character())
})

test_that("the folder-name rule applies to installs, not to named artifacts", {
  artifact <- tempfile("trimmer_dev_")
  dir.create(artifact)
  on.exit(unlink(artifact, recursive = TRUE), add = TRUE)
  for (f in c("model.onnx", "tokenizer.json", "trim_batch_service.py")) {
    writeLines("x", file.path(artifact, f))
  }
  writeLines(
    '{"artifact_version":"1.3-0","worker_contract":"model-only-v1"}',
    file.path(artifact, "artifact.json")
  )
  expect_identical(redsan:::.doceds_onnx_validate_artifact(artifact), "1.3-0")
})

test_that("an invalid existing install folder is replaced without overwrite", {
  cache <- local_trimmer_cache()
  broken <- file.path(cache, "1.3.0")
  dir.create(broken, recursive = TRUE)
  writeLines("half-copied", file.path(broken, "model.onnx"))
  zip_file <- trimmer_archive_fixture(version = "1.3.0", model = "good weights")
  on.exit(unlink(zip_file), add = TRUE)

  msg <- testthat::capture_messages(edsan_install_trimmer(zip_file))

  expect_match(paste(msg, collapse = ""), "not a valid installation; replacing it")
  expect_identical(
    readLines(file.path(broken, "model.onnx"), warn = FALSE),
    "good weights"
  )
})

test_that("the install message names why discovery fails", {
  local_trimmer_cache()
  zip_file <- trimmer_archive_fixture(version = "1.3.0")
  on.exit(unlink(zip_file), add = TRUE)
  withr::local_envvar(EDSAN_TRIMMER_PATH = tempfile("missing_"))

  msg <- testthat::capture_messages(edsan_install_trimmer(zip_file))

  expect_match(
    paste(msg, collapse = ""),
    "Selected:  no, discovery fails: EDSAN_TRIMMER_PATH is set to"
  )
})

test_that("edsan_install_trimmer accepts one nested artifact root", {
  cache <- local_trimmer_cache()
  zip_file <- trimmer_archive_fixture(nested = TRUE)
  on.exit(unlink(zip_file), add = TRUE)

  suppressMessages(edsan_install_trimmer(zip_file))

  target <- file.path(cache, "1.2.0")
  expect_true(file.exists(file.path(target, "model.onnx")))
  expect_true(file.exists(file.path(target, "artifact.json")))
  expect_false(dir.exists(file.path(target, "release")))
})

test_that("an overwriting install restores the previous one when publish fails", {
  cache <- local_trimmer_cache()
  original <- trimmer_archive_fixture(version = "1.2.0", model = "original weights")
  rebuilt <- trimmer_archive_fixture(version = "1.2.0", model = "rebuilt weights")
  on.exit(unlink(c(original, rebuilt)), add = TRUE)
  suppressMessages(edsan_install_trimmer(original))
  testthat::local_mocked_bindings(
    .doceds_onnx_publish_artifact = function(...) FALSE,
    .package = "redsan"
  )

  expect_error(
    edsan_install_trimmer(rebuilt, overwrite = TRUE),
    "Could not publish the validated trimmer artifact"
  )
  expect_identical(
    readLines(file.path(cache, "1.2.0", "model.onnx"), warn = FALSE),
    "original weights"
  )
  expect_identical(edsan_trimmer_versions()$version, "1.2.0")
})

test_that("a failed first install leaves no version folder behind", {
  cache <- local_trimmer_cache()
  zip_file <- trimmer_archive_fixture(version = "1.2.0")
  on.exit(unlink(zip_file), add = TRUE)
  testthat::local_mocked_bindings(
    .doceds_onnx_publish_artifact = function(...) FALSE,
    .package = "redsan"
  )

  expect_error(
    edsan_install_trimmer(zip_file),
    "Could not publish the validated trimmer artifact"
  )
  expect_identical(list.dirs(cache, recursive = FALSE, full.names = FALSE), character())
})

test_that("edsan_trimmer_cache_dir returns the cache root without a layout suffix", {
  cache_dir <- edsan_trimmer_cache_dir()
  expect_true(is.character(cache_dir))
  expect_true(nzchar(cache_dir))
  expect_true(grepl("edsan_doc_trimmer", cache_dir))
  expect_identical(basename(cache_dir), "edsan_doc_trimmer")

  redirected <- local_trimmer_cache()
  expect_identical(edsan_trimmer_cache_dir(), redirected)
})

test_that("edsan_install_trimmer rejects directory paths with guidance", {
  local_trimmer_cache()
  tmp_dir <- tempfile("not_a_zip_")
  dir.create(tmp_dir)
  on.exit(unlink(tmp_dir, recursive = TRUE), add = TRUE)

  expect_error(
    edsan_install_trimmer(tmp_dir),
    "Specified path is a directory, not a zip archive"
  )
})

test_that("edsan_install_trimmer detects zip missing model.onnx", {
  cache <- local_trimmer_cache()
  tmp_zip_dir <- tempfile("wrong_zip_src_")
  dir.create(tmp_zip_dir)
  on.exit(unlink(tmp_zip_dir, recursive = TRUE), add = TRUE)

  # Zip without model.onnx
  fake_readme <- file.path(tmp_zip_dir, "README.md")
  writeLines("not a model", fake_readme)
  zip_file <- tempfile(fileext = ".zip")
  on.exit(unlink(zip_file), add = TRUE)
  utils::zip(zip_file, files = fake_readme, flags = "-j")

  expect_error(
    edsan_install_trimmer(zip_file),
    "Invalid trimmer archive: 'model.onnx' not found after extraction"
  )
  expect_identical(list.dirs(cache, recursive = FALSE, full.names = FALSE), character())
})

test_that(".edsan_get_trimmer_dir unwraps direct model.onnx file path", {
  local_trimmer_cache()
  tmp_model_dir <- tempfile("model_dir_")
  dir.create(tmp_model_dir)
  on.exit(unlink(tmp_model_dir, recursive = TRUE), add = TRUE)

  model_file <- file.path(tmp_model_dir, "model.onnx")
  writeLines("dummy", model_file)

  # User sets path directly to model.onnx file instead of directory
  withr::local_envvar(EDSAN_TRIMMER_PATH = model_file)

  resolved <- redsan:::.edsan_get_trimmer_dir()
  expect_identical(normalizePath(resolved), normalizePath(tmp_model_dir))
})

test_that("discovery picks the highest installed version", {
  cache <- local_trimmer_cache()
  install_trimmer_versions(c("1.2.0", "1.10.0", "1.3.0"))

  expect_identical(
    normalizePath(redsan:::.edsan_get_trimmer_dir()),
    normalizePath(file.path(cache, "1.10.0"))
  )
  versions <- edsan_trimmer_versions()
  expect_identical(versions$version, c("1.10.0", "1.3.0", "1.2.0"))
  expect_identical(versions$selected, c(TRUE, FALSE, FALSE))
  expect_named(versions, c("version", "path", "selected"))
})

test_that("EDSAN_TRIMMER_VERSION pins an installed version", {
  cache <- local_trimmer_cache()
  install_trimmer_versions(c("1.2.0", "1.3.0"))
  withr::local_envvar(EDSAN_TRIMMER_VERSION = "1.2.0")

  expect_identical(
    normalizePath(redsan:::.edsan_get_trimmer_dir()),
    normalizePath(file.path(cache, "1.2.0"))
  )
  expect_identical(edsan_trimmer_versions()$selected, c(FALSE, TRUE))
})

test_that("an unknown EDSAN_TRIMMER_VERSION errors and lists installed versions", {
  local_trimmer_cache()
  install_trimmer_versions(c("1.2.0", "1.3.0"))
  withr::local_envvar(EDSAN_TRIMMER_VERSION = "9.9.9")

  expect_error(
    redsan:::.edsan_get_trimmer_dir(),
    "EDSAN_TRIMMER_VERSION is '9.9.9'.*Installed versions: 1.3.0, 1.2.0"
  )
  # Listing must stay usable exactly when selection is broken.
  versions <- edsan_trimmer_versions()
  expect_identical(versions$version, c("1.3.0", "1.2.0"))
  expect_identical(versions$selected, c(FALSE, FALSE))
})

test_that("EDSAN_TRIMMER_PATH wins over the pin and the cache", {
  local_trimmer_cache()
  install_trimmer_versions(c("1.2.0", "1.3.0"))
  external <- trimmer_protocol_fixture()$model_dir
  withr::local_envvar(
    EDSAN_TRIMMER_PATH = external,
    EDSAN_TRIMMER_VERSION = "1.2.0"
  )

  expect_identical(
    normalizePath(redsan:::.edsan_get_trimmer_dir()),
    normalizePath(external)
  )
  expect_identical(edsan_trimmer_versions()$selected, c(FALSE, FALSE))
})

test_that("an EDSAN_TRIMMER_PATH without model.onnx is an error, not a fallback", {
  local_trimmer_cache()
  install_trimmer_versions("1.3.0")
  empty <- withr::local_tempdir()
  withr::local_envvar(EDSAN_TRIMMER_PATH = empty)

  expect_error(
    redsan:::.edsan_get_trimmer_dir(),
    "EDSAN_TRIMMER_PATH is set to .*'model.onnx' was not found"
  )
  # Listing stays usable and selects nothing.
  expect_identical(edsan_trimmer_versions()$selected, FALSE)
})

test_that("a whitespace-only EDSAN_TRIMMER_PATH counts as unset", {
  local_trimmer_cache()
  install_trimmer_versions("1.3.0")
  withr::local_envvar(EDSAN_TRIMMER_PATH = "   ")

  expect_match(redsan:::.edsan_get_trimmer_dir(), "1\\.3\\.0$")
})

test_that("the legacy REDSAN_TRIMMER_PATH is used only when EDSAN_TRIMMER_PATH is unset", {
  local_trimmer_cache()
  legacy <- trimmer_protocol_fixture()$model_dir
  current <- trimmer_protocol_fixture()$model_dir

  withr::local_envvar(REDSAN_TRIMMER_PATH = legacy)
  expect_identical(
    normalizePath(redsan:::.edsan_get_trimmer_dir()),
    normalizePath(legacy)
  )

  withr::local_envvar(EDSAN_TRIMMER_PATH = current)
  expect_identical(
    normalizePath(redsan:::.edsan_get_trimmer_dir()),
    normalizePath(current)
  )
})

test_that("folders that do not match their manifest version are ignored", {
  cache <- local_trimmer_cache()
  install_trimmer_versions("1.2.0")
  # Name is not a bare version.
  write_trimmer_artifact(file.path(cache, "v1.3"), version = "1.3")
  # Name is a version, but the manifest says something else.
  write_trimmer_artifact(file.path(cache, "1.4.0"), version = "1.3.0")
  # Name is a version, but the artifact is incomplete.
  dir.create(file.path(cache, "1.5.0"))
  writeLines("dummy", file.path(cache, "1.5.0", "model.onnx"))

  expect_identical(edsan_trimmer_versions()$version, "1.2.0")
  expect_identical(
    normalizePath(redsan:::.edsan_get_trimmer_dir()),
    normalizePath(file.path(cache, "1.2.0"))
  )
})

test_that("a valid legacy v1 slot is used with a reinstall message", {
  cache <- local_trimmer_cache()
  write_trimmer_artifact(file.path(cache, "v1"), version = "1.2.0")

  expect_message(
    resolved <- redsan:::.edsan_get_trimmer_dir(),
    "legacy trimmer install.*Reinstall"
  )
  expect_identical(
    normalizePath(resolved),
    normalizePath(file.path(cache, "v1"))
  )
  expect_identical(nrow(edsan_trimmer_versions()), 0L)
})

test_that("a versioned install takes precedence over the legacy v1 slot", {
  cache <- local_trimmer_cache()
  write_trimmer_artifact(file.path(cache, "v1"), version = "1.2.0")
  install_trimmer_versions("1.3.0")

  expect_no_message(resolved <- redsan:::.edsan_get_trimmer_dir())
  expect_identical(
    normalizePath(resolved),
    normalizePath(file.path(cache, "1.3.0"))
  )
})

test_that("an empty cache errors with install instructions and no checkout fallback", {
  cache <- local_trimmer_cache()

  expect_error(
    redsan:::.edsan_get_trimmer_dir(),
    "edsan-doc-trimmer model not found.*edsan_install_trimmer"
  )
  expect_identical(nrow(edsan_trimmer_versions()), 0L)
  expect_named(edsan_trimmer_versions(), c("version", "path", "selected"))

  # A development-checkout export next to the working directory is never used
  # unless EDSAN_TRIMMER_PATH names it.
  workspace <- withr::local_tempdir()
  project <- file.path(workspace, "project")
  dir.create(project)
  write_trimmer_artifact(
    file.path(workspace, "edsan-doc-trimmer", "artifacts", "active_learning", "onnx_export"),
    version = "1.2.0"
  )
  withr::local_dir(project)
  expect_error(redsan:::.edsan_get_trimmer_dir(), "model not found")
})

test_that("trimming reports the artifact once per top-level call", {
  fixture <- trimmer_protocol_fixture()
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)
  docs <- function(ids) {
    data.frame(
      ELTID = ids,
      RECTYPE = "CRH",
      RECTXT = paste("Text", ids),
      stringsAsFactors = FALSE
    )
  }
  bundle <- function(ids) {
    structure(list(sources = list(doceds = docs(ids))), class = "edsan_event_bundle")
  }
  run <- function(data) {
    testthat::capture_messages(
      trim_doceds_onnx(
        data,
        python_exe = fixture$runner,
        model_dir = fixture$model_dir
      )
    )
  }
  expected <- sprintf(
    "edsan-doc-trimmer 1.2.0 (%s)\n",
    normalizePath(fixture$model_dir)
  )

  expect_identical(run(c("one", "two")), expected)
  expect_identical(run(docs(c("a", "b"))), expected)
  expect_identical(run(bundle(c("a", "b"))), expected)
  expect_identical(run(list(bundle("a"), bundle("b"), bundle("c"))), expected)
  expect_identical(run(character(0)), character(0))
})

test_that("trimming without model_dir reports the discovered artifact", {
  cache <- local_trimmer_cache()
  fixture <- trimmer_protocol_fixture()
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)
  target <- file.path(cache, "1.2.0")
  dir.create(cache, recursive = TRUE)
  file.copy(fixture$model_dir, cache, recursive = TRUE)
  file.rename(file.path(cache, basename(fixture$model_dir)), target)

  msg <- testthat::capture_messages(
    trim_doceds_onnx("Clinical narrative", python_exe = fixture$runner)
  )

  expect_identical(
    msg,
    sprintf("edsan-doc-trimmer 1.2.0 (%s)\n", normalizePath(target))
  )
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
      "TRIM_REDUCTION_PCT", "TRIM_PRESERVED_INTERVALS",
      "TRIM_EXECUTION_PROVIDER"
    )
  )
})


test_that("the artifact spec identifies what produced a trimmed text", {
  model_dir <- trimmer_protocol_fixture()$model_dir
  spec <- doceds_onnx_spec(model_dir)

  expect_identical(spec$package, "redsan")
  expect_identical(spec$version, as.character(utils::packageVersion("redsan")))
  expect_identical(spec$path, normalizePath(model_dir))
  expect_match(spec$digest, "^[0-9a-f]{64}$")
  expect_identical(spec$digest_algorithm, "sha256")
  expect_identical(spec$digest_schema, "doceds-onnx-artifact-v1")
  expect_identical(spec$artifact_version, "1.2.0")
  expect_identical(spec$worker_contract, "model-only-v1")

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
  # spec exists for: the version still reads 1.2.0 and the digest does not.
  writeLines("different weights", file.path(model_dir, "model.onnx"))
  after <- doceds_onnx_spec(model_dir)
  expect_identical(after$artifact_version, "1.2.0")
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


test_that("selected execution provider is recorded in table output", {
  fixture <- trimmer_protocol_fixture(
    "results[[1L]]$execution_provider <- 'CUDAExecutionProvider'"
  )
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  result <- trim_doceds_onnx(
    data.frame(RECTXT = "Clinical narrative", stringsAsFactors = FALSE),
    python_exe = fixture$runner,
    model_dir = fixture$model_dir
  )

  expect_identical(result$TRIM_EXECUTION_PROVIDER, "CUDAExecutionProvider")
})

test_that("marked runtime notices are surfaced from successful worker runs", {
  fixture <- trimmer_protocol_fixture(c(
    "writeLines('EDSAN_TRIMMER_NOTICE:WARNING:CUDA_PROVIDER_MISSING:Install onnxruntime-gpu', stderr())",
    "writeLines('unmarked worker diagnostic', stderr())"
  ))
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  expect_warning(
    trim_doceds_onnx(
      data.frame(RECTXT = "Clinical narrative", stringsAsFactors = FALSE),
      python_exe = fixture$runner,
      model_dir = fixture$model_dir
    ),
    "Install onnxruntime-gpu"
  )
})


test_that("older compatible workers without provider metadata yield NA", {
  fixture <- trimmer_protocol_fixture(
    "results[[1L]]$execution_provider <- NULL"
  )
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  result <- trim_doceds_onnx(
    data.frame(RECTXT = "Clinical narrative", stringsAsFactors = FALSE),
    python_exe = fixture$runner,
    model_dir = fixture$model_dir
  )

  expect_true(is.na(result$TRIM_EXECUTION_PROVIDER))
})

test_that("unmarked successful-worker stderr remains hidden", {
  fixture <- trimmer_protocol_fixture(
    "writeLines('unmarked worker diagnostic', stderr())"
  )
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  msg <- testthat::capture_messages(
    trim_doceds_onnx(
      data.frame(RECTXT = "Clinical narrative", stringsAsFactors = FALSE),
      python_exe = fixture$runner,
      model_dir = fixture$model_dir
    )
  )
  # Only the artifact announcement; the unmarked diagnostic stays hidden.
  expect_length(msg, 1L)
  expect_match(msg, "^edsan-doc-trimmer 1\\.2\\.0 \\(")
})


# --- chunking, checkpoints and progress ------------------------------------

# Wrap the real worker so tests can count how many worker runs a call made.
count_worker_runs <- function(env = parent.frame()) {
  counter <- new.env()
  counter$n <- 0L
  original <- .doceds_onnx_run_worker
  testthat::local_mocked_bindings(
    .doceds_onnx_run_worker = function(...) {
      counter$n <- counter$n + 1L
      original(...)
    },
    .env = env
  )
  counter
}

chunk_texts <- c("alpha note", "beta note", "gamma note", "delta note", "epsilon note")

chunk_bundle <- function(texts, prefix) {
  structure(
    list(sources = list(doceds = data.frame(
      ELTID = sprintf("%s%d", rep(prefix, length(texts)), seq_along(texts)),
      RECTYPE = rep("CRH2AB", length(texts)),
      RECTXT = texts,
      stringsAsFactors = FALSE
    ))),
    class = "edsan_event_bundle"
  )
}

test_that("chunking calls the worker once per chunk and does not change results", {
  fixture <- trimmer_protocol_fixture()
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)
  runs <- count_worker_runs()

  trim <- function(data, ...) {
    trim_doceds_onnx(
      data,
      python_exe = fixture$runner,
      model_dir = fixture$model_dir,
      progress = FALSE,
      ...
    )
  }
  inputs <- list(
    character = setNames(chunk_texts, letters[1:5]),
    table = data.frame(ELTID = paste0("e", 1:5), RECTXT = chunk_texts, stringsAsFactors = FALSE),
    bundle = chunk_bundle(chunk_texts, "b"),
    cohort = list(
      one = chunk_bundle(chunk_texts[1:2], "x"),
      empty = chunk_bundle(character(), "y"),
      two = chunk_bundle(chunk_texts[3:5], "z")
    )
  )

  for (name in names(inputs)) {
    runs$n <- 0L
    single <- trim(inputs[[name]], chunk_size = 500)
    expect_identical(runs$n, 1L, label = paste(name, "single-chunk runs"))

    runs$n <- 0L
    chunked <- trim(inputs[[name]], chunk_size = 2)
    expect_identical(runs$n, 3L, label = paste(name, "chunked runs"))
    expect_identical(chunked, single, label = paste(name, "result"))
  }
})

test_that("chunk_size, checkpoint_dir and progress are validated", {
  expect_error(trim_doceds_onnx("x", chunk_size = 0), "chunk_size")
  expect_error(trim_doceds_onnx("x", chunk_size = 1.5), "chunk_size")
  expect_error(trim_doceds_onnx("x", chunk_size = c(1, 2)), "chunk_size")
  expect_error(trim_doceds_onnx("x", chunk_size = NA_real_), "chunk_size")
  expect_error(trim_doceds_onnx("x", checkpoint_dir = c("a", "b")), "checkpoint_dir")
  expect_error(trim_doceds_onnx("x", checkpoint_dir = ""), "checkpoint_dir")
  expect_error(trim_doceds_onnx("x", progress = NA), "progress")
  expect_identical(formals(trim_doceds_onnx)$chunk_size, 500L)
  expect_null(formals(trim_doceds_onnx)$checkpoint_dir)
  expect_identical(formals(trim_doceds_onnx)$progress, quote(interactive()))
})

test_that("rerunning with the same checkpoint_dir reloads finished chunks", {
  fixture <- trimmer_protocol_fixture()
  checkpoints <- tempfile("trim_checkpoints_")
  on.exit(unlink(c(fixture$model_dir, checkpoints), recursive = TRUE), add = TRUE)
  runs <- count_worker_runs()
  trim <- function(texts = chunk_texts) {
    trim_doceds_onnx(
      texts,
      python_exe = fixture$runner,
      model_dir = fixture$model_dir,
      chunk_size = 2,
      checkpoint_dir = checkpoints,
      progress = FALSE
    )
  }

  first <- trim()
  expect_identical(runs$n, 3L)
  files <- list.files(checkpoints, full.names = TRUE)
  expect_length(files, 3L)

  runs$n <- 0L
  expect_identical(trim(), first)
  expect_identical(runs$n, 0L)

  runs$n <- 0L
  file.remove(files[[2L]])
  expect_identical(trim(), first)
  expect_identical(runs$n, 1L)
  expect_length(list.files(checkpoints), 3L)

  # A changed text invalidates only the chunk that holds it.
  runs$n <- 0L
  changed <- chunk_texts
  changed[[5L]] <- "epsilon note, revised"
  trim(changed)
  expect_identical(runs$n, 1L)
})

test_that("a changed artifact digest invalidates every checkpoint", {
  fixture <- trimmer_protocol_fixture()
  checkpoints <- tempfile("trim_checkpoints_")
  on.exit(unlink(c(fixture$model_dir, checkpoints), recursive = TRUE), add = TRUE)
  runs <- count_worker_runs()
  trim <- function() {
    trim_doceds_onnx(
      chunk_texts,
      python_exe = fixture$runner,
      model_dir = fixture$model_dir,
      chunk_size = 2,
      checkpoint_dir = checkpoints,
      progress = FALSE
    )
  }

  trim()
  writeLines("different weights", file.path(fixture$model_dir, "model.onnx"))
  runs$n <- 0L
  trim()
  expect_identical(runs$n, 3L)
})

test_that("an unreadable checkpoint is recomputed and replaced", {
  fixture <- trimmer_protocol_fixture()
  checkpoints <- tempfile("trim_checkpoints_")
  on.exit(unlink(c(fixture$model_dir, checkpoints), recursive = TRUE), add = TRUE)
  runs <- count_worker_runs()
  trim <- function() {
    trim_doceds_onnx(
      chunk_texts,
      python_exe = fixture$runner,
      model_dir = fixture$model_dir,
      chunk_size = 5,
      checkpoint_dir = checkpoints,
      progress = FALSE
    )
  }

  first <- trim()
  writeLines("not an rds file", list.files(checkpoints, full.names = TRUE))
  runs$n <- 0L
  expect_identical(trim(), first)
  expect_identical(runs$n, 1L)
  runs$n <- 0L
  trim()
  expect_identical(runs$n, 0L)
})

test_that("a chunk that fails validation is not checkpointed and names its document", {
  fixture <- trimmer_protocol_fixture(c(
    "results <- lapply(results, function(r) {",
    "  if (identical(r$id, 'doc_3')) r$reduction_pct <- 'bad'",
    "  r",
    "})"
  ))
  checkpoints <- tempfile("trim_checkpoints_")
  on.exit(unlink(c(fixture$model_dir, checkpoints), recursive = TRUE), add = TRUE)
  runs <- count_worker_runs()

  expect_error(
    trim_doceds_onnx(
      chunk_texts,
      python_exe = fixture$runner,
      model_dir = fixture$model_dir,
      chunk_size = 2,
      checkpoint_dir = checkpoints,
      progress = FALSE
    ),
    "invalid result for document doc_3"
  )
  # Chunk 1 was good and kept; chunk 2 failed and stopped the run.
  expect_length(list.files(checkpoints), 1L)
  expect_identical(runs$n, 2L)
})

test_that("a cohort validation error names the stay and ELTID", {
  fixture <- trimmer_protocol_fixture(c(
    "results <- lapply(results, function(r) {",
    "  if (identical(r$id, 'two/z2')) r$reduction_pct <- 'bad'",
    "  r",
    "})"
  ))
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)
  cohort <- list(
    one = chunk_bundle(chunk_texts[1:2], "x"),
    chunk_bundle(chunk_texts[3], "y"),
    two = chunk_bundle(chunk_texts[4:5], "z")
  )

  expect_error(
    trim_doceds_onnx(
      cohort,
      python_exe = fixture$runner,
      model_dir = fixture$model_dir,
      progress = FALSE
    ),
    "invalid result for document two/z2",
    fixed = TRUE
  )
  expect_identical(
    redsan:::.doceds_onnx_cohort_ids(cohort[[2L]]$sources$doceds, "", 2L),
    "#2/y1"
  )
  no_eltid <- data.frame(RECTXT = c("a", "b"), stringsAsFactors = FALSE)
  expect_identical(
    redsan:::.doceds_onnx_cohort_ids(no_eltid, NULL, 4L),
    c("#4/row 1", "#4/row 2")
  )
})

test_that("changing EDSAN_TRIMMER_DEVICE recomputes instead of mixing providers", {
  fixture <- trimmer_protocol_fixture(c(
    "results <- lapply(results, function(r) {",
    "  if (Sys.getenv('FAKE_PROVIDER') == 'cuda') r$execution_provider <- 'CUDAExecutionProvider'",
    "  r",
    "})"
  ))
  checkpoints <- tempfile("trim_checkpoints_")
  on.exit(unlink(c(fixture$model_dir, checkpoints), recursive = TRUE), add = TRUE)
  runs <- count_worker_runs()
  trim <- function() {
    trim_doceds_onnx(
      chunk_texts,
      python_exe = fixture$runner,
      model_dir = fixture$model_dir,
      chunk_size = 2,
      checkpoint_dir = checkpoints,
      progress = FALSE
    )
  }

  withr::with_envvar(c(EDSAN_TRIMMER_DEVICE = "cpu", FAKE_PROVIDER = "cpu"), trim())
  runs$n <- 0L
  result <- withr::with_envvar(
    c(EDSAN_TRIMMER_DEVICE = "cuda", FAKE_PROVIDER = "cuda"),
    trim()
  )
  expect_identical(runs$n, 3L)
  expect_identical(attr(result, "TRIM_EXECUTION_PROVIDER"), "CUDAExecutionProvider")

  # Under `auto` the key cannot see a provider change; the error names the cause.
  unlink(checkpoints, recursive = TRUE)
  withr::with_envvar(c(EDSAN_TRIMMER_DEVICE = NA, FAKE_PROVIDER = "cpu"), trim())
  file.remove(list.files(checkpoints, full.names = TRUE)[[1L]])
  expect_error(
    withr::with_envvar(c(EDSAN_TRIMMER_DEVICE = NA, FAKE_PROVIDER = "cuda"), trim()),
    "reloaded from checkpoints"
  )
})

test_that("execution provider must match the first chunk", {
  fixture <- trimmer_protocol_fixture(c(
    "results <- lapply(results, function(r) {",
    "  if (r$id %in% c('doc_3', 'doc_4')) r$execution_provider <- 'CUDAExecutionProvider'",
    "  r",
    "})"
  ))
  checkpoints <- tempfile("trim_checkpoints_")
  on.exit(unlink(c(fixture$model_dir, checkpoints), recursive = TRUE), add = TRUE)

  expect_error(
    trim_doceds_onnx(
      chunk_texts,
      python_exe = fixture$runner,
      model_dir = fixture$model_dir,
      chunk_size = 2,
      checkpoint_dir = checkpoints,
      progress = FALSE
    ),
    "inconsistent execution provider"
  )
  expect_length(list.files(checkpoints), 1L)
})

test_that("progress prints one line per chunk only when asked", {
  fixture <- trimmer_protocol_fixture()
  checkpoints <- tempfile("trim_checkpoints_")
  on.exit(unlink(c(fixture$model_dir, checkpoints), recursive = TRUE), add = TRUE)
  trim <- function(...) {
    trim_doceds_onnx(
      chunk_texts,
      python_exe = fixture$runner,
      model_dir = fixture$model_dir,
      chunk_size = 2,
      ...
    )
  }
  # The once-per-call artifact announcement is not a progress line.
  collect_messages <- function(expr) {
    lines <- character()
    withCallingHandlers(
      expr,
      message = function(m) {
        lines <<- c(lines, conditionMessage(m))
        invokeRestart("muffleMessage")
      }
    )
    lines[!grepl("^edsan-doc-trimmer ", lines)]
  }

  lines <- collect_messages(trim(progress = TRUE, checkpoint_dir = checkpoints))
  expect_length(lines, 3L)
  expect_match(
    lines[[1L]],
    "chunk 1/3: 2/5 documents \\(40%\\), elapsed [0-9:]+, ETA [0-9:]+"
  )
  expect_match(
    lines[[3L]],
    "chunk 3/3: 5/5 documents \\(100%\\), elapsed [0-9:]+, ETA 0:00:00"
  )
  expect_false(any(grepl("checkpoint", lines)))

  lines <- collect_messages(trim(progress = TRUE, checkpoint_dir = checkpoints))
  expect_length(lines, 3L)
  expect_true(all(grepl("[checkpoint]", lines, fixed = TRUE)))

  expect_length(collect_messages(trim(progress = FALSE)), 0L)
  # Non-interactive test sessions: the default is off.
  expect_false(interactive())
  expect_length(collect_messages(trim()), 0L)
})

test_that("worker notices are surfaced once per call, not once per chunk", {
  fixture <- trimmer_protocol_fixture(
    "writeLines('EDSAN_TRIMMER_NOTICE:WARNING:CUDA_PROVIDER_MISSING:Install onnxruntime-gpu', stderr())"
  )
  on.exit(unlink(fixture$model_dir, recursive = TRUE), add = TRUE)

  warnings <- character()
  withCallingHandlers(
    trim_doceds_onnx(
      chunk_texts,
      python_exe = fixture$runner,
      model_dir = fixture$model_dir,
      chunk_size = 2,
      progress = FALSE
    ),
    warning = function(w) {
      warnings <<- c(warnings, conditionMessage(w))
      invokeRestart("muffleWarning")
    }
  )
  expect_length(warnings, 1L)
})

test_that("empty inputs never spawn a worker, even with checkpoints and progress", {
  checkpoints <- tempfile("trim_checkpoints_")
  testthat::local_mocked_bindings(
    .doceds_onnx_run_worker = function(...) stop("worker must not run")
  )

  expect_silent(trim_doceds_onnx(
    character(0),
    checkpoint_dir = checkpoints,
    progress = TRUE
  ))
  empty <- data.frame(RECTXT = character(), stringsAsFactors = FALSE)
  expect_equal(
    nrow(trim_doceds_onnx(empty, checkpoint_dir = checkpoints, progress = TRUE)),
    0L
  )
  expect_false(dir.exists(checkpoints))
})
