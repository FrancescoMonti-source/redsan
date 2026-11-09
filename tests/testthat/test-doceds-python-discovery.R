test_that("explicit Python override wins over auto-discovery", {
  python <- tempfile("python_")
  file.create(python)
  withr::local_envvar(REDSAN_PYTHON_PATH = python)
  testthat::local_mocked_bindings(
    .edsan_python_candidates = function() stop("auto-discovery must not run"),
    .edsan_python_probe = function(executable) list(ok = TRUE, reason = ""),
    .package = "redsan"
  )

  expect_identical(
    redsan:::.edsan_get_python_exe(),
    normalizePath(python, winslash = "/", mustWork = TRUE)
  )
})

test_that("auto-discovery skips incompatible interpreters", {
  first <- tempfile("py_first_")
  second <- tempfile("py_second_")
  file.create(first, second)
  withr::local_envvar(REDSAN_PYTHON_PATH = NA)
  checked <- character()
  testthat::local_mocked_bindings(
    .edsan_python_candidates = function() c(first, second),
    .edsan_python_probe = function(executable) {
      checked <<- c(checked, executable)
      if (identical(executable, normalizePath(first, winslash = "/", mustWork = TRUE))) {
        list(ok = FALSE, reason = "No module named 'onnxruntime'")
      } else {
        list(ok = TRUE, reason = "")
      }
    },
    .package = "redsan"
  )

  expect_identical(
    redsan:::.edsan_get_python_exe(),
    normalizePath(second, winslash = "/", mustWork = TRUE)
  )
  expect_length(checked, 2L)
})

test_that("Python discovery diagnostics distinguish missing dependencies", {
  first <- tempfile("py_first_")
  file.create(first)
  withr::local_envvar(REDSAN_PYTHON_PATH = NA)
  testthat::local_mocked_bindings(
    .edsan_python_candidates = function() c(first, "/missing/python"),
    .edsan_python_probe = function(executable) {
      list(ok = FALSE, reason = "No module named 'tokenizers'")
    },
    .package = "redsan"
  )
  expect_error(
    redsan:::.edsan_get_python_exe(),
    "No module named 'tokenizers'",
    fixed = TRUE
  )
})

test_that("an invalid explicit override is never silently ignored", {
  withr::local_envvar(REDSAN_PYTHON_PATH = "/missing/explicit/python")
  testthat::local_mocked_bindings(
    .edsan_python_candidates = function() stop("must not use fallback"),
    .package = "redsan"
  )
  expect_error(
    redsan:::.edsan_get_python_exe(),
    "REDSAN_PYTHON_PATH does not point",
    fixed = TRUE
  )
})

test_that("Windows per-user Python installs are discovery candidates", {
  if (.Platform$OS.type != "windows") {
    skip("Windows only")
  }
  localapp <- tempfile("localapp_")
  install_dir <- file.path(localapp, "Programs", "Python", "Python313")
  dir.create(install_dir, recursive = TRUE)
  file.create(file.path(install_dir, "python.exe"))
  withr::local_envvar(LOCALAPPDATA = localapp)
  testthat::local_mocked_bindings(
    .edsan_python_launcher_candidates = function() character(),
    .package = "redsan"
  )
  expect_true(
    file.path(install_dir, "python.exe") %in%
      redsan:::.edsan_python_candidates()
  )
})
