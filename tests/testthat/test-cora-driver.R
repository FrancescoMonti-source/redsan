test_that("CORA JDBC driver honors explicit environment override", {
  old <- Sys.getenv("REDSAN_OJDBC_JAR", unset = NA_character_)
  on.exit({
    if (is.na(old)) {
      Sys.unsetenv("REDSAN_OJDBC_JAR")
    } else {
      Sys.setenv(REDSAN_OJDBC_JAR = old)
    }
  }, add = TRUE)

  jar <- tempfile(fileext = ".jar")
  file.create(jar)
  on.exit(unlink(jar), add = TRUE)

  Sys.setenv(REDSAN_OJDBC_JAR = jar)

  expect_identical(
    redsan:::.cora_default_ojdbc(),
    normalizePath(jar, winslash = "/", mustWork = TRUE)
  )
})
