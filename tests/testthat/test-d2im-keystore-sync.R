test_that("D2IM sync adds only Python keys absent from R", {
  expect_identical(
    redsan:::.d2im_sync_missing_keys(
      c("db.a", "db.b", "db.b", "db.c"),
      c("db.a", "db.c")
    ),
    "db.b"
  )

  expect_identical(
    redsan:::.d2im_sync_missing_keys(
      c("db.a", "db.b"),
      character()
    ),
    c("db.a", "db.b")
  )

  expect_identical(
    redsan:::.d2im_sync_missing_keys(
      c("db.a", "db.b"),
      c("db.a", "db.b")
    ),
    character()
  )
})

test_that("D2IM sync Python resolution prefers a valid PYKERNEL", {
  old <- Sys.getenv("PYKERNEL", unset = NA_character_)
  on.exit({
    if (is.na(old)) {
      Sys.unsetenv("PYKERNEL")
    } else {
      Sys.setenv(PYKERNEL = old)
    }
  }, add = TRUE)

  fake_python <- tempfile()
  file.create(fake_python)
  on.exit(unlink(fake_python), add = TRUE)

  Sys.setenv(PYKERNEL = fake_python)
  expect_identical(redsan:::.d2im_sync_default_python(), fake_python)
})

test_that("D2IM sync Python resolution falls back when PYKERNEL is absent", {
  old <- Sys.getenv("PYKERNEL", unset = NA_character_)
  on.exit({
    if (is.na(old)) {
      Sys.unsetenv("PYKERNEL")
    } else {
      Sys.setenv(PYKERNEL = old)
    }
  }, add = TRUE)

  Sys.unsetenv("PYKERNEL")
  expect_identical(
    redsan:::.d2im_sync_default_python(),
    "/opt/kernels/py3.14/bin/python"
  )
})
