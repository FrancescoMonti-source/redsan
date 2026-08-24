test_that("ICCA validation accepts SELECT and CTE queries", {
  expect_identical(
    redsan:::.icca_validate_read_query("SELECT TOP 0 * FROM dbo.D_Encounter;"),
    "SELECT TOP 0 * FROM dbo.D_Encounter"
  )
  expect_identical(
    redsan:::.icca_validate_read_query(
      "WITH x AS (SELECT 1 AS n) SELECT n FROM x"
    ),
    "WITH x AS (SELECT 1 AS n) SELECT n FROM x"
  )
})

test_that("ICCA validation rejects writes and multiple statements", {
  expect_error(
    redsan:::.icca_validate_read_query("DELETE FROM dbo.D_Encounter"),
    "read-only"
  )
  expect_error(
    redsan:::.icca_validate_read_query(
      "SELECT encounterid INTO #x FROM dbo.D_Encounter"
    ),
    "modify"
  )
  expect_error(
    redsan:::.icca_validate_read_query(
      "SELECT 1; SELECT 2"
    ),
    "exactly one"
  )
})

test_that("write-like words inside literals and comments do not block reads", {
  expect_identical(
    redsan:::.icca_validate_read_query(
      "SELECT 'delete' AS label -- update is documentation"
    ),
    "SELECT 'delete' AS label -- update is documentation"
  )
})

test_that("ICCA query forwards positional parameters and keystore path", {
  seen <- NULL
  fake_backend <- function(sql, params, d2im_keystore_path) {
    seen <<- list(
      sql = sql,
      params = params,
      d2im_keystore_path = d2im_keystore_path
    )
    data.frame(encounterid = c("E1", "E2"), stringsAsFactors = FALSE)
  }

  out <- redsan:::.icca_query(
    "SELECT encounterid FROM dbo.D_Encounter WHERE encounternumber IN (%s, %s)",
    params = c("100", "200"),
    d2im_keystore_path = "/tmp/python-keystore",
    backend = fake_backend
  )

  expect_s3_class(out, "tbl_df")
  expect_identical(out$encounterid, c("E1", "E2"))
  expect_identical(seen$params, list("100", "200"))
  expect_identical(seen$d2im_keystore_path, "/tmp/python-keystore")
})

test_that("ICCA query fails closed on an invalid backend result", {
  expect_error(
    redsan:::.icca_query(
      "SELECT 1",
      backend = function(sql, params, d2im_keystore_path) NULL
    ),
    "must return a data frame"
  )
})

test_that("ICCA parameter validation remains explicit", {
  expect_null(redsan:::.icca_normalize_params(NULL))
  expect_identical(redsan:::.icca_normalize_params(1:2), list(1L, 2L))
  expect_error(
    redsan:::.icca_normalize_params(new.env(parent = emptyenv())),
    "atomic vector"
  )
  expect_error(
    redsan:::.icca_query(
      "SELECT 1",
      d2im_keystore_path = "",
      backend = function(...) data.frame()
    ),
    "non-empty path"
  )
})
