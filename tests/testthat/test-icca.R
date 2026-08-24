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
      "SELECT encounterId INTO #x FROM dbo.D_Encounter"
    ),
    "modify"
  )
  expect_error(
    redsan:::.icca_validate_read_query("SELECT 1; SELECT 2"),
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

test_that("ICCA query binds positional parameters", {
  seen <- NULL
  fake_execute <- function(connection, sql, params) {
    seen <<- list(connection = connection, sql = sql, params = params)
    data.frame(encounterId = c("E1", "E2"), stringsAsFactors = FALSE)
  }

  supplied_connection <- structure(list(), class = "fake_connection")
  out <- redsan:::.icca_query(
    "SELECT encounterId FROM dbo.D_Encounter WHERE encounterNumber IN (?, ?)",
    params = c("100", "200"),
    connection = supplied_connection,
    execute = fake_execute
  )

  expect_s3_class(out, "tbl_df")
  expect_identical(out$encounterId, c("E1", "E2"))
  expect_identical(seen$connection, supplied_connection)
  expect_identical(seen$params, list("100", "200"))
})

test_that("caller-owned ICCA connections are not disconnected", {
  disconnected <- FALSE
  supplied_connection <- structure(list(), class = "fake_connection")

  redsan:::.icca_query(
    "SELECT 1",
    connection = supplied_connection,
    execute = function(connection, sql, params) data.frame(value = 1L),
    disconnect = function(connection) disconnected <<- TRUE
  )

  expect_false(disconnected)
})

test_that("internally-created ICCA connections are always disconnected", {
  disconnected <- FALSE
  created_connection <- structure(list(), class = "fake_connection")

  out <- redsan:::.icca_query(
    "SELECT 1",
    connect = function() created_connection,
    execute = function(connection, sql, params) data.frame(value = 1L),
    disconnect = function(connection) disconnected <<- TRUE
  )

  expect_identical(out$value, 1L)
  expect_true(disconnected)
})

test_that("internally-created ICCA connections close after query errors", {
  disconnected <- FALSE
  created_connection <- structure(list(), class = "fake_connection")

  expect_error(
    redsan:::.icca_query(
      "SELECT 1",
      connect = function() created_connection,
      execute = function(connection, sql, params) stop("backend failed"),
      disconnect = function(connection) disconnected <<- TRUE
    ),
    "backend failed"
  )
  expect_true(disconnected)
})

test_that("ICCA query fails closed on invalid backend results", {
  expect_error(
    redsan:::.icca_query(
      "SELECT 1",
      connection = structure(list(), class = "fake_connection"),
      execute = function(connection, sql, params) NULL
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
})

test_that("ICCA connection rejects invalid keystore ports before DBI", {
  local_mocked_bindings(
    .icca_keystore_value = function(key) {
      switch(
        key,
        "db.iccaadu.srv" = "server",
        "db.iccaadu.port" = "not-a-port",
        "db.iccaadu.usr" = "user",
        "db.iccaadu.pwd" = "secret"
      )
    },
    .icca_require_namespace = function(package, purpose) TRUE,
    .package = "redsan"
  )

  expect_error(redsan:::.icca_connect(), "valid TCP port")
})
