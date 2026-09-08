test_that("CORA validation accepts reads and rejects unsafe SQL", {
  expect_identical(
    redsan:::.cora_validate_read_query("SELECT * FROM CORA_REC.TEST_TABLE;"),
    "SELECT * FROM CORA_REC.TEST_TABLE"
  )

  expect_identical(
    redsan:::.cora_validate_read_query(
      "WITH x AS (SELECT 1 AS n FROM dual) SELECT * FROM x"
    ),
    "WITH x AS (SELECT 1 AS n FROM dual) SELECT * FROM x"
  )

  expect_identical(
    redsan:::.cora_validate_read_query(
      "SELECT 'DELETE FROM CORA_REC.X' AS txt FROM dual"
    ),
    "SELECT 'DELETE FROM CORA_REC.X' AS txt FROM dual"
  )

  expect_error(
    redsan:::.cora_validate_read_query("DELETE FROM CORA_REC.TEST_TABLE"),
    "read-only"
  )
  expect_error(
    redsan:::.cora_validate_read_query("SELECT 1 FROM dual; SELECT 2 FROM dual"),
    "exactly one"
  )
  expect_error(
    redsan:::.cora_validate_read_query("SELECT * FROM CORA_REC.TEST_TABLE FOR UPDATE"),
    "modify or lock"
  )
})

test_that("CORA query returns a tibble using a supplied connection", {
  disconnected <- FALSE
  connection <- structure(list(), class = "fake_connection")

  out <- redsan:::.cora_query(
    "SELECT USER AS session_user FROM dual",
    connection = connection,
    execute = function(connection, sql) {
      expect_s3_class(connection, "fake_connection")
      expect_identical(sql, "SELECT USER AS session_user FROM dual")
      data.frame(session_user = "EDSAN")
    },
    disconnect = function(connection) {
      disconnected <<- TRUE
    }
  )

  expect_s3_class(out, "tbl_df")
  expect_identical(out$session_user, "EDSAN")
  expect_false(disconnected)
})

test_that("CORA query owns and closes transient connections", {
  disconnected <- FALSE
  seen_jar <- NULL

  out <- redsan:::.cora_query(
    "SELECT 1 AS n FROM dual",
    ojdbc_jar = "/tmp/ojdbc.jar",
    connect = function(ojdbc_jar) {
      seen_jar <<- ojdbc_jar
      structure(list(), class = "fake_connection")
    },
    execute = function(connection, sql) {
      data.frame(n = 1L)
    },
    disconnect = function(connection) {
      disconnected <<- TRUE
    }
  )

  expect_identical(seen_jar, "/tmp/ojdbc.jar")
  expect_true(disconnected)
  expect_identical(out$n, 1L)
})

test_that("CORA backend errors are contextualized", {
  expect_error(
    redsan:::.cora_query(
      "SELECT * FROM CORA_REC.MISSING_TABLE",
      connection = structure(list(), class = "fake_connection"),
      execute = function(connection, sql) stop("ORA-00942: table or view does not exist")
    ),
    "CORA query failed: ORA-00942"
  )
})
