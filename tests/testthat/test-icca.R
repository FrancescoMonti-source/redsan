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

test_that("ICCA EVTID validation is explicit and deduplicates requests", {
  expect_identical(
    redsan:::.icca_validate_evtids(c(" 357015848 ", "357015848", "123")),
    c("357015848", "123")
  )
  expect_error(redsan:::.icca_validate_evtids(357015848), "must be character")
  expect_error(redsan:::.icca_validate_evtids(c("357015848", "")), "empty")
})

test_that("empty ICCA EVTID input never contacts CT or SQL", {
  ct_called <- FALSE
  sql_called <- FALSE

  out <- redsan:::.icca_get_encounter(
    character(),
    reidentify = function(...) {
      ct_called <<- TRUE
      stop("should not be called")
    },
    query = function(...) {
      sql_called <<- TRUE
      stop("should not be called")
    }
  )

  expect_false(ct_called)
  expect_false(sql_called)
  expect_s3_class(out, "tbl_df")
  expect_identical(nrow(out), 0L)
  expect_identical(names(out)[[1L]], "EVTID")
})

test_that("ICCA encounter retrieval uses transient IEPs and returns EVTIDs", {
  seen <- NULL
  fake_reidentify <- function(ids, id_type, env, ks_path) {
    expect_identical(ids, c("EVT-1", "EVT-2"))
    expect_identical(id_type, "EVTID")
    tibble::tibble(
      EDSAN_ID = ids,
      EDSAN_TYPE = "EVTID",
      HIS_ID = c("IEP-1", "IEP-2"),
      HIS_TYPE = "IEP",
      status = "matched",
      n_matches = 1L
    )
  }
  fake_query <- function(sql, params, connection) {
    seen <<- list(sql = sql, params = params, connection = connection)
    data.frame(
      encounterId = c(11L, 22L),
      patientId = c(1L, 2L),
      episodeId = c(101L, 202L),
      encounterNumber = c("IEP-2", "IEP-1"),
      gender = c("F", "M"),
      primaryDiagnosis = c("A", "B"),
      isArchived = c(FALSE, TRUE),
      systemId = c(7L, 7L),
      stringsAsFactors = FALSE
    )
  }

  supplied_connection <- structure(list(), class = "fake_connection")
  out <- redsan:::.icca_get_encounter(
    c("EVT-1", "EVT-2"),
    connection = supplied_connection,
    reidentify = fake_reidentify,
    query = fake_query
  )

  expect_match(seen$sql, "D_Encounter", fixed = TRUE)
  expect_match(seen$sql, "encounterNumber IN (?, ?)", fixed = TRUE)
  expect_false(grepl("lifeTimeNumber|firstName|lastName|dateOfBirth|accountNumber",
                     seen$sql))
  expect_identical(seen$params, c("IEP-1", "IEP-2"))
  expect_identical(seen$connection, supplied_connection)
  expect_identical(out$EVTID, c("EVT-2", "EVT-1"))
  expect_false("encounterNumber" %in% names(out))
})

test_that("ICCA retrieval trusts multiple mappings returned by CT", {
  seen_params <- NULL
  fake_multiple <- function(ids, id_type, env, ks_path) {
    tibble::tibble(
      EDSAN_ID = c("EVT-1", "EVT-1"),
      EDSAN_TYPE = "EVTID",
      HIS_ID = c("IEP-1", "IEP-2"),
      HIS_TYPE = "IEP",
      status = "multiple_matches",
      n_matches = 2L
    )
  }
  fake_query <- function(sql, params, connection) {
    seen_params <<- params
    data.frame(
      encounterId = c(1L, 2L),
      patientId = c(10L, 10L),
      episodeId = c(100L, 200L),
      encounterNumber = c("IEP-1", "IEP-2"),
      gender = c("F", "F"),
      primaryDiagnosis = c("A", "B"),
      isArchived = c(FALSE, FALSE),
      systemId = c(7L, 7L),
      stringsAsFactors = FALSE
    )
  }

  out <- redsan:::.icca_get_encounter(
    "EVT-1",
    reidentify = fake_multiple,
    query = fake_query
  )

  expect_identical(seen_params, c("IEP-1", "IEP-2"))
  expect_identical(out$EVTID, c("EVT-1", "EVT-1"))
  expect_identical(out$encounterId, c(1L, 2L))
})

test_that("EVTIDs without CT mappings produce no ICCA query", {
  sql_called <- FALSE
  fake_missing <- function(ids, id_type, env, ks_path) {
    tibble::tibble(
      EDSAN_ID = ids,
      EDSAN_TYPE = "EVTID",
      HIS_ID = NA_character_,
      HIS_TYPE = "IEP",
      status = "not_found",
      n_matches = 0L
    )
  }

  out <- redsan:::.icca_get_encounter(
    "EVT-1",
    reidentify = fake_missing,
    query = function(...) {
      sql_called <<- TRUE
      stop("should not be called")
    }
  )

  expect_false(sql_called)
  expect_identical(nrow(out), 0L)
})
