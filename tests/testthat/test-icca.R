test_that("ICCA validation accepts reads and rejects writes", {
  expect_identical(
    redsan:::.icca_validate_read_query("SELECT TOP 0 * FROM dbo.D_Encounter;"),
    "SELECT TOP 0 * FROM dbo.D_Encounter"
  )
  expect_error(redsan:::.icca_validate_read_query("DELETE FROM dbo.D_Encounter"), "read-only")
  expect_error(redsan:::.icca_validate_read_query("SELECT 1; SELECT 2"), "exactly one")
})

test_that("ICCA query binds positional parameters", {
  seen <- NULL
  out <- redsan:::.icca_query(
    "SELECT encounterId FROM dbo.D_Encounter WHERE encounterNumber IN (?, ?)",
    params = c("100", "200"),
    connection = structure(list(), class = "fake_connection"),
    execute = function(connection, sql, params) {
      seen <<- params
      data.frame(encounterId = c(1L, 2L))
    }
  )
  expect_identical(seen, list("100", "200"))
  expect_identical(out$encounterId, c(1L, 2L))
})

test_that("empty EVTID input never contacts CT or SQL", {
  ct_called <- FALSE
  sql_called <- FALSE
  out <- redsan:::.icca_get_encounter(
    character(),
    reidentify = function(...) { ct_called <<- TRUE; stop("unexpected") },
    query = function(...) { sql_called <<- TRUE; stop("unexpected") }
  )
  expect_false(ct_called)
  expect_false(sql_called)
  expect_identical(nrow(out), 0L)
})

test_that("get_icca uses transient IEPs and returns EVTIDs", {
  seen <- NULL
  fake_reidentify <- function(ids, id_type, env, ks_path) {
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
    seen <<- list(sql = sql, params = params)
    data.frame(
      encounterId = c(11L, 22L),
      patientId = c(1L, 2L),
      episodeId = c(101L, 202L),
      encounterNumber = c("IEP-2", "IEP-1"),
      gender = c("F", "M"),
      primaryDiagnosis = c("A", "B"),
      isArchived = c(FALSE, TRUE),
      systemId = c(7L, 7L)
    )
  }
  out <- redsan:::.icca_get_encounter(
    c("EVT-1", "EVT-2"),
    reidentify = fake_reidentify,
    query = fake_query
  )
  expect_identical(seen$params, c("IEP-1", "IEP-2"))
  expect_false(grepl("lifeTimeNumber|firstName|lastName|dateOfBirth|accountNumber", seen$sql))
  expect_identical(out$EVTID, c("EVT-2", "EVT-1"))
  expect_false("encounterNumber" %in% names(out))
})

test_that("get_icca trusts multiple mappings returned by CT", {
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
  out <- redsan:::.icca_get_encounter(
    "EVT-1",
    reidentify = fake_multiple,
    query = function(sql, params, connection) {
      expect_identical(params, c("IEP-1", "IEP-2"))
      data.frame(
        encounterId = c(1L, 2L),
        patientId = c(10L, 10L),
        episodeId = c(100L, 200L),
        encounterNumber = c("IEP-1", "IEP-2"),
        gender = c("F", "F"),
        primaryDiagnosis = c("A", "B"),
        isArchived = c(FALSE, FALSE),
        systemId = c(7L, 7L)
      )
    }
  )
  expect_identical(out$EVTID, c("EVT-1", "EVT-1"))
})

test_that("missing CT mappings produce no SQL query", {
  sql_called <- FALSE
  out <- redsan:::.icca_get_encounter(
    "EVT-1",
    reidentify = function(ids, id_type, env, ks_path) {
      tibble::tibble(
        EDSAN_ID = ids,
        EDSAN_TYPE = "EVTID",
        HIS_ID = NA_character_,
        HIS_TYPE = "IEP",
        status = "not_found",
        n_matches = 0L
      )
    },
    query = function(...) { sql_called <<- TRUE; stop("unexpected") }
  )
  expect_false(sql_called)
  expect_identical(nrow(out), 0L)
})
