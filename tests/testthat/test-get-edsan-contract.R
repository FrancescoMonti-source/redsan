test_that("module date keys prevent cross-source retrieval", {
  expect_error(
    get_edsan("pmsi", query = list(RECDATE = "{2024-01-01,2024-01-31}")),
    "pmsi module only supports DATENT and DATSORT"
  )
})

test_that("get_edsan forwards PMSI source_policy to normalization", {
  raw <- list(
    list(
      PATID = "P1", EVTID = "E1", ELTID = "L1",
      SEJUM = "U1", SEJUF = "F1", SRC = "DW"
    ),
    list(
      PATID = "P1", EVTID = "E1", ELTID = "L2",
      SEJUM = "U1", SEJUF = "F1", SRC = "C"
    )
  )
  testthat::local_mocked_bindings(
    .edsan_call = function(...) list(ok = TRUE, value = raw, error = NULL),
    .package = "redsan"
  )

  default <- get_edsan("pmsi")
  all_sources <- get_edsan("pmsi", source_policy = "all")

  expect_identical(
    list(default = default$main$ELTID, all = all_sources$main$ELTID),
    list(default = "L2", all = c("L1", "L2"))
  )
})

test_that("numeric identifier queries keep their non-scientific form", {
  observed_query <- NULL
  testthat::local_mocked_bindings(
    .edsan_call = function(module, query, what, fields = NULL, ...) {
      observed_query <<- query
      list(
        ok = TRUE,
        value = data.frame(
          PATID = numeric(),
          EVTID = numeric(),
          ELTID = numeric()
        ),
        error = NULL
      )
    },
    .package = "redsan"
  )

  get_edsan(
    "doceds",
    query = list(EVTID = c(100000, 200000))
  )

  expect_identical(observed_query$EVTID, "100000 OR 200000")
})

test_that("biology results returned in table form survive combination", {
  reference <- edsan_reference("bio")
  known_code <- reference$TYPEANA[[1L]]
  flat <- data.frame(
    PATID = "P1",
    EVTID = c("E1", "E1"),
    BIOL_ID = c("B1", "B2"),
    TYPEANA = c(known_code, "FAIT_MAISON"),
    NUMRES = c(4.2, 1),
    stringsAsFactors = FALSE
  )
  testthat::local_mocked_bindings(
    .edsan_call = function(...) list(ok = TRUE, value = flat, error = NULL),
    .package = "redsan"
  )

  out <- get_edsan("biol", query = list(EVTID = "E1"))

  expect_identical(nrow(out), 2L)
  expect_identical(out$BIOL_ID, c("B1", "B2"))
  expect_identical(
    out$TYPEANA_LABEL,
    c(reference$TYPEANA_LABEL[[1L]], NA_character_)
  )
})

test_that("biology results returned as exam lists still flatten across batches", {
  raw <- list(
    list(
      PATID = "P1", EVTID = "E1", ELTID = "L1",
      RESULTATS = data.frame(TYPEANA = "FAIT_MAISON", NUMRES = 1)
    )
  )
  testthat::local_mocked_bindings(
    .edsan_call = function(...) list(ok = TRUE, value = raw, error = NULL),
    .package = "redsan"
  )

  out <- get_edsan("biol", query = list(EVTID = "E1"))

  expect_identical(nrow(out), 1L)
  expect_identical(out$EVTID, "E1")
  expect_true("TYPEANA_LABEL" %in% names(out))
})
