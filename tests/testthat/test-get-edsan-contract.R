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
