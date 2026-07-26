test_that("module date keys prevent cross-source retrieval", {
  # Rationale: retrieval contract. A date key belonging to another module must
  # be rejected locally; otherwise the backend could return a plausible but
  # incorrectly scoped extract.
  expect_error(
    get_edsan("pmsi", query = list(RECDATE = "{2024-01-01,2024-01-31}")),
    "pmsi module only supports DATENT and DATSORT"
  )
})

test_that("source_policy cannot be silently ignored outside processed PMSI data", {
  # Rationale: retrieval contract. A plausible PMSI policy on another module,
  # idtriplets, or raw output must fail before retrieval instead of appearing to
  # work while leaving the payload unchanged.
  message <- "`source_policy` is only valid for"

  expect_error(
    get_edsan("doceds", source_policy = "all"),
    message,
    fixed = TRUE
  )
  expect_error(
    get_edsan("pmsi", what = "idtriplets", source_policy = "all"),
    message,
    fixed = TRUE
  )
  expect_error(
    get_edsan("pmsi", process = FALSE, source_policy = "all"),
    message,
    fixed = TRUE
  )
})

test_that("get_edsan forwards PMSI source_policy to normalization", {
  # Rationale: retrieval contract. The public one-call workflow must preserve
  # an explicit request for all normalized main sources instead of silently
  # applying the new C-over-DW default.
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

  expect_identical(default$main$ELTID, "L2")
  expect_identical(all_sources$main$ELTID, c("L1", "L2"))
})

test_that("numeric identifier queries keep their non-scientific form", {
  # Rationale: retrieval contract. An opaque identifier must reach EDSAN with
  # the same lexical value used by normalized tables and event bundles.
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
