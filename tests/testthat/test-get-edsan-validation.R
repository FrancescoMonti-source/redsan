test_that("module date keys are validated before backend calls", {
  # Rationale: retrieval input contract. Date-key mistakes should fail locally,
  # before a live EDSAN request is made with the wrong module semantics.
  expect_error(
    get_edsan("doceds", query = list(DATEXAM = "{2024-01-01,2024-01-31}")),
    "doceds module only supports RECDATE"
  )

  expect_error(
    get_edsan("pmsi", query = list(RECDATE = "{2024-01-01,2024-01-31}")),
    "pmsi module only supports DATENT and DATSORT"
  )

  expect_error(
    get_edsan("biol", query = list(DATENT = "{2024-01-01,2024-01-31}")),
    "biol module only supports DATEXAM"
  )
})

test_that("field parsing normalizes vectors and comma-separated strings", {
  # Rationale: retrieval input contract. The public `fields` argument accepts
  # both a vector and a comma string, and must produce the same backend request.
  expect_identical(
    redsan:::.edsan_parse_fields("PATID, EVTID,, ELTID"),
    c("PATID", "EVTID", "ELTID")
  )
  expect_identical(
    redsan:::.edsan_parse_fields(c("PATID", "EVTID", "PATID")),
    c("PATID", "EVTID")
  )
})

test_that("missing live backend is reported as a retrieval error", {
  # Rationale: retrieval integration contract. Local checks often lack d2imr;
  # the call helper should return an explicit backend error shape.
  skip_if(
    requireNamespace("d2imr", quietly = TRUE),
    "d2imr is installed in this environment"
  )

  result <- redsan:::.edsan_call("doceds", list(RECDATE = "{2024-01-01,2024-01-02}"))

  expect_false(result$ok)
  expect_match(result$error, "Package 'd2imr' is required")
})
