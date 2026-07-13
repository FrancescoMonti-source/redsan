test_that("module date keys prevent cross-source retrieval", {
  # Rationale: retrieval contract. A date key belonging to another module must
  # be rejected locally; otherwise the backend could return a plausible but
  # incorrectly scoped extract.
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
