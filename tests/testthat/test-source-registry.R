test_that("EDSAN source registry documents module source contracts", {
  # Rationale: source contract. Downstream code should read HDW structure from
  # redsan instead of re-guessing date keys, identifiers, and source time kind.
  sources <- edsan_sources()

  expect_setequal(sources$module, c("doceds", "pmsi", "biol"))
  expect_equal(edsan_sources("doceds")$default_batch_key, "RECDATE")
  expect_equal(edsan_sources("biol")$source_time_kind, "point")

  pmsi_diag <- edsan_sources("pmsi", "diag")
  expect_equal(pmsi_diag$source_time_kind, "interval")
  expect_equal(pmsi_diag$source_time_start, "DATENT")
  expect_equal(pmsi_diag$source_time_end, "DATSORT")
})

test_that("unknown source tables fail explicitly", {
  # Rationale: source contract. A misspelled table should fail at the registry
  # boundary rather than silently falling through into retrieval code.
  expect_error(edsan_sources("pmsi", "documents"), "Unknown EDSAN table")
})
