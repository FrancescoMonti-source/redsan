test_that("EDSAN source registry documents module source contracts", {
  # Rationale: source contract. Downstream code should read HDW structure from
  # redsan instead of re-guessing date keys, identifiers, and source time kind.
  expect_setequal(edsan_sources()$module, c("doceds", "pmsi", "biol", "viro"))
  expect_equal(edsan_sources("doceds")$default_batch_key, "RECDATE")
  expect_equal(edsan_sources("doceds")$normalizer, "process_doceds")
  expect_equal(edsan_sources("biol")$source_time_kind, "point")
  expect_equal(edsan_sources("viro")$default_batch_key, "DATEPRELEV")
  expect_equal(edsan_sources("viro")$normalizer, "process_viro")

  pmsi_diag <- edsan_sources("pmsi", "diag")
  expect_equal(pmsi_diag$source_time_kind, "interval")
  expect_equal(pmsi_diag$source_time_start, "DATENT")
  expect_equal(pmsi_diag$source_time_end, "DATSORT")
})
