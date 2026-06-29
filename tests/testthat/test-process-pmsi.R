test_that("process_pmsi returns stable normalized PMSI tables", {
  # Rationale: process-output contract. PMSI parsing should preserve native IDs,
  # split stay/act/diagnosis grains, and keep date-only values distinct from
  # explicitly timed values.
  raw <- list(
    list(
      PATID = "P1",
      EVTID = "E1",
      ELTID = "L1",
      DATENT = "2024-01-01 08:30",
      DATSORT = "2024-01-03",
      PATBD = "1980-01-01",
      PATAGE = "44",
      PATSEX = "M",
      SEJDUR = "2",
      SEJUM = "2024-01-01",
      SEJUF = "2024-01-03",
      DALL = "01:A41 02:I10",
      CODEACTE1 = "ABCD001",
      DATEACTE1 = "2024-01-02 09:15",
      UFPRO1 = "UF1"
    )
  )

  out <- process_pmsi(raw)

  expect_named(out, c("main", "actes", "diag"))
  expect_s3_class(out$main, "tbl_df")
  expect_s3_class(out$actes, "tbl_df")
  expect_s3_class(out$diag, "tbl_df")

  expect_equal(out$main$PATID, "P1")
  expect_true(inherits(out$main$DATENT, "POSIXct"))
  expect_equal(as.character(out$main$HEURE_DATENT), "08:30:00")
  expect_true(is.na(out$main$HEURE_DATSORT))

  expect_equal(out$actes$CODEACTE, "ABCD001")
  expect_equal(out$diag$diag, c("A41", "I10"))
  expect_equal(out$diag$type_diag, c("01", "02"))
})
