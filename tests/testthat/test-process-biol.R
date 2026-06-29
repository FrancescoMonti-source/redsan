test_that("process_biol preserves exam metadata and result payload columns", {
  # Rationale: process-output contract. Biology normalization should retain
  # source traceability while leaving analyte/value columns as returned by EDSAN.
  raw <- list(
    examA = list(
      PATID = "P1",
      EVTID = "E1",
      ELTID = "L1",
      DATEXAM = "2024-01-01 08:30",
      SEJUM = "2024-01-01",
      SEJUF = "2024-01-03",
      PATBD = "1980-01-01",
      PATAGE = "44",
      PATSEX = "M",
      CSTE_LABO = "LAB",
      RESULTATS = data.frame(TYPEANA = "K.K", VALEUR = "5.4")
    ),
    examB = list(
      PATID = "P2",
      EVTID = "E2",
      ELTID = "L2",
      DATEXAM = "2024-01-02",
      SEJUM = "2024-01-02",
      SEJUF = "2024-01-04",
      PATBD = "1981-01-01",
      PATAGE = "43",
      PATSEX = "F",
      CSTE_LABO = "LAB",
      RESULTATS = data.frame()
    )
  )

  out <- process_biol(raw)

  expect_s3_class(out, "tbl_df")
  expect_equal(nrow(out), 1)
  expect_equal(out$PATID, "P1")
  expect_equal(out$BIOL_ID, "examA")
  expect_equal(out$TYPEANA, "K.K")
  expect_equal(out$VALEUR, "5.4")
  expect_true(inherits(out$DATEXAM, "POSIXct"))
  expect_equal(as.character(out$HEURE_DATEXAM), "08:30:00")
})
