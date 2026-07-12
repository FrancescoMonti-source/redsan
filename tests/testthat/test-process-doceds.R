test_that("process_doceds types recorded dates without inventing times", {
  # Rationale: process-output contract. Downstream windowing must receive a
  # typed point time while retaining whether DOCEDS supplied an explicit time.
  raw <- data.frame(
    PATID = c("P1", "P2"),
    ELTID = c("L1", "L2"),
    RECDATE = c("2024-01-01 08:30", "02/01/2024"),
    RECTXT = c("synthetic one", "synthetic two")
  )

  out <- process_doceds(raw)

  expect_s3_class(out, "tbl_df")
  expect_equal(out$ELTID, c("L1", "L2"))
  expect_true(inherits(out$RECDATE, "POSIXct"))
  expect_equal(as.character(out$HEURE_RECDATE), c("08:30:00", NA_character_))
})

test_that("process_doceds rejects non-tabular payloads", {
  # Rationale: process-input contract. A nested/raw payload is not a prepared
  # DOCEDS table and must fail at the package boundary.
  expect_error(process_doceds(list(RECDATE = "2024-01-01")), "requires a data frame")
})
