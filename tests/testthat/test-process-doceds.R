test_that("process_doceds distinguishes recorded dates from recorded times", {
  raw <- data.frame(
    PATID = c("P1", "P2"),
    ELTID = c("L1", "L2"),
    RECDATE = c("2024-01-01 08:30", "02/01/2024"),
    RECTXT = c("synthetic one", "synthetic two")
  )

  out <- process_doceds(raw)

  expect_true(inherits(out$RECDATE, "POSIXct"))
  expect_identical(
    as.character(out$HEURE_RECDATE),
    c("08:30:00", NA_character_)
  )
})
