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

  expect_true(inherits(out$RECDATE, "POSIXct"))
  expect_equal(as.character(out$HEURE_RECDATE), c("08:30:00", NA_character_))
})

test_that("process_doceds types PATAGE without changing the remaining payload", {
  # Rationale: process-output contract. DOCEDS should expose the same numeric
  # age type as the other normalized modules while preserving document fields.
  raw <- data.frame(
    PATID = c("P1", "P2"),
    EVTID = c("E1", "E2"),
    ELTID = c("L1", "L2"),
    PATAGE = c("44", "43"),
    RECTYPE = c("CR", "CR"),
    RECTXT = c("synthetic one", "synthetic two")
  )

  out <- process_doceds(raw)

  expect_type(out$PATAGE, "double")
  expect_equal(out$PATAGE, c(44, 43))

  other_columns <- setdiff(names(raw), "PATAGE")
  expect_equal(out[other_columns], tibble::as_tibble(raw[other_columns]))
})
