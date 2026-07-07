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
  # diag carries the subject attributes like main and actes (one row per DALL token).
  expect_equal(out$diag$PATSEX, c("M", "M"))
  expect_equal(out$diag$PATAGE, c("44", "44"))
})

test_that("process_pmsi handles empty and missing-detail payloads", {
  # Rationale: process-output contract. Empty PMSI results or payloads without
  # actes/DALL are valid no-evidence shapes, not parser failures.
  empty <- process_pmsi(list())

  expect_named(empty, c("main", "actes", "diag"))
  expect_equal(nrow(empty$main), 0)
  expect_true(all(c("PATID", "EVTID", "ELTID", "DATENT", "DATSORT") %in% names(empty$main)))
  expect_equal(nrow(empty$actes), 0)
  expect_true(all(c("CODEACTE", "DATEACTE", "UFPRO", "UFDEM", "NOMENCLATURE") %in% names(empty$actes)))
  expect_equal(nrow(empty$diag), 0)
  expect_true(all(c("diag", "type_diag") %in% names(empty$diag)))

  no_details <- process_pmsi(list(list(PATID = "P1", EVTID = "E1", ELTID = "L1")))

  expect_equal(nrow(no_details$main), 1)
  expect_equal(nrow(no_details$actes), 0)
  expect_equal(nrow(no_details$diag), 0)
})

test_that("process_pmsi joins event-level stay dates to detail tables", {
  # Rationale: process-output contract. Detail tables should inherit event-level
  # stay bounds from parsed dates in main, so min/max must not be string-based.
  raw <- list(
    list(
      PATID = "P1",
      EVTID = "E1",
      ELTID = "L1",
      DATENT = "10/01/2024 08:00",
      DATSORT = "20/01/2024",
      DALL = "01:A41",
      CODEACTE1 = "ACTE1",
      DATEACTE1 = "2024-01-12",
      UFPRO1 = "UF1"
    ),
    list(
      PATID = "P1",
      EVTID = "E1",
      ELTID = "L2",
      DATENT = "02/02/2024 09:00",
      DATSORT = "03/02/2024",
      DALL = "02:I10",
      CODEACTE1 = "ACTE2",
      DATEACTE1 = "2024-02-02",
      UFPRO1 = "UF2"
    )
  )

  out <- process_pmsi(raw)

  expect_true(inherits(out$diag$DATENT, "POSIXct"))
  expect_true(inherits(out$diag$DATSORT, "POSIXct"))
  expect_equal(unique(as.Date(out$diag$DATENT, tz = "Europe/Paris")), as.Date("2024-01-10"))
  expect_equal(unique(as.Date(out$diag$DATSORT, tz = "Europe/Paris")), as.Date("2024-02-03"))
  expect_equal(unique(as.Date(out$actes$DATENT, tz = "Europe/Paris")), as.Date("2024-01-10"))
  expect_equal(unique(as.Date(out$actes$DATSORT, tz = "Europe/Paris")), as.Date("2024-02-03"))
})
