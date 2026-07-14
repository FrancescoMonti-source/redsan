test_that("process_pmsi returns stable normalized PMSI tables", {
  # Rationale: source/output sentinel plus process contract. The normalized
  # table names and minimum columns are an executable compatibility surface for
  # the raw export; the remaining assertions protect silent parsing changes.
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
  expect_true(all(c("PATID", "EVTID", "ELTID", "DATENT", "DATSORT") %in% names(out$main)))
  expect_true(all(c("EVTID", "CODEACTE", "DATEACTE") %in% names(out$actes)))
  expect_true(all(c("EVTID", "diag", "type_diag") %in% names(out$diag)))

  expect_true(inherits(out$main$DATENT, "POSIXct"))
  expect_equal(as.character(out$main$HEURE_DATENT), "08:30:00")
  expect_true(is.na(out$main$HEURE_DATSORT))

  expect_equal(out$actes$CODEACTE, "ABCD001")
  expect_true(inherits(out$actes$DATEACTE, "POSIXct"))
  expect_equal(as.character(out$actes$HEURE_DATEACTE), "09:15:00")
  expect_equal(out$diag$diag, c("A41", "I10"))
  expect_equal(out$diag$type_diag, c("01", "02"))
  # diag carries the subject attributes like main and actes (one row per DALL token).
  expect_equal(out$diag$PATSEX, c("M", "M"))
  expect_equal(out$main$PATAGE, 44)
  expect_equal(out$actes$PATAGE, 44)
  expect_equal(out$diag$PATAGE, c(44, 44))
})

test_that("process_pmsi handles empty and missing-detail payloads", {
  # Rationale: process-output contract. Empty PMSI results or payloads without
  # actes/DALL are valid no-evidence shapes, not parser failures.
  empty <- process_pmsi(list())

  expect_true(all(vapply(empty, nrow, integer(1)) == 0L))

  no_details <- process_pmsi(list(list(PATID = "P1", EVTID = "E1", ELTID = "L1")))

  expect_equal(nrow(no_details$main), 1)
  expect_equal(nrow(no_details$actes), 0)
  expect_equal(nrow(no_details$diag), 0)
})

test_that("prefer_pmsi_main_source applies C over DW only within a complete unit key", {
  # Rationale: source-normalization invariant. C replaces DW only for the same
  # patient-event unit, while DW fallback, unclassified sources, incomplete
  # keys, native row identifiers, column types, and retained order stay intact.
  main <- tibble::tibble(
    PATID = c("P1", "P1", "P1", "P1", "P1", "P1", NA, NA, "P3", "P3"),
    EVTID = c("E1", "E1", "E1", "E1", "E1", "E1", "E2", "E2", NA, NA),
    ELTID = paste0("L", 1:10),
    SEJUM = c("U1", "U1", "U2", "U3", "U3", "U3", "U4", "U4", "U5", "U5"),
    SEJUF = c("F1", "F1", "F2", "F3", "F3", "F3", "F4", "F4", "F5", "F5"),
    SRC = c("DW", "C", "DW", "C", "other", NA, "DW", "C", "DW", "C"),
    sequence = seq_len(10)
  )

  out <- prefer_pmsi_main_source(main)

  expect_identical(out$ELTID, paste0("L", 2:10))
  expect_identical(names(out), names(main))
  expect_identical(vapply(out, typeof, character(1)), vapply(main, typeof, character(1)))
  expect_identical(
    vapply(out, function(x) paste(class(x), collapse = "/"), character(1)),
    vapply(main, function(x) paste(class(x), collapse = "/"), character(1))
  )
})

test_that("process_pmsi joins event-level stay dates to detail tables", {
  # Rationale: process-output contract. Detail tables should inherit event-level
  # stay bounds for their PATID + EVTID from parsed dates in main, so min/max
  # must not be string-based or leak across patients sharing an EVTID.
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
    ),
    list(
      PATID = "P2",
      EVTID = "E1",
      ELTID = "L3",
      DATENT = "05/03/2024 09:00",
      DATSORT = "06/03/2024",
      DALL = "03:J20",
      CODEACTE1 = "ACTE3",
      DATEACTE1 = "2024-03-05",
      UFPRO1 = "UF3"
    )
  )

  out <- process_pmsi(raw)

  expect_true(inherits(out$diag$DATENT, "POSIXct"))
  expect_true(inherits(out$diag$DATSORT, "POSIXct"))
  for (table_name in c("diag", "actes")) {
    table <- out[[table_name]]
    p1 <- table$PATID == "P1" & table$EVTID == "E1"
    p2 <- table$PATID == "P2" & table$EVTID == "E1"
    expect_equal(unique(as.Date(table$DATENT[p1], tz = "Europe/Paris")), as.Date("2024-01-10"))
    expect_equal(unique(as.Date(table$DATSORT[p1], tz = "Europe/Paris")), as.Date("2024-02-03"))
    expect_equal(unique(as.Date(table$DATENT[p2], tz = "Europe/Paris")), as.Date("2024-03-05"))
    expect_equal(unique(as.Date(table$DATSORT[p2], tz = "Europe/Paris")), as.Date("2024-03-06"))
  }
})
