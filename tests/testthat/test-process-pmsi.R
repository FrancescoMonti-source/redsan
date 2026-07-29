test_that("process_pmsi detects raw-export shape and parsing drift", {
  raw <- list(
    list(
      PATID = "P1",
      EVTID = "E1",
      ELTID = "L1",
      DATENT = "2024-01-01 08:30",
      DATSORT = "2024-01-03",
      PATAGE = "44",
      PATSEX = "M",
      SEJUM = "2024-01-01",
      SEJUF = "2024-01-03",
      DALL = "01:A41 02:I10",
      CODEACTE1 = "ABCD001",
      DATEACTE1 = "2024-01-02 09:15",
      NOMENCLATURE1 = "CCAM",
      UFPRO1 = "UF1"
    )
  )

  out <- process_pmsi(raw)

  expect_named(out, c("main", "actes", "diag"))
  expect_true(all(
    c("PATID", "EVTID", "ELTID", "DATENT", "DATSORT") %in%
      names(out$main)
  ))
  expect_true(all(
    c("EVTID", "CODEACTE", "DATEACTE", "NOMENCLATURE") %in%
      names(out$actes)
  ))
  expect_true(all(
    c("EVTID", "diag", "type_diag", "CODE_LABEL") %in%
      names(out$diag)
  ))
  expect_identical(
    list(
      main_time = as.character(out$main$HEURE_DATENT),
      acte = out$actes[c("CODEACTE", "NOMENCLATURE")],
      acte_time = as.character(out$actes$HEURE_DATEACTE),
      diag = out$diag[c("diag", "type_diag")]
    ),
    list(
      main_time = "08:30:00",
      acte = tibble::tibble(CODEACTE = "ABCD001", NOMENCLATURE = "CCAM"),
      acte_time = "09:15:00",
      diag = tibble::tibble(
        diag = c("A41", "I10"),
        type_diag = c("01", "02")
      )
    )
  )
})

test_that("C replaces DW only at the complete PMSI unit grain", {
  main <- tibble::tibble(
    PATID = c("P1", "P1", "P1", "P1", "P1", "P1", NA, NA, "P3", "P3"),
    EVTID = c("E1", "E1", "E1", "E1", "E1", "E1", "E2", "E2", NA, NA),
    ELTID = paste0("L", 1:10),
    SEJUM = c("U1", "U1", "U2", "U3", "U3", "U3", "U4", "U4", "U5", "U5"),
    SEJUF = c("F1", "F1", "F2", "F3", "F3", "F3", "F4", "F4", "F5", "F5"),
    SRC = c("DW", "C", "DW", "C", "other", NA, "DW", "C", "DW", "C")
  )

  expect_identical(
    prefer_pmsi_src_c_over_dw(main)$ELTID,
    paste0("L", 2:10)
  )
})

test_that("PMSI event bounds use complete main data at patient-event grain", {
  raw <- list(
    list(
      PATID = "P1", EVTID = "E1", ELTID = "L1",
      SEJUM = "U1", SEJUF = "F1", SRC = "DW",
      DATENT = "10/01/2024 08:00", DATSORT = "20/01/2024",
      DALL = "01:A41", CODEACTE1 = "ACTE1", DATEACTE1 = "2024-01-12"
    ),
    list(
      PATID = "P1", EVTID = "E1", ELTID = "L2",
      SEJUM = "U1", SEJUF = "F1", SRC = "C",
      DATENT = "02/02/2024 09:00", DATSORT = "03/02/2024",
      DALL = "02:I10", CODEACTE1 = "ACTE2", DATEACTE1 = "2024-02-02"
    ),
    list(
      PATID = "P2", EVTID = "E1", ELTID = "L3",
      SEJUM = "U2", SEJUF = "F2", SRC = "C",
      DATENT = "05/03/2024 09:00", DATSORT = "06/03/2024",
      DALL = "03:J20", CODEACTE1 = "ACTE3", DATEACTE1 = "2024-03-05"
    )
  )

  out <- process_pmsi(raw)
  out_all <- process_pmsi(raw, source_policy = "all")
  bounds <- lapply(out[c("actes", "diag")], function(table) {
    dplyr::distinct(
      table,
      PATID,
      EVTID,
      DATENT = as.Date(DATENT, tz = "Europe/Paris"),
      DATSORT = as.Date(DATSORT, tz = "Europe/Paris")
    )
  })

  expect_identical(
    list(
      filtered_main = out$main$ELTID,
      complete_main = out_all$main$ELTID,
      actes = out$actes$CODEACTE,
      diag = out$diag$diag,
      bounds = bounds
    ),
    list(
      filtered_main = c("L2", "L3"),
      complete_main = c("L1", "L2", "L3"),
      actes = c("ACTE1", "ACTE2", "ACTE3"),
      diag = c("A41", "I10", "J20"),
      bounds = list(
        actes = tibble::tibble(
          PATID = c("P1", "P2"),
          EVTID = c("E1", "E1"),
          DATENT = as.Date(c("2024-01-10", "2024-03-05")),
          DATSORT = as.Date(c("2024-02-03", "2024-03-06"))
        ),
        diag = tibble::tibble(
          PATID = c("P1", "P2"),
          EVTID = c("E1", "E1"),
          DATENT = as.Date(c("2024-01-10", "2024-03-05")),
          DATSORT = as.Date(c("2024-02-03", "2024-03-06"))
        )
      )
    )
  )
})
