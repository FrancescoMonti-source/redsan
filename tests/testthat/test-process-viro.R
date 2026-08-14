test_that("process_viro uses VIRO source identity and sampling time", {
  raw <- list(
    viroA = list(
      PATID = "P1",
      EVTID = "E1",
      DATEPRELEV = "2024-01-01 08:30",
      RESULTATS = data.frame(
        TYPEANA = c("PCR", "PCR"),
        STRRES = c("positive", "negative"),
        NUMRES = I(list(12.5, NA_real_))
      )
    ),
    viroB = list(
      PATID = "P2",
      EVTID = "E2",
      DATEPRELEV = "2024-01-02",
      RESULTATS = data.frame()
    )
  )

  out <- process_viro(raw)

  expect_identical(
    list(
      rows = nrow(out),
      ELTID = out$ELTID,
      has_VIRO_ID = "VIRO_ID" %in% names(out),
      result = out$STRRES,
      date_is_posix = inherits(out$DATEPRELEV, "POSIXct"),
      time = as.character(out$HEURE_DATEPRELEV)
    ),
    list(
      rows = 2L,
      ELTID = rep("viroA", 2),
      has_VIRO_ID = FALSE,
      result = c("positive", "negative"),
      date_is_posix = TRUE,
      time = rep("08:30:00", 2)
    )
  )
})
