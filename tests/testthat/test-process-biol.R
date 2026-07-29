test_that("process_biol preserves source-element grain while expanding results", {
  raw <- list(
    examA = list(
      PATID = "P1",
      EVTID = "E1",
      ELTID = "L1",
      DATEXAM = "2024-01-01 08:30",
      PATAGE = "44",
      RESULTATS = data.frame(
        TYPEANA = c("K.K", "K.K", "K.K"),
        NUMRES = I(list(5.4, 3, NA_real_)),
        STRRES = c(NA_character_, NA_character_, "qualitative")
      )
    ),
    examB = list(
      PATID = "P2",
      EVTID = "E2",
      DATEXAM = "2024-01-02",
      RESULTATS = data.frame()
    )
  )

  out <- process_biol(raw)

  expect_identical(
    list(
      rows = nrow(out),
      PATID = out$PATID,
      BIOL_ID = out$BIOL_ID,
      TYPEANA = out$TYPEANA,
      NUMRES = out$NUMRES,
      STRRES = out$STRRES,
      date_is_posix = inherits(out$DATEXAM, "POSIXct"),
      time = as.character(out$HEURE_DATEXAM)
    ),
    list(
      rows = 3L,
      PATID = rep("P1", 3),
      BIOL_ID = rep("examA", 3),
      TYPEANA = rep("K.K", 3),
      NUMRES = c(5.4, 3, NA_real_),
      STRRES = c(NA_character_, NA_character_, "qualitative"),
      date_is_posix = TRUE,
      time = rep("08:30:00", 3)
    )
  )
})
