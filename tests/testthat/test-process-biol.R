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
  reference <- edsan_reference("bio")
  expected_label <- reference$TYPEANA_LABEL[reference$TYPEANA == "K.K"]

  expect_identical(
    list(
      rows = nrow(out),
      PATID = out$PATID,
      BIOL_ID = out$BIOL_ID,
      TYPEANA = out$TYPEANA,
      TYPEANA_LABEL = out$TYPEANA_LABEL,
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
      TYPEANA_LABEL = rep(expected_label, 3),
      NUMRES = c(5.4, 3, NA_real_),
      STRRES = c(NA_character_, NA_character_, "qualitative"),
      date_is_posix = TRUE,
      time = rep("08:30:00", 3)
    )
  )
})

test_that("label_biol refreshes labels and preserves unmatched rows", {
  reference <- edsan_reference("bio")
  known_code <- reference$TYPEANA[[1L]]
  known_label <- reference$TYPEANA_LABEL[[1L]]
  biology <- tibble::tibble(
    TYPEANA = c(known_code, "FAIT_MAISON"),
    TYPEANA_LABEL = c("stale", "stale"),
    NUMRES = c(4.2, 1)
  )

  labelled <- label_biol(biology)

  expect_identical(labelled$TYPEANA, biology$TYPEANA)
  expect_identical(labelled$NUMRES, biology$NUMRES)
  expect_identical(
    labelled$TYPEANA_LABEL,
    c(known_label, NA_character_)
  )
})

test_that("label_biol validates its input contract", {
  expect_error(label_biol(list()), "must be a data frame")
  expect_identical(nrow(process_biol(list())), 0L)
  expect_identical(
    names(label_biol(tibble::tibble())),
    c("TYPEANA", "TYPEANA_LABEL")
  )
  expect_error(
    label_biol(tibble::tibble(NUMRES = 1)),
    "missing required column: TYPEANA",
    fixed = TRUE
  )
})
