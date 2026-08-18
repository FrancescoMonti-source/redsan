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

test_that("list-column TYPEANA is labelled instead of breaking the reference join", {
  reference <- edsan_reference("bio")
  known_code <- reference$TYPEANA[[1L]]
  # Raw EDSAN payloads wrap each scalar result in a one-element list, TYPEANA
  # included; the reference join needs an atomic key.
  raw <- list(L1 = list(
    PATID = "P1", EVTID = "E1", ELTID = "L1",
    DATEXAM = "2024-01-01 08:30",
    RESULTATS = data.frame(
      TYPEANA = I(list(known_code, "FAIT_MAISON")),
      NUMRES = I(list(4.2, 1))
    )
  ))

  out <- process_biol(raw)

  expect_identical(out$TYPEANA, c(known_code, "FAIT_MAISON"))
  expect_identical(
    out$TYPEANA_LABEL,
    c(reference$TYPEANA_LABEL[[1L]], NA_character_)
  )
  expect_identical(out$NUMRES, c(4.2, 1))
})

test_that("process_biol makes wrapped scalar result fields atomic", {
  raw <- list(L1 = list(
    PATID = "P1", EVTID = "E1", ELTID = "L1",
    DATEXAM = "2024-01-01 08:30",
    RESULTATS = data.frame(
      TYPEANA = I(list("A", "B", "C")),
      NUMRES = I(list(NA_real_, NA_real_, NA_real_)),
      STRRES = I(list("positif", NULL, c("faible", "douteux"))),
      UNITE = I(list("UI/mL", NULL, "")),
      CMT = I(list(NULL, "commentaire", NULL))
    )
  ))

  out <- process_biol(raw)

  expect_false(any(vapply(out, is.list, logical(1))))
  expect_identical(out$STRRES, c("positif", NA_character_, "faible;douteux"))
  expect_identical(out$UNITE, c("UI/mL", NA_character_, ""))
  expect_identical(out$CMT, c(NA_character_, "commentaire", NA_character_))
})

test_that("label_biol flattens TYPEANA shapes it receives from older artifacts", {
  reference <- edsan_reference("bio")
  known_code <- reference$TYPEANA[[1L]]
  biology <- tibble::tibble(
    TYPEANA = I(list(known_code, NULL, c("A", "B"), factor("FAIT_MAISON"))),
    NUMRES = c(1, 2, 3, 4)
  )

  labelled <- label_biol(biology)

  expect_identical(
    labelled$TYPEANA,
    c(known_code, NA_character_, "A;B", "FAIT_MAISON")
  )
  expect_identical(
    labelled$TYPEANA_LABEL,
    c(reference$TYPEANA_LABEL[[1L]], NA_character_, NA_character_, NA_character_)
  )
  expect_identical(labelled$NUMRES, c(1, 2, 3, 4))
})

test_that("virology results expose an atomic analyte code", {
  out <- process_viro(list(L1 = list(
    PATID = "P1", EVTID = "E1", DATEPRELEV = "2024-01-01",
    RESULTATS = data.frame(TYPEANA = I(list("VIRO.PCR")), STRRES = "NEGATIF")
  )))

  expect_identical(out$TYPEANA, "VIRO.PCR")
})
