test_that("map_header_structure preserves document provenance", {
  raw <- tibble::tibble(
    PATID = c("P1", "P2"),
    EVTID = c("E1", "E2"),
    ELTID = c("L1", "L2"),
    RECDATE = as.Date(c("2024-01-01", "2024-01-02")),
    RECTYPE = c("CR", "CR"),
    RECTXT = c(
      "ANTÉCÉDENTS:\nEXAMEN CLINIQUE:\nTexte 1",
      "ANTECEDENTS\nEXAMEN CLINIQUE:\nTexte 2"
    )
  )

  out <- map_header_structure(raw)

  expect_true(all(c(
    "PATID", "EVTID", "ELTID", "RECDATE", "RECTYPE", "doc_id",
    "line_no", "line_raw", "line_key"
  ) %in% names(out$lines)))
  expect_equal(out$lines$doc_id, rep(c("L1", "L2"), each = 3))

  antecedents <- dplyr::filter(
    out$candidate_lines_raw,
    line_key == "antecedents"
  )

  expect_equal(antecedents$n_docs, 2L)
  expect_equal(antecedents$n_variants, 2L)
  expect_true(grepl("ANTECEDENTS", antecedents$examples, fixed = TRUE))
  expect_true(grepl("ANTÉCÉDENTS:", antecedents$examples, fixed = TRUE))
  expect_true(all(c("PATID", "EVTID", "ELTID") %in% names(out$header_hits)))
  expect_true(all(c("PATID", "EVTID", "ELTID") %in% names(out$doc_signatures)))
})

test_that("map_header_structure applies an optional noise pattern", {
  raw <- tibble::tibble(
    DOC_ID = "D1",
    TEXT = "ANTECEDENTS\nTELEPHONE:\nTexte"
  )

  out <- map_header_structure(
    raw,
    text_col = "TEXT",
    id_col = "DOC_ID",
    metadata_cols = character(),
    noise_pattern = "^telephone"
  )

  expect_true(any(out$candidate_lines_raw$line_key == "telephone"))
  expect_false(any(out$candidate_lines$line_key == "telephone"))
  expect_equal(out$lines$doc_id, rep("D1", 3))
})

test_that("map_header_structure filters candidates without dropping documents", {
  raw <- tibble::tibble(
    DOC_ID = c("D1", "D2", "D3"),
    TEXT = c(
      "COMMUN:\nTexte",
      "COMMUN\nRARE:\nTexte",
      "Texte sans en-tête"
    )
  )

  out <- map_header_structure(
    raw,
    text_col = "TEXT",
    id_col = "DOC_ID",
    metadata_cols = character(),
    min_docs = 2L
  )

  expect_equal(out$candidate_lines$line_key, "commun")
  expect_equal(
    out$doc_signatures,
    tibble::tibble(
      doc_id = c("D1", "D2", "D3"),
      header_sequence = c("commun", "commun", ""),
      n_header_hits = c(1L, 1L, 0L)
    )
  )
})

test_that("map_header_structure reports missing input columns", {
  expect_error(
    map_header_structure(tibble::tibble(ELTID = "L1")),
    "Missing columns: RECTXT"
  )
})
