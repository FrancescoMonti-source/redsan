test_that("map_header_structure retains provenance across normalized variants", {
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
  antecedents <- dplyr::filter(
    out$candidate_lines_raw,
    line_key == "antecedents"
  )
  hits <- dplyr::filter(out$header_hits, line_key == "antecedents")

  expect_identical(
    antecedents[c("line_key", "n_docs", "n_variants")],
    tibble::tibble(
      line_key = "antecedents",
      n_docs = 2L,
      n_variants = 2L
    )
  )
  expect_identical(
    hits[c("PATID", "EVTID", "ELTID", "doc_id", "line_key")],
    tibble::tibble(
      PATID = c("P1", "P2"),
      EVTID = c("E1", "E2"),
      ELTID = c("L1", "L2"),
      doc_id = c("L1", "L2"),
      line_key = c("antecedents", "antecedents")
    )
  )
})

test_that("candidate filtering does not drop documents from signatures", {
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

  expect_identical(
    out$doc_signatures,
    tibble::tibble(
      doc_id = c("D1", "D2", "D3"),
      header_sequence = c("commun", "commun", ""),
      n_header_hits = c(1L, 1L, 0L)
    )
  )
})
