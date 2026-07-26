test_that("EDSAN source registry documents module source contracts", {
  # Rationale: source contract. Downstream code should read HDW structure from
  # redsan instead of re-guessing date keys, identifiers, and source time kind.
  expect_setequal(edsan_sources()$module, c("doceds", "pmsi", "biol", "viro"))
  expect_equal(edsan_sources("doceds")$default_batch_key, "RECDATE")
  expect_equal(edsan_sources("doceds")$normalizer, "process_doceds")
  expect_equal(edsan_sources("biol")$source_time_kind, "point")
  expect_equal(edsan_sources("viro")$default_batch_key, "DATEPRELEV")
  expect_equal(edsan_sources("viro")$normalizer, "process_viro")

  pmsi_diag <- edsan_sources("pmsi", "diag")
  expect_equal(pmsi_diag$source_time_kind, "interval")
  expect_equal(pmsi_diag$source_time_start, "DATENT")
  expect_equal(pmsi_diag$source_time_end, "DATSORT")
})

test_that("registered identifiers are stable character values", {
  # Rationale: source contract. Identifiers are opaque coordinates; normalized
  # outputs must not expose source-dependent numeric types or scientific text.
  ids <- list(PATID = 100000, EVTID = 200000, ELTID = 300000)
  pmsi <- process_pmsi(list(c(
    ids,
    list(
      DATENT = "2024-01-01",
      DATSORT = "2024-01-02",
      DALL = "01:A41",
      CODEACTE1 = "ACT1",
      DATEACTE1 = "2024-01-01"
    )
  )))
  tables <- list(
    "doceds/documents" = process_doceds(as.data.frame(ids)),
    "pmsi/main" = pmsi$main,
    "pmsi/actes" = pmsi$actes,
    "pmsi/diag" = pmsi$diag,
    "biol/results" = process_biol(list(c(
      ids,
      list(
        DATEXAM = "2024-01-01",
        RESULTATS = data.frame(TYPEANA = "K.K", NUMRES = 5)
      )
    ))),
    "viro/results" = process_viro(list(list(
      PATID = 100000,
      EVTID = 200000,
      DATEPRELEV = "2024-01-01",
      RESULTATS = data.frame(TYPEANA = "PCR", STRRES = "negative")
    )))
  )
  registry <- edsan_sources()
  identifier_columns <- unlist(
    lapply(seq_len(nrow(registry)), function(index) {
      table_key <- paste(
        registry$module[[index]],
        registry$table[[index]],
        sep = "/"
      )
      table <- tables[[table_key]]
      identifiers <- intersect(
        registry$identifiers[[index]],
        names(table)
      )
      as.list(table[identifiers])
    }),
    recursive = FALSE
  )

  expect_true(all(vapply(
    identifier_columns,
    is.character,
    logical(1)
  )))
  expect_setequal(
    unique(unlist(identifier_columns, use.names = FALSE)),
    c("1", "100000", "200000", "300000")
  )
})
