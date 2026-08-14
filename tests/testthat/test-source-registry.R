test_that("registered identifiers remain opaque character coordinates", {
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
    "biol/results" = process_biol(list(
      `300000` = c(
        ids,
        list(
          DATEXAM = "2024-01-01",
          RESULTATS = data.frame(TYPEANA = "K.K", NUMRES = 5)
        )
      )
    )),
    "viro/results" = process_viro(list(
      `300000` = list(
        PATID = 100000,
        EVTID = 200000,
        DATEPRELEV = "2024-01-01",
        RESULTATS = data.frame(TYPEANA = "PCR", STRRES = "negative")
      )
    ))
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
    c("100000", "200000", "300000")
  )
})
