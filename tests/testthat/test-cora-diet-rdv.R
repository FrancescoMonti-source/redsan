test_that("CORA Diet SQL covers hospital and RDV event paths", {
  sql <- redsan:::.cora_diet_documents_sql("745068610")

  expect_match(sql, "FROM ICSF.MVTUS m", fixed = TRUE)
  expect_match(sql, "d.NOEVT = m.NOMVTUS", fixed = TRUE)
  expect_match(sql, "d.TYPEEVT = 'H'", fixed = TRUE)
  expect_match(sql, "m.NOSEJ IN ('745068610')", fixed = TRUE)

  expect_match(sql, "FROM ICSF.T_EVT_RDV r", fixed = TRUE)
  expect_match(sql, "d.NOEVT = r.NOEVT", fixed = TRUE)
  expect_match(sql, "d.TYPEEVT = 'R'", fixed = TRUE)
  expect_match(sql, "r.NOSEJ IN ('745068610')", fixed = TRUE)

  expect_match(sql, "d.NOSOUSVOLET = 443", fixed = TRUE)
  expect_match(sql, "d.ETATDOC = 1", fixed = TRUE)
})

test_that("CORA Diet RDV SQL keeps the existing output contract", {
  sql <- redsan:::.cora_diet_documents_sql("745068610")

  expect_match(sql, "d.NOEVT AS CORA_NOEVT", fixed = TRUE)
  expect_match(sql, "CAST(NULL AS VARCHAR2(10)) AS NOUSHEB", fixed = TRUE)
  expect_match(sql, "CAST(NULL AS VARCHAR2(10)) AS NOUSRESP", fixed = TRUE)
})


test_that("CORA Diet default H and R queries batch large IEP lists", {
  ieps <- as.character(seq_len(1001L))
  seen <- new.env(parent = emptyenv())
  seen$batch_sizes <- list()
  seen$ieps <- list()

  fake_query <- function(connection, sql) {
    in_clauses <- regmatches(
      sql,
      gregexpr("IN \\([^)]*\\)", sql, perl = TRUE)
    )[[1L]]
    ieps_by_path <- lapply(in_clauses, function(clause) {
      quoted_ieps <- regmatches(
        clause,
        gregexpr("'[0-9]+'", clause, perl = TRUE)
      )[[1L]]
      gsub("'", "", quoted_ieps, fixed = TRUE)
    })
    seen$batch_sizes[[length(seen$batch_sizes) + 1L]] <- lengths(ieps_by_path)
    seen$ieps[[length(seen$ieps) + 1L]] <- ieps_by_path[[1L]]

    tibble::tibble(
      IEP = ieps_by_path[[1L]],
      DTDOC = as.Date("2025-01-01")
    )
  }

  out <- redsan:::.cora_query_diet_documents(
    connection = structure(list(), class = "fake_connection"),
    ieps = c(ieps, ieps[[1L]]),
    query_fn = fake_query
  )

  expect_identical(
    seen$batch_sizes,
    list(c(900L, 900L), c(101L, 101L))
  )
  expect_identical(
    sort(unname(unlist(seen$ieps))),
    sort(ieps)
  )
  expect_identical(nrow(out), length(ieps))
})
