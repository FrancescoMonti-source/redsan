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
