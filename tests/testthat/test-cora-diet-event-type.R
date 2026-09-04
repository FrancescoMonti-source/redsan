test_that("CORA Diet defaults to H and R", {
  sql <- redsan:::.cora_diet_documents_sql("745068610")
  expect_match(sql, "d.TYPEEVT = 'H'", fixed = TRUE)
  expect_match(sql, "d.TYPEEVT = 'R'", fixed = TRUE)
})

test_that("CORA Diet can select H only", {
  sql <- redsan:::.cora_diet_documents_sql("745068610", event_type = "H")
  expect_match(sql, "d.TYPEEVT = 'H'", fixed = TRUE)
  expect_false(grepl("d.TYPEEVT = 'R'", sql, fixed = TRUE))
})

test_that("CORA Diet can select R only", {
  sql <- redsan:::.cora_diet_documents_sql("745068610", event_type = "R")
  expect_match(sql, "d.TYPEEVT = 'R'", fixed = TRUE)
  expect_false(grepl("d.TYPEEVT = 'H'", sql, fixed = TRUE))
})

test_that("get_cora_diet exposes event_type", {
  expect_true("event_type" %in% names(formals(get_cora_diet)))
  expect_identical(eval(formals(get_cora_diet)$event_type), c("H", "R"))
})
