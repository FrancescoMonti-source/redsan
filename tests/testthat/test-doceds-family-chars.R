test_that("doceds_family_chars aggregates unnamed per-document lists", {
  result <- doceds_family_chars(list(
    c(rgpd = 120L, letter_header = 80L),
    c(rgpd = 60L)
  ))

  expect_identical(result, c(rgpd = 180L, letter_header = 80L))
})

test_that("doceds_family_chars handles single document", {
  result <- doceds_family_chars(list(
    c(rgpd = 10L, letter_header = 5L)
  ))

  expect_identical(result, c(rgpd = 10L, letter_header = 5L))
})

test_that("doceds_family_chars handles empty list", {
  result <- doceds_family_chars(list())

  expect_identical(result, setNames(integer(), character()))
})

test_that("doceds_family_chars is robust to named outer lists", {
  # When the per_document list is named (e.g., from list(...) with names),
  # the function should still properly aggregate families across documents
  # without prefixing family names with document names.
  result <- doceds_family_chars(list(
    doc_a = c(rgpd = 10L),
    doc_b = c(rgpd = 20L)
  ))

  expect_identical(result, c(rgpd = 30L))
})

test_that("doceds_family_chars with named list and multiple families", {
  result <- doceds_family_chars(list(
    doc_a = c(rgpd = 120L, letter_header = 80L),
    doc_b = c(rgpd = 60L, footer = 40L)
  ))

  expect_identical(result, c(rgpd = 180L, letter_header = 80L, footer = 40L))
})

test_that("doceds_family_chars returns results ordered by size descending", {
  result <- doceds_family_chars(list(
    doc_a = c(small = 10L, large = 100L, medium = 50L),
    doc_b = c(small = 5L)
  ))

  expect_identical(result, c(large = 100L, medium = 50L, small = 15L))
})
