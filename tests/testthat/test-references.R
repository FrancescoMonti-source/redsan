test_that("packaged reference mappings have usable unique codes", {
  stored_names <- setdiff(edsan_references()$name, "actes")
  invalid <- vapply(stored_names, function(name) {
    reference <- edsan_reference(name)
    key <- reference[[1L]]

    anyNA(key) ||
      any(!nzchar(trimws(key))) ||
      anyDuplicated(key) > 0L ||
      names(reference)[[1L]] %in% key
  }, logical(1))

  expect_identical(names(invalid)[invalid], character())
})
