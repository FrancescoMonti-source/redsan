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

test_that("the derived acts reference keeps one row per nomenclature and code", {
  actes <- edsan_reference("actes")
  stored <- c(ccam = "CCAM", cdam = "CDAM", csarr = "CSARR", ngap = "NGAP")

  expect_identical(names(actes), c("NOMENCLATURE", "CODEACTE", "CODEACTE_LABEL"))
  expect_setequal(unique(actes$NOMENCLATURE), unname(stored))
  expect_identical(
    vapply(names(stored), function(name) {
      sum(actes$NOMENCLATURE == stored[[name]])
    }, integer(1)),
    vapply(names(stored), function(name) {
      nrow(edsan_reference(name))
    }, integer(1))
  )
  expect_identical(
    anyDuplicated(paste(actes$NOMENCLATURE, actes$CODEACTE, sep = "|")),
    0L
  )
})

test_that("a reference retains codes whose label is undocumented", {
  rectypes <- edsan_reference("rectypes")

  expect_true("OPROOM" %in% rectypes$RECTYPE)
  expect_true(is.na(rectypes$RECTYPE_LABEL[rectypes$RECTYPE == "OPROOM"]))
  expect_false(anyNA(rectypes$RECTYPE))
})

test_that("bacteriology and biology analytes stay separate references", {
  bio <- edsan_reference("bio")
  bact <- edsan_reference("bact")
  shared <- intersect(bio$TYPEANA, bact$TYPEANA)

  expect_identical(names(bact), names(bio))
  expect_gt(length(shared), 0L)
  expect_false(identical(
    bio$TYPEANA_LABEL[match(shared, bio$TYPEANA)],
    bact$TYPEANA_LABEL[match(shared, bact$TYPEANA)]
  ))
})

test_that("the local PMSI unit bridge is one row per SEJUF", {
  ref <- edsan_reference("uf2umpmsi")

  expect_identical(names(ref), c("SEJUF", "SEJUM", "UM_PMSI"))
  expect_identical(nrow(ref), 203L)
  expect_identical(anyDuplicated(ref$SEJUF), 0L)
  expect_identical(length(unique(ref$UM_PMSI)), 199L)
  expect_identical(
    ref$UM_PMSI[match(c("5522", "5622", "5623", "6245"), ref$SEJUF)],
    c("5530", "5618", "5619", "6241")
  )
})
