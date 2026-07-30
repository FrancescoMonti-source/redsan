test_that("label_doceds joins rectypes, keeping unmatched and list-column keys", {
  documents <- data.frame(ELTID = c("L1", "L2", "L3"), stringsAsFactors = FALSE)
  # A raw payload can expose RECTYPE as a one-element list per row, which the
  # reference join cannot use: that shape cost a silent failure once already.
  documents$RECTYPE <- list("AJ", "PAS_UN_TYPE", NULL)

  labelled <- label_doceds(documents)

  expect_type(labelled$RECTYPE, "character")
  expect_identical(labelled$RECTYPE_LABEL[[1L]], "Action Juridique Std")
  expect_true(is.na(labelled$RECTYPE_LABEL[[2L]]))
  expect_identical(labelled$ELTID, c("L1", "L2", "L3"))
})

test_that("process_doceds tolerates a payload without RECTYPE", {
  bare <- process_doceds(data.frame(ELTID = "L1", stringsAsFactors = FALSE))

  expect_false("RECTYPE_LABEL" %in% names(bare))
  expect_identical(nrow(bare), 1L)
})

test_that("event bundles guarantee RECTYPE_LABEL on doceds", {
  sources <- list(
    doceds = tibble::tibble(EVTID = "E1", ELTID = "L1", RECTYPE = "AJ"),
    pmsi = list(
      main = tibble::tibble(EVTID = "E1"),
      actes = tibble::tibble(EVTID = "E1"),
      diag = tibble::tibble(EVTID = "E1")
    )
  )

  bundle <- build_event_bundle("E1", sources)

  expect_identical(bundle$sources$doceds$RECTYPE_LABEL, "Action Juridique Std")
})
