test_that("normalized sources are partitioned into ordered event bundles", {
  sources <- list(
    doceds = tibble::tibble(
      EVTID = c("E2", "E1", "OTHER"),
      ELTID = c("D2", "D1", "DX")
    ),
    pmsi = list(
      main = tibble::tibble(EVTID = c("E1", "E2"), stay = c(1L, 2L)),
      actes = tibble::tibble(EVTID = "E1", CODEACTE = "A1"),
      diag = tibble::tibble()
    ),
    biol = tibble::tibble(EVTID = "E1", BIOL_ID = "B1")
  )

  bundles <- build_event_bundles(c("E2", "E1", "E3"), sources)

  expect_identical(names(bundles), c("E2", "E1", "E3"))
  expect_true(all(vapply(bundles, inherits, logical(1), "edsan_event_bundle")))
  expect_identical(bundles$E2$sources$doceds$ELTID, "D2")
  expect_identical(bundles$E1$sources$pmsi$actes$CODEACTE, "A1")
  expect_identical(names(bundles$E1$sources$pmsi), c("main", "actes", "diag"))
  expect_equal(nrow(bundles$E3$sources$doceds), 0L)
  expect_equal(nrow(bundles$E3$sources$pmsi$main), 0L)
  expect_equal(nrow(bundles$E3$sources$pmsi$diag), 0L)
  expect_false(any(vapply(bundles, function(x) "OTHER" %in% x$sources$doceds$EVTID,
                          logical(1))))

  single <- build_event_bundle("E1", sources)
  expect_identical(single$event_id, "E1")
  expect_identical(single$sources, bundles$E1$sources)

  numeric <- build_event_bundle(1, list(doceds = tibble::tibble(EVTID = "1")))
  expect_identical(numeric$event_id, "1")
})

test_that("batch retrieval calls each selected source once with all event ids", {
  calls <- list()
  fake_get_edsan <- function(module, what, query, process, ...) {
    calls[[length(calls) + 1L]] <<- list(
      module = module, what = what, query = query, process = process
    )
    if (identical(module, "pmsi")) {
      return(list(
        main = tibble::tibble(EVTID = query$EVTID),
        actes = tibble::tibble(EVTID = character()),
        diag = tibble::tibble(EVTID = character())
      ))
    }
    tibble::tibble(EVTID = query$EVTID)
  }

  testthat::local_mocked_bindings(
    get_edsan = fake_get_edsan,
    .package = "redsan"
  )

  bundles <- get_event_bundles(
    c("E1", "E2"),
    sources = c("doceds", "pmsi", "biol")
  )

  expect_length(calls, 3L)
  expect_identical(vapply(calls, `[[`, character(1), "module"),
                   c("doceds", "pmsi", "biol"))
  expect_true(all(vapply(calls, function(x) {
    identical(x$query, list(EVTID = c("E1", "E2")))
  }, logical(1))))
  expect_true(all(vapply(calls, function(x) identical(x$what, "data"), logical(1))))
  expect_true(all(vapply(calls, function(x) isTRUE(x$process), logical(1))))
  expect_identical(names(bundles), c("E1", "E2"))

  calls <- list()
  single <- get_event_bundle("E1", sources = "doceds")
  expect_s3_class(single, "edsan_event_bundle")
  expect_identical(single$event_id, "E1")
  expect_length(calls, 1L)
})

test_that("invalid identifiers and malformed sources fail clearly", {
  expect_error(build_event_bundles(character(), list(doceds = tibble::tibble())),
               "one or more EVTID values", fixed = TRUE)
  expect_error(build_event_bundles(c("E1", "E1"), list(doceds = tibble::tibble())),
               "duplicate EVTID", fixed = TRUE)
  expect_error(build_event_bundle(c("E1", "E2"), list(doceds = tibble::tibble())),
               "exactly one EVTID", fixed = TRUE)
  expect_error(
    build_event_bundle("E1", list(doceds = tibble::tibble(value = 1L))),
    "must contain an EVTID column",
    fixed = TRUE
  )
  expect_error(
    build_event_bundle("E1", list(pmsi = list(main = tibble::tibble()))),
    "main, actes, and diag",
    fixed = TRUE
  )
  expect_error(get_event_bundles("E1", sources = "bact"),
               "Unknown EDSAN source(s): bact", fixed = TRUE)
})
