test_that("event bundle retrieves selected modules without reshaping them", {
  calls <- list()
  fake_get_edsan <- function(module, what, query, process, ...) {
    calls[[length(calls) + 1L]] <<- list(
      module = module,
      what = what,
      query = query,
      process = process
    )

    switch(
      module,
      doceds = tibble::tibble(EVTID = query$EVTID, ELTID = "D1"),
      pmsi = list(
        main = tibble::tibble(EVTID = query$EVTID),
        actes = tibble::tibble(EVTID = query$EVTID, ELTID = "A1"),
        diag = tibble::tibble(EVTID = query$EVTID, ELTID = "X1")
      ),
      biol = tibble::tibble(EVTID = query$EVTID, BIOL_ID = "B1"),
      viro = tibble::tibble(EVTID = query$EVTID, VIRO_ID = "V1")
    )
  }

  testthat::local_mocked_bindings(
    get_edsan = fake_get_edsan,
    .package = "redsan"
  )

  bundle <- get_event_bundle("E1", sources = c("doceds", "pmsi"))

  expect_s3_class(bundle, "edsan_event_bundle")
  expect_identical(bundle$event_id, "E1")
  expect_identical(names(bundle$sources), c("doceds", "pmsi"))
  expect_identical(names(bundle$sources$pmsi), c("main", "actes", "diag"))
  expect_true(inherits(bundle$created_at, "POSIXct"))
  expect_length(calls, 2L)
  expect_true(all(vapply(calls, function(x) identical(x$query, list(EVTID = "E1")), logical(1))))
  expect_true(all(vapply(calls, function(x) identical(x$what, "data"), logical(1))))
  expect_true(all(vapply(calls, function(x) isTRUE(x$process), logical(1))))
})

test_that("event bundle expands all from the source registry", {
  modules <- character()
  testthat::local_mocked_bindings(
    get_edsan = function(module, ...) {
      modules <<- c(modules, module)
      tibble::tibble()
    },
    .package = "redsan"
  )

  bundle <- get_event_bundle("E1")

  expect_identical(names(bundle$sources), unique(edsan_sources()$module))
  expect_identical(modules, unique(edsan_sources()$module))
})

test_that("event bundle rejects invalid identifiers and unknown sources", {
  expect_error(get_event_bundle(character()), "one non-missing EVTID", fixed = TRUE)
  expect_error(get_event_bundle(NA_character_), "one non-missing EVTID", fixed = TRUE)
  expect_error(get_event_bundle(""), "one non-missing EVTID", fixed = TRUE)
  expect_error(
    get_event_bundle("E1", sources = "bact"),
    "Unknown EDSAN source(s): bact",
    fixed = TRUE
  )
})

test_that("event bundle print is compact and preserves the object", {
  bundle <- structure(
    list(
      event_id = "E1",
      sources = list(
        doceds = tibble::tibble(ELTID = c("D1", "D2")),
        pmsi = list(
          main = tibble::tibble(EVTID = "E1"),
          actes = tibble::tibble(),
          diag = tibble::tibble(ELTID = c("X1", "X2"))
        )
      ),
      created_at = Sys.time()
    ),
    class = c("edsan_event_bundle", "list")
  )

  output <- capture.output(returned <- print(bundle))

  expect_match(output[[1]], "EDSaN event bundle: E1", fixed = TRUE)
  expect_true(any(grepl("doceds: 2 rows", output, fixed = TRUE)))
  expect_true(any(grepl("pmsi: main=1, actes=0, diag=2", output, fixed = TRUE)))
  expect_identical(returned, bundle)
})
