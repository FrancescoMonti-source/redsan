test_that("event bundles isolate rows at the EVTID boundary", {
  sources <- list(
    doceds = tibble::tibble(
      EVTID = c("E2", "E1", "OTHER"),
      ELTID = c("D2", "D1", "DX")
    ),
    pmsi = list(
      main = tibble::tibble(EVTID = c("E1", "E2"), stay = c(1L, 2L)),
      actes = tibble::tibble(EVTID = "E1", CODEACTE = "A1"),
      diag = tibble::tibble(EVTID = character())
    )
  )

  bundles <- build_event_bundles(c("E2", "E1", "E3"), sources)
  observed <- lapply(bundles, function(bundle) {
    list(
      event_id = bundle$event_id,
      doceds = bundle$sources$doceds$ELTID,
      pmsi_main = bundle$sources$pmsi$main$stay,
      pmsi_actes = bundle$sources$pmsi$actes$CODEACTE
    )
  })

  expect_identical(
    observed,
    list(
      E2 = list(
        event_id = "E2", doceds = "D2", pmsi_main = 2L,
        pmsi_actes = character()
      ),
      E1 = list(
        event_id = "E1", doceds = "D1", pmsi_main = 1L,
        pmsi_actes = "A1"
      ),
      E3 = list(
        event_id = "E3", doceds = character(), pmsi_main = integer(),
        pmsi_actes = character()
      )
    )
  )
})

test_that("batch retrieval avoids per-event EDSAN queries", {
  calls <- list()
  fake_get_edsan <- function(module, what, query, process, ...) {
    calls[[length(calls) + 1L]] <<- list(
      module = module,
      what = what,
      query = query,
      process = process
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

  get_event_bundles(
    c("E1", "E2"),
    modules = c("doceds", "pmsi", "biol")
  )

  expect_identical(
    calls,
    lapply(c("doceds", "pmsi", "biol"), function(module) {
      list(
        module = module,
        what = "data",
        query = list(EVTID = c("E1", "E2")),
        process = TRUE
      )
    })
  )
})
