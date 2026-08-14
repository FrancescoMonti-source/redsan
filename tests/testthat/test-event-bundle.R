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

test_that("bundles upgrade legacy element IDs and biology labels", {
  reference <- edsan_reference("bio")
  known_code <- reference$TYPEANA[[1L]]
  known_label <- reference$TYPEANA_LABEL[[1L]]

  sources <- list(
    biol = tibble::tibble(
      EVTID = c("E1", "E2"),
      ELTID = c("B1", "B2"),
      BIOL_ID = c("B1", "B2"),
      TYPEANA = c(known_code, "FAIT_MAISON"),
      NUMRES = c(4.2, 1)
    ),
    viro = tibble::tibble(
      EVTID = c("E1", "E2"),
      VIRO_ID = c("V1", "V2")
    )
  )

  bundles <- build_event_bundles(c("E1", "E2"), sources)

  expect_identical(bundles[["E1"]]$sources$biol$TYPEANA_LABEL, known_label)
  expect_identical(bundles[["E2"]]$sources$biol$TYPEANA_LABEL, NA_character_)
  expect_identical(bundles[["E1"]]$sources$biol$ELTID, "B1")
  expect_identical(bundles[["E1"]]$sources$viro$ELTID, "V1")
  expect_false("BIOL_ID" %in% names(bundles[["E1"]]$sources$biol))
  expect_false("VIRO_ID" %in% names(bundles[["E1"]]$sources$viro))
  expect_identical(
    build_event_bundle("E1", sources)$sources$biol$TYPEANA_LABEL,
    known_label
  )
})

test_that("bundles keep existing biology labels and untypeable sources as they are", {
  sources <- list(
    biol = tibble::tibble(
      EVTID = "E1",
      TYPEANA = "FAIT_MAISON",
      TYPEANA_LABEL = "local label"
    ),
    doceds = tibble::tibble(EVTID = "E1", TYPEANA = "not a biology column")
  )

  bundle <- build_event_bundle("E1", sources)

  expect_identical(bundle$sources$biol$TYPEANA_LABEL, "local label")
  expect_false("TYPEANA_LABEL" %in% names(bundle$sources$doceds))
})

test_that("empty biology sources still expose the labelled columns", {
  bundle <- build_event_bundle("E1", list(biol = tibble::tibble()))

  expect_identical(
    names(bundle$sources$biol),
    c("ELTID", "TYPEANA", "TYPEANA_LABEL")
  )
  expect_identical(nrow(bundle$sources$biol), 0L)
})

test_that("biology sources without TYPEANA are partitioned unchanged", {
  sources <- list(biol = tibble::tibble(EVTID = c("E1", "E2"), NUMRES = c(1, 2)))

  bundle <- build_event_bundle("E1", sources)

  expect_identical(names(bundle$sources$biol), c("EVTID", "NUMRES"))
})

test_that("get_event_bundle is a wrapper around get_event_bundles", {
  reference <- edsan_reference("bio")
  known_code <- reference$TYPEANA[[1L]]
  known_label <- reference$TYPEANA_LABEL[[1L]]

  calls <- list()
  fake_get_edsan <- function(module, what, query, process, ...) {
    calls[[length(calls) + 1L]] <<- list(module = module, query = query)
    # Mirrors an EDSAN artifact normalized before `label_biol()` existed: the
    # bundle must still expose TYPEANA_LABEL to downstream consumers.
    tibble::tibble(
      EVTID = query$EVTID,
      TYPEANA = rep(known_code, length(query$EVTID))
    )
  }

  testthat::local_mocked_bindings(
    get_edsan = fake_get_edsan,
    .package = "redsan"
  )

  bundle <- get_event_bundle("E1", modules = "biol")
  plural <- get_event_bundles("E1", modules = "biol")

  expect_identical(bundle$sources, plural[["E1"]]$sources)
  expect_identical(bundle$event_id, "E1")
  expect_identical(bundle$sources$biol$TYPEANA_LABEL, known_label)
  expect_identical(
    calls,
    rep(list(list(module = "biol", query = list(EVTID = "E1"))), 2L)
  )
})

test_that("get_event_bundle rejects anything other than one EVTID", {
  expect_error(get_event_bundle(c("E1", "E2")), "exactly one EVTID", fixed = TRUE)
  expect_error(
    get_event_bundle(character()),
    "must contain one or more EVTID values",
    fixed = TRUE
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
