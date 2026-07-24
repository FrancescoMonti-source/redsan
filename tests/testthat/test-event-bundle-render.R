test_that("render_event_bundle includes all present sources by default", {
  bundle <- structure(
    list(
      event_id = "E1",
      sources = list(
        doceds = tibble::tibble(EVTID = "E1", ELTID = "D1", RECTXT = "text"),
        pmsi = list(
          main = tibble::tibble(EVTID = "E1", SEJUM = "U1"),
          actes = tibble::tibble(EVTID = "E1", CODEACTE = "A1"),
          diag = tibble::tibble(EVTID = "E1", CODEDIAG = "D1")
        )
      ),
      created_at = as.POSIXct("2026-07-24 12:00:00", tz = "UTC")
    ),
    class = c("edsan_event_bundle", "list")
  )

  parsed <- jsonlite::fromJSON(
    render_event_bundle(bundle, pretty = FALSE),
    simplifyVector = FALSE
  )

  expect_identical(parsed$event_id, "E1")
  expect_identical(names(parsed$sources), c("doceds", "pmsi"))
  expect_identical(parsed$sources$doceds[[1]]$ELTID, "D1")
  expect_identical(parsed$sources$pmsi$main[[1]]$SEJUM, "U1")
  expect_identical(parsed$sources$pmsi$actes[[1]]$CODEACTE, "A1")
  expect_identical(parsed$sources$pmsi$diag[[1]]$CODEDIAG, "D1")
})

test_that("render_event_bundle selects whole present sources", {
  bundle <- structure(
    list(
      event_id = "E1",
      sources = list(
        doceds = tibble::tibble(ELTID = "D1"),
        biol = tibble::tibble(BIOL_ID = "B1"),
        viro = tibble::tibble(VIRO_ID = "V1")
      ),
      created_at = Sys.time()
    ),
    class = c("edsan_event_bundle", "list")
  )

  parsed <- jsonlite::fromJSON(
    render_event_bundle(bundle, sources = c("doceds", "viro"), pretty = FALSE),
    simplifyVector = FALSE
  )

  expect_identical(names(parsed$sources), c("doceds", "viro"))
  expect_identical(parsed$sources$doceds[[1]]$ELTID, "D1")
  expect_identical(parsed$sources$viro[[1]]$VIRO_ID, "V1")
})

test_that("render_event_bundle does not retrieve absent sources", {
  bundle <- structure(
    list(
      event_id = "E1",
      sources = list(doceds = tibble::tibble()),
      created_at = Sys.time()
    ),
    class = c("edsan_event_bundle", "list")
  )

  expect_error(
    render_event_bundle(bundle, sources = "biol"),
    "Source(s) not present in this event bundle: biol",
    fixed = TRUE
  )
})

test_that("render_event_bundle validates its bundle", {
  expect_error(
    render_event_bundle(list()),
    "`bundle` must be an edsan_event_bundle.",
    fixed = TRUE
  )
})

test_that("render_event_bundle preserves empty tables", {
  bundle <- structure(
    list(
      event_id = "E1",
      sources = list(biol = tibble::tibble(BIOL_ID = character())),
      created_at = Sys.time()
    ),
    class = c("edsan_event_bundle", "list")
  )

  parsed <- jsonlite::fromJSON(
    render_event_bundle(bundle, pretty = FALSE),
    simplifyVector = FALSE
  )

  expect_identical(parsed$sources$biol, list())
})
