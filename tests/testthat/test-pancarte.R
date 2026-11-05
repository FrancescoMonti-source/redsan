pancarte_test_records <- function() {
  first <- list(
    ELTID = "L1", EVTID = "E1", PATID = "P1", PATAGE = 65, PATSEX = "F",
    DATENT = "2024-01-01", DATSORT = "2024-01-02", JOURMES = "2024-01-01",
    DATEMES = "2024-01-01T08:30:00", SEJUF = "F1", TYPE = "POULS-BTS-MIN",
    RES = "89", CMT = NULL, SEJUM = "U1", NUMRES = 89
  )
  second <- first
  second$TYPE <- "SELLES-QUALITE-QUANTITE-"
  second$RES <- "+"
  second["NUMRES"] <- list(NULL)
  third <- first
  third$ELTID <- "L2"
  third$EVTID <- "E2"
  third$TYPE <- "UNKNOWN"
  list(transport_0 = first, transport_1 = second, transport_2 = third)
}

test_that("pancarte retains measurements and reads ELTID from fields", {
  out <- pancarte_normalize(pancarte_test_records())

  expect_identical(nrow(out), 3L)
  expect_identical(out$ELTID, c("L1", "L1", "L2"))
  expect_identical(out$STRRES, c("89", "+", "89"))
  expect_identical(out$NUMRES, c(89, NA_real_, 89))
  expect_identical(out$PATAGE, rep(65, 3))
  expect_identical(out$TYPE_LABEL, c("Pouls bts/min", "Selles (qualité/quantité)", NA_character_))
  expect_false(any(c("RES", "JOURMES") %in% names(out)))
  expect_false(any(vapply(out, is.list, logical(1))))
  expect_true(all(is.na(out$CMT)))
  expect_s3_class(out$DATEMES, "POSIXct")
  expect_identical(as.character(out$HEURE_DATEMES), rep("08:30:00", 3))
  expect_true(all(is.na(out$HEURE_DATENT)))
  expect_identical(format(out$DATENT, "%Y-%m-%d"), rep("2024-01-01", 3))
})

test_that("pancarte preserves numeric identifiers without scientific notation", {
  raw <- pancarte_test_records()[1]
  raw[[1]]$PATID <- 100000
  raw[[1]]$EVTID <- 200000
  raw[[1]]$ELTID <- 300000
  out <- pancarte_normalize(raw)

  expect_identical(out$PATID, "100000")
  expect_identical(out$EVTID, "200000")
  expect_identical(out$ELTID, "300000")
})

test_that("pancarte unit lists remain single atomic cells", {
  raw <- pancarte_test_records()[1]
  raw[[1]]$SEJUF <- list("F1", "F2")
  raw[[1]]$SEJUM <- list("U1", "U2")
  out <- pancarte_normalize(raw)

  expect_identical(nrow(out), 1L)
  expect_identical(out$SEJUF, "F1;F2")
  expect_identical(out$SEJUM, "U1;U2")
})

test_that("empty pancarte results expose typed measurement columns", {
  out <- pancarte_normalize(list())

  expect_identical(nrow(out), 0L)
  expect_identical(out$ELTID, character())
  expect_identical(out$TYPE_LABEL, character())
  expect_identical(out$STRRES, character())
  expect_identical(out$NUMRES, numeric())
  expect_s3_class(out$DATEMES, "POSIXct")
  expect_s3_class(out$HEURE_DATEMES, "hms")
})

test_that("pancarte labels refresh without expanding or dropping rows", {
  out <- label_pancarte(data.frame(
    TYPE = c("ACTIVITE", "TOTAL", "UNKNOWN", "ACTIVITE"),
    TYPE_LABEL = rep("old", 4)
  ))

  expect_identical(nrow(out), 4L)
  expect_identical(out$TYPE_LABEL, c("Activité", "Total", NA_character_, "Activité"))
  reference <- edsan_reference("pancarte")
  expect_identical(names(reference), c("TYPE", "TYPE_LABEL"))
  expect_identical(nrow(reference), 232L)
  expect_identical(anyDuplicated(reference$TYPE), 0L)
})

test_that("pancarte retrieval combines ID batches and preserves raw records", {
  raw <- pancarte_test_records()
  calls <- list()
  testthat::local_mocked_bindings(
    .edsan_call = function(module, query, what, fields = NULL, ...) {
      calls[[length(calls) + 1L]] <<- query
      keep <- vapply(raw, function(record) record$EVTID == query$EVTID, logical(1))
      list(ok = TRUE, value = raw[keep], error = NULL)
    },
    .package = "redsan"
  )

  out <- edsan_get("pancarte", query = list(EVTID = c("E1", "E2")), max_in_ids = 1)
  expect_identical(out$ELTID, c("L1", "L1", "L2"))
  expect_identical(length(calls), 2L)
  expect_true("TYPE_LABEL" %in% names(out))
  expect_identical(
    edsan_get("pancarte", query = list(EVTID = "E1"), process = FALSE),
    raw[1:2]
  )
})

test_that("pancarte table responses and identifier triplets survive retrieval", {
  flat <- data.frame(
    PATID = "P1", EVTID = "E1", ELTID = c("L1", "L1"),
    DATEMES = "2024-01-01T08:30:00", TYPE = c("POULS-BTS-MIN", "EVA"),
    RES = c("89", "2"), NUMRES = c(89, 2)
  )
  testthat::local_mocked_bindings(
    .edsan_call = function(module, query, what, fields = NULL, ...) {
      value <- if (what == "idtriplets") flat[c("ELTID", "EVTID", "PATID")] else flat
      list(ok = TRUE, value = value, error = NULL)
    },
    .package = "redsan"
  )

  out <- edsan_get("pancarte", query = list(EVTID = "E1"))
  expect_identical(out$NUMRES, c(89, 2))
  expect_identical(out$ELTID, c("L1", "L1"))
  ids <- edsan_get("pancarte", what = "idtriplets", query = list(EVTID = "E1"))
  expect_setequal(names(ids), c("ELTID", "EVTID", "PATID"))
  expect_false("TYPE_LABEL" %in% names(ids))
})

test_that("pancarte is registered with measurement time and its normalizer", {
  spec <- edsan_source_catalog("pancarte")

  expect_identical(spec$table, "results")
  expect_identical(spec$grain, "measurement")
  expect_identical(spec$source_time_start, "DATEMES")
  expect_identical(spec$query_date_keys, list("DATEMES"))
  expect_identical(spec$default_batch_key, "DATEMES")
  expect_identical(spec$normalizer, "pancarte_normalize")
  expect_error(
    edsan_get("pancarte", query = list(DATEXAM = "{2024-01-01,2024-01-02}")),
    "pancarte module only supports DATEMES"
  )
})

test_that("event bundles retain and label every pancarte measurement", {
  source <- pancarte_normalize(pancarte_test_records())
  source$TYPE_LABEL <- NULL
  bundles <- edsan_event_bundles(c("E2", "E1", "E3"), list(pancarte = source))

  expect_identical(bundles$E1$sources$pancarte$ELTID, c("L1", "L1"))
  expect_identical(bundles$E2$sources$pancarte$ELTID, "L2")
  expect_identical(nrow(bundles$E3$sources$pancarte), 0L)
  expect_true("TYPE_LABEL" %in% names(bundles$E3$sources$pancarte))
  expect_identical(bundles$E1$sources$pancarte$TYPE_LABEL[[1]], "Pouls bts/min")

  testthat::local_mocked_bindings(
    .edsan_call = function(...) list(ok = TRUE, value = pancarte_test_records(), error = NULL),
    .package = "redsan"
  )
  retrieved <- edsan_get_event_bundle("E1", modules = "pancarte")
  expect_identical(retrieved$sources$pancarte, bundles$E1$sources$pancarte)
})

test_that("pancarte infers time batches from query date ranges", {
  for (range in list("{2024-01-01,2024-12-31}", c("2024-01-01", "2024-12-31"))) {
    calls <- character()
    testthat::local_mocked_bindings(
      .edsan_call = function(module, query, what, fields = NULL, ...) {
        calls <<- c(calls, query$DATEMES)
        record <- pancarte_test_records()[[1L]]
        record$JOURMES <- substr(query$DATEMES, 2, 11)
        record$DATEMES <- paste0(record$JOURMES, "T08:30:00")
        value <- stats::setNames(list(record), paste0("batch_", length(calls)))
        list(ok = TRUE, value = value, error = NULL)
      },
      .package = "redsan"
    )

    out <- edsan_get("pancarte", query = list(DATEMES = range))

    expect_identical(calls, c("{2024-01-01,2024-06-30}", "{2024-07-01,2024-12-31}"))
    expect_identical(format(out$DATEMES, "%Y-%m-%d"), c("2024-01-01", "2024-07-01"))
    expect_identical(out$ELTID, c("L1", "L1"))
  }
})

test_that("pancarte date batching recovers from backend limits without repeating completed periods", {
  full_range <- "{2024-01-01,2024-12-31}"
  for (probe in c(FALSE, TRUE)) {
    calls <- character()
    testthat::local_mocked_bindings(
      .edsan_call = function(module, query, what, fields = NULL, ...) {
        calls <<- c(calls, query$DATEMES)
        if (query$DATEMES %in% c(full_range, "{2024-07-01,2024-12-31}")) {
          return(list(ok = FALSE, value = NULL, error = "Allowed size limit exceeded"))
        }
        record <- pancarte_test_records()[[1L]]
        record$JOURMES <- substr(query$DATEMES, 2, 11)
        record$DATEMES <- paste0(record$JOURMES, "T08:30:00")
        value <- stats::setNames(list(record), paste0("batch_", length(calls)))
        list(ok = TRUE, value = value, error = NULL)
      },
      .package = "redsan"
    )

    out <- edsan_get(
      "pancarte", query = list(DATEMES = full_range),
      batch_on_error_only = probe, return_audit = TRUE
    )

    expected_calls <- c(
      "{2024-01-01,2024-06-30}", "{2024-07-01,2024-12-31}",
      "{2024-07-01,2024-09-30}", "{2024-10-01,2024-12-31}"
    )
    if (probe) expected_calls <- c(full_range, expected_calls)
    expect_identical(calls, expected_calls)
    expect_identical(
      format(out$data$DATEMES, "%Y-%m-%d"),
      c("2024-01-01", "2024-07-01", "2024-10-01")
    )
    expect_identical(out$data$ELTID, rep("L1", 3))
    expect_equal(sum(!out$audit$ok), 1L + as.integer(probe))
  }
})
