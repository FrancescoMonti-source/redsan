bactibs_record <- function() {
  list(PATID = "P1", EVTID = "E1", ELTID = "L1", PATAGE = "65", PATSEX = "F",
       DATEPRELEV = "2024-01-01", HEUREPRELEV = "08:30", SEJUM = "U1", SEJUF = "F1",
       NATUREPVT = " urine ", IDENTIFICATION = " E. coli ", LBLANA = " antibiotic ",
       LBLRES = " resistant ", STRRES = " R ", CMI = "<=0.5", NUMRES = "0.5",
       ST = "final", CASFM = "2024", DLVL = "1", TRI = "2", TYPEANA = "A")
}

test_that("bactibs preserves fields and applies bacteriology normalization", {
  raw <- list(a = bactibs_record())
  out <- bactibs_normalize(raw)
  expect_identical(out$PATID, "P1")
  expect_identical(out$PATAGE, 65)
  expect_s3_class(out$DATEPRELEV, "POSIXct")
  expect_identical(out$HEUREPRELEV, "08:30")
  expect_identical(out$NATUREPVT, "urine")
  expect_identical(out$IDENTIFICATION, "E. coli")
  expect_identical(out$LBLANA, "antibiotic")
  expect_identical(out$LBLRES, "resistant")
  expect_identical(out$STRRES, "R")
  expect_true(all(c("DLVL", "TRI", "TYPEANA") %in% names(out)))
  expect_identical(nrow(bactibs_normalize(list())), 0L)
})

test_that("bactibs retrieval, batches and event bundles work independently of bact", {
  raw <- list(a = bactibs_record())
  testthat::local_mocked_bindings(
    .edsan_call = function(module, query, what, fields = NULL, ...) {
      expect_identical(module, "bactibs")
      list(ok = TRUE, value = raw, error = NULL)
    }, .package = "redsan"
  )
  expect_identical(edsan_get("bactibs", query = list(EVTID = "E1"))$ELTID, "L1")
  expect_identical(edsan_get("bactibs", query = list(EVTID = "E1"), process = FALSE), raw)
  expect_identical(edsan_get_event_bundle("E1", modules = "bactibs")$sources$bactibs$CMI, "<=0.5")
  expect_false("bactibs" %in% redsan:::.resolve_event_bundle_modules("all"))
  expect_identical(edsan_source_catalog("bactibs")$default_batch_key, "DATEPRELEV")
  bounds <- redsan:::.edsan_infer_batch_window(
    "bactibs", list(DATEPRELEV = "{2024-01-01,2024-01-31}"), "DATEPRELEV"
  )
  expect_identical(bounds$start, as.Date("2024-01-01"))
})

test_that("bactibs requests a range one date at a time and drops unrelated rows", {
  days <- c("2026-05-21", "2026-05-22", "2026-05-23")
  sent <- character()
  testthat::local_mocked_bindings(
    .edsan_call = function(module, query, what, fields = NULL, ...) {
      day <- query$DATEPRELEV
      sent <<- c(sent, day)
      expect_named(query, "DATEPRELEV")
      expect_setequal(fields, c("ELTID", "DATEPRELEV"))
      list(ok = TRUE, value = tibble::tibble(
        ELTID = c(day, "unrelated"), DATEPRELEV = c(day, "2025-05-14")
      ), error = NULL)
    }, .package = "redsan"
  )

  out <- edsan_get("bactibs", query = list(DATEPRELEV = "{2026-05-21,2026-05-23}"),
                   periods_by = "1 day",
                   batch_on_error_only = TRUE, fields = "ELTID", return_audit = TRUE)
  expect_identical(sent, days)
  expect_identical(out$data, tibble::tibble(ELTID = days))
  expect_identical(out$audit$period[!is.na(out$audit$period)], days)

  sent <- character()
  one_day <- edsan_get("bactibs", query = list(DATEPRELEV = days[[1]]), fields = "ELTID")
  expect_identical(sent, days[[1]])
  expect_identical(one_day$ELTID, days[[1]])

  sent <- character()
  expect_warning(get_edsan("bactibs", query = list(DATEPRELEV = range(days)), fields = "ELTID"),
                 "deprecated")
  expect_identical(sent, days)
})

test_that("a rejected bactibs day reports the date and stops the range", {
  sent <- character()
  testthat::local_mocked_bindings(
    .edsan_backend_function = function(use_data_endpoint) {
      function(api_mod, api_query, env) {
        day <- jsonlite::fromJSON(api_query)$DATEPRELEV
        sent <<- c(sent, day)
        if (day == "2026-05-22") return("Bad result")
        tibble::tibble(DATEPRELEV = day)
      }
    }, .package = "redsan"
  )

  expect_error(
    edsan_get("bactibs", query = list(DATEPRELEV = c("2026-05-21", "2026-05-23")),
              periods_by = "1 day"),
    "period=2026-05-22.*Bad result"
  )
  expect_identical(sent, c("2026-05-21", "2026-05-22"))
})

test_that("bactibs batches dates with OR at the selected granularity", {
  days <- c("2026-05-21", "2026-05-22", "2026-05-23")
  sent <- character()
  testthat::local_mocked_bindings(
    .edsan_call = function(module, query, what, fields = NULL, ...) {
      sent <<- c(sent, query$DATEPRELEV)
      expect_setequal(fields, c("ELTID", "DATEPRELEV"))
      list(ok = TRUE, value = tibble::tibble(
        ELTID = c(days, "outside"), DATEPRELEV = c(days, "2025-05-14")
      ), error = NULL)
    }, .package = "redsan"
  )

  query <- list(DATEPRELEV = range(days))
  out <- edsan_get("bactibs", query = query, fields = "ELTID")
  expect_identical(sent, days)
  expect_identical(out$ELTID, days)

  sent <- character()
  out <- edsan_get("bactibs", query = query, periods_by = "2 days",
                   fields = "ELTID", batch_on_error_only = TRUE, return_audit = TRUE)
  expect_identical(sent, c(paste(days[1:2], collapse = " OR "), days[[3]]))
  expect_identical(out$data$ELTID, days)
  expect_identical(out$audit$period[!is.na(out$audit$period)], sent)
})

test_that("bactibs caps monthly requests at seven dates and preserves month boundaries", {
  sent <- character()
  testthat::local_mocked_bindings(
    .edsan_call = function(module, query, what, fields = NULL, ...) {
      sent <<- c(sent, query$DATEPRELEV)
      days <- strsplit(query$DATEPRELEV, " OR ", fixed = TRUE)[[1L]]
      if (startsWith(days[[1L]], "{")) return(list(ok = TRUE, value = list(), error = NULL))
      if (length(days) > 7L) days <- tail(days, 1L)
      list(ok = TRUE, value = tibble::tibble(ELTID = days, DATEPRELEV = days), error = NULL)
    }, .package = "redsan"
  )

  query <- list(DATEPRELEV = "{2026-05-01,2026-06-03}")
  expect_error(edsan_get("bactibs", query = query, periods_by = "month", max_time_batches = 5L),
               "time_batch_too_many_batches: 6")
  expect_length(sent, 0L)

  out <- edsan_get("bactibs", query = query, periods_by = "month", return_audit = TRUE)
  requests <- out$audit[!is.na(out$audit$time_idx), ]
  expect_identical(sent, requests$period)
  expect_identical(lengths(strsplit(sent, " OR ", fixed = TRUE)), c(7L, 7L, 7L, 7L, 3L, 3L))
  expect_identical(as.character(requests$start),
                   c("2026-05-01", "2026-05-08", "2026-05-15", "2026-05-22", "2026-05-29", "2026-06-01"))
  expect_identical(out$data$ELTID,
                   as.character(seq(as.Date("2026-05-01"), as.Date("2026-06-03"), by = "day")))

  sent <- character()
  edsan_get("bactibs", query = query, periods_by = "month", process = FALSE)
  expect_identical(sent, c("{2026-05-01,2026-05-31}", "{2026-06-01,2026-06-03}"))
})
