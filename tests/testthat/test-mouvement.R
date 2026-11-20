mouvement_records <- function() {
  list(
    a = list(PATID = "P1", EVTID = "E1", ELTID = "L1", PATAGE = "65",
             PATSEX = "F", DATENT = "2024-01-01T08:30:00", DATSORT = "2024-01-02",
             SEJUF = "F1", SEJUM = "U1"),
    b = list(PATID = "P1", EVTID = "E1", ELTID = "L2", PATAGE = "65",
             PATSEX = "F", DATENT = "2024-01-02", DATSORT = NULL,
             SEJUF = list("F1", "F2"), SEJUM = "U2")
  )
}

test_that("movement normalization retains rows and normalizes fields", {
  out <- mouvement_normalize(mouvement_records())
  expect_identical(out$ELTID, c("L1", "L2"))
  expect_identical(out$PATAGE, c(65, 65))
  expect_identical(out$SEJUF, c("F1", "F1;F2"))
  expect_s3_class(out$DATENT, "POSIXct")
  expect_identical(as.character(out$HEURE_DATENT)[[1]], "08:30:00")
  expect_false(any(vapply(out, is.list, logical(1))))
  expect_identical(nrow(mouvement_normalize(list())), 0L)
})

test_that("movement retrieval combines tables and supports raw records", {
  raw <- mouvement_records()
  testthat::local_mocked_bindings(
    .edsan_call = function(module, query, what, fields = NULL, ...) {
      expect_identical(module, "mouvement")
      value <- if (query$EVTID == "E1") raw else raw[0]
      list(ok = TRUE, value = value, error = NULL)
    }, .package = "redsan"
  )
  expect_identical(edsan_get("mouvement", query = list(EVTID = "E1"))$ELTID,
                   c("L1", "L2"))
  expect_identical(edsan_get("mouvement", query = list(EVTID = "E1"), process = FALSE), raw)
  bundle <- edsan_get_event_bundle("E1", modules = "mouvement")
  expect_identical(bundle$sources$mouvement$ELTID, c("L1", "L2"))
})

test_that("movement date windows use bounds on the selected key", {
  spec <- edsan_source_catalog("mouvement")
  expect_identical(spec$query_date_keys, list(c("DATENT", "DATSORT")))
  bounds <- redsan:::.edsan_infer_batch_window(
    "mouvement", list(DATSORT = "{2024-01-01,2024-01-31}"), "DATSORT"
  )
  expect_identical(bounds$start, as.Date("2024-01-01"))
  expect_identical(bounds$end, as.Date("2024-01-31"))
  table <- data.frame(PATID = "P1", EVTID = "E1", ELTID = "L1")
  combined <- redsan:::.edsan_combine("mouvement", list(table, table))
  expect_identical(nrow(combined), 2L)
})
