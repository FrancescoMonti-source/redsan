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
