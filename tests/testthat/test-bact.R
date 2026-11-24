bact_records <- function() {
  list(
    a = list(PATID = "P1", EVTID = "E1", ELTID = "L1", PATSEX = "F",
      PATAGE = "65", DATPRELEV = "2024-01-01", HEURE_PRELEV = "08:30",
      SEJUM = "U1", SEJUF = "F1", NATUREPVT = "urine", IDENTIFICATION = "E. coli",
      LBLANA = "antibiotic", LBLRES = "resistant", CMI = "<=0.5",
      STRRES = "R", NUMRES = "0.5", ST = "final", CASFM = "2024"),
    b = list(PATID = "P1", EVTID = "E1", ELTID = "L1", PATSEX = "F",
      PATAGE = "65", DATPRELEV = "2024-01-01", HEURE_PRELEV = "08:30",
      SEJUM = "U1", SEJUF = "F1", NATUREPVT = "urine", IDENTIFICATION = "E. coli",
      LBLANA = "other", LBLRES = "sensitive", CMI = ">2", STRRES = "S",
      NUMRES = NULL, ST = "final", CASFM = "2024")
  )
}

test_that("bact preserves results, collection time and susceptibility values", {
  out <- bact_normalize(bact_records())
  expect_identical(nrow(out), 2L)
  expect_identical(out$ELTID, c("L1", "L1"))
  expect_identical(out$CMI, c("<=0.5", ">2"))
  expect_identical(out$HEURE_PRELEV, c("08:30", "08:30"))
  expect_identical(out$PATAGE, c(65, 65))
  expect_s3_class(out$DATPRELEV, "POSIXct")
  expect_identical(nrow(bact_normalize(list())), 0L)
})

test_that("bact supports retrieval and event bundles", {
  raw <- bact_records()
  testthat::local_mocked_bindings(
    .edsan_call = function(module, query, what, fields = NULL, ...) {
      expect_identical(module, "bact")
      list(ok = TRUE, value = raw, error = NULL)
    }, .package = "redsan"
  )
  expect_identical(edsan_get("bact", query = list(EVTID = "E1"))$CMI, c("<=0.5", ">2"))
  expect_identical(edsan_get("bact", query = list(EVTID = "E1"), process = FALSE), raw)
  expect_identical(nrow(edsan_get_event_bundle("E1", modules = "bact")$sources$bact), 2L)
  expect_false("bact" %in% redsan:::.resolve_event_bundle_modules("all"))
})

test_that("bact batching accepts the collection date", {
  spec <- edsan_source_catalog("bact")
  expect_identical(spec$query_date_keys, list("DATPRELEV"))
  bounds <- redsan:::.edsan_infer_batch_window(
    "bact", list(DATPRELEV = "{2024-01-01,2024-01-31}"), "DATPRELEV"
  )
  expect_identical(bounds$start, as.Date("2024-01-01"))
  table <- data.frame(PATID = "P1", EVTID = "E1", ELTID = "L1")
  expect_identical(nrow(redsan:::.edsan_combine("bact", list(table, table))), 2L)
})


test_that("bact trims selected text fields without changing internal spaces or other fields", {
  raw <- data.frame(
    NATUREPVT = c("  urine  ", NA_character_),
    IDENTIFICATION = c(" E.  coli ", "  "),
    LBLANA = c(" antibiotic ", " x "),
    LBLRES = c(" resistant ", " y "),
    STRRES = c(" R ", " S "), CMI = c(" <=0.5 ", " >2 ")
  )
  out <- bact_normalize(raw)
  expect_identical(out$NATUREPVT, c("urine", NA_character_))
  expect_identical(out$IDENTIFICATION, c("E.  coli", ""))
  expect_identical(out$LBLANA, c("antibiotic", "x"))
  expect_identical(out$LBLRES, c("resistant", "y"))
  expect_identical(out$STRRES, c("R", "S"))
  expect_identical(out$CMI, raw$CMI)
})


test_that("bact converts the actual DATEPRELEV field without changing the time", {
  raw <- bact_records()
  for (i in seq_along(raw)) {
    raw[[i]]$DATEPRELEV <- raw[[i]]$DATPRELEV
    raw[[i]]$HEUREPRELEV <- raw[[i]]$HEURE_PRELEV
    raw[[i]]$DATPRELEV <- NULL
    raw[[i]]$HEURE_PRELEV <- NULL
  }
  out <- bact_normalize(raw)
  expect_s3_class(out$DATEPRELEV, "POSIXct")
  expect_identical(format(out$DATEPRELEV, "%Y-%m-%d", tz = "Europe/Paris"),
                   c("2024-01-01", "2024-01-01"))
  expect_identical(out$HEUREPRELEV, c("08:30", "08:30"))
  expect_false(anyNA(out$DATEPRELEV))
})
