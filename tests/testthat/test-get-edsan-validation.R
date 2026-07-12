test_that("module date keys are validated before backend calls", {
  # Rationale: retrieval input contract. Date-key mistakes should fail locally,
  # before a live EDSAN request is made with the wrong module semantics.
  expect_error(
    get_edsan("doceds", query = list(DATEXAM = "{2024-01-01,2024-01-31}")),
    "doceds module only supports RECDATE"
  )

  expect_error(
    get_edsan("pmsi", query = list(RECDATE = "{2024-01-01,2024-01-31}")),
    "pmsi module only supports DATENT and DATSORT"
  )

  expect_error(
    get_edsan("biol", query = list(DATENT = "{2024-01-01,2024-01-31}")),
    "biol module only supports DATEXAM"
  )
})

test_that("field parsing normalizes vectors and comma-separated strings", {
  # Rationale: retrieval input contract. The public `fields` argument accepts
  # both a vector and a comma string, and must produce the same backend request.
  expect_identical(
    redsan:::.edsan_parse_fields("PATID, EVTID,, ELTID"),
    c("PATID", "EVTID", "ELTID")
  )
  expect_identical(
    redsan:::.edsan_parse_fields(c("PATID", "EVTID", "PATID")),
    c("PATID", "EVTID")
  )
})

test_that("data payloads are normalized by module unless the escape is used", {
  # Rationale: retrieval/normalization contract. A what="data" call chains the
  # matching processor by default (process=TRUE); process=FALSE, idtriplets, and
  # doceds (no processor) return the payload unchanged.
  raw_pmsi <- list(list(
    PATID = "P1", EVTID = "E1", ELTID = "L1",
    DATENT = "2024-01-01 08:30", DATSORT = "2024-01-03",
    PATSEX = "M", DALL = "01:A41 02:I10"
  ))

  # default TRUE + what="data" + pmsi -> normalized tables
  out <- redsan:::.edsan_maybe_process(raw_pmsi, "pmsi", "data", TRUE)
  expect_named(out, c("main", "actes", "diag"))
  expect_equal(out$diag$diag, c("A41", "I10"))
  expect_equal(out$diag$PATSEX, c("M", "M"))

  # escape hatch: process=FALSE returns the raw list unchanged
  expect_identical(
    redsan:::.edsan_maybe_process(raw_pmsi, "pmsi", "data", FALSE),
    raw_pmsi
  )

  # idtriplets has nothing to normalize -> passthrough even with process=TRUE
  trip <- tibble::tibble(ELTID = "L1", EVTID = "E1", PATID = "P1")
  expect_identical(
    redsan:::.edsan_maybe_process(trip, "pmsi", "idtriplets", TRUE),
    trip
  )

  # doceds data is flat but still needs recorded-time normalization
  doc <- tibble::tibble(ELTID = "L1", RECDATE = "2024-01-01", RECTYPE = "CR")
  normalized_doc <- redsan:::.edsan_maybe_process(doc, "doceds", "data", TRUE)
  expect_true(inherits(normalized_doc$RECDATE, "POSIXct"))
  expect_true(is.na(normalized_doc$HEURE_RECDATE))
})

test_that("missing live backend is reported as a retrieval error", {
  # Rationale: retrieval integration contract. Local checks often lack d2imr;
  # the call helper should return an explicit backend error shape.
  skip_if(
    requireNamespace("d2imr", quietly = TRUE),
    "d2imr is installed in this environment"
  )

  result <- redsan:::.edsan_call("doceds", list(RECDATE = "{2024-01-01,2024-01-02}"))

  expect_false(result$ok)
  expect_match(result$error, "Package 'd2imr' is required")
})
