test_that("process_viro preserves virology metadata and result payload columns", {
  # Rationale: process-output contract. VIRO normalization should mirror BIOL
  # while keeping the VIRO-specific identifier and source date.
  raw <- list(
    viroA = list(
      PATID = "P1",
      EVTID = "E1",
      DATEPRELEV = "2024-01-01 08:30",
      SEJUM = "2024-01-01",
      SEJUF = "2024-01-03",
      PATBD = "1980-01-01",
      PATAGE = "44",
      PATSEX = "M",
      RESULTATS = data.frame(
        TYPEANA = c("PCR", "PCR"),
        LABEL_RESULT = c("Virus A", "Virus B"),
        CODELOINC = c("111", "222"),
        STRRES = c("positive", "negative"),
        NUMRES = I(list(12.5, NA_real_)),
        CMT = c("ok", NA_character_)
      )
    ),
    viroB = list(
      PATID = "P2",
      EVTID = "E2",
      DATEPRELEV = "2024-01-02",
      RESULTATS = data.frame()
    )
  )

  out <- process_viro(raw)

  expect_equal(out$PATID, rep("P1", 2))
  expect_equal(out$EVTID, rep("E1", 2))
  expect_equal(out$VIRO_ID, rep("viroA", 2))
  expect_false("ELTID" %in% names(out))
  expect_true(inherits(out$DATEPRELEV, "POSIXct"))
  expect_equal(as.character(out$HEURE_DATEPRELEV), rep("08:30:00", 2))
  expect_equal(out$PATAGE, rep(44, 2))
  expect_equal(out$NUMRES, c(12.5, NA_real_))
  expect_type(out$NUMRES, "double")
  expect_equal(out$STRRES, c("positive", "negative"))
})

test_that("get_edsan supports VIRO source contract and normalization", {
  # Rationale: retrieval contract. VIRO should be selectable like existing EDSAN
  # modules and should batch on DATEPRELEV by default.
  raw <- list(
    viroA = list(
      PATID = "P1",
      EVTID = "E1",
      DATEPRELEV = "2024-01-01",
      RESULTATS = data.frame(TYPEANA = "PCR", STRRES = "positive")
    )
  )

  out <- with_mocked_bindings(
    get_edsan("viro", query = list(DATEPRELEV = "{2024-01-01,2024-01-31}")),
    .edsan_call = function(module, query, what, fields = NULL, ...) {
      expect_equal(module, "viro")
      expect_equal(query$DATEPRELEV, "{2024-01-01,2024-01-31}")
      list(ok = TRUE, value = raw, error = NULL)
    },
    .package = "redsan"
  )

  expect_equal(out$VIRO_ID, "viroA")
  expect_equal(out$STRRES, "positive")
})
