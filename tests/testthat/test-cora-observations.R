test_that("CORA observation type selection uses active catalog identifiers", {
  catalog <- tibble::tibble(
    NOSOUSVOLET = c("443", "270", "285"),
    CODE = c("DIET", "KINE", "ERGO"),
    LIBLONG = c("Diet", "Kiné", "Ergo")
  )
  expect_identical(
    redsan:::.cora_resolve_observation_types(c("DIET", "KINE"), catalog)$CODE,
    c("DIET", "KINE")
  )
  expect_error(
    redsan:::.cora_resolve_observation_types("TESTS", catalog),
    "Unknown or inactive"
  )
  expect_error(
    redsan:::.cora_resolve_observation_types("DIET' OR 1=1", catalog),
    "Unknown or inactive"
  )
})

test_that("generic SQL uses H/R paths and actual CORA sous-volet IDs", {
  sql <- redsan:::.cora_observation_documents_sql(
    "745068610", c("443", "270")
  )
  expect_match(sql, "ICSF.MVTUS", fixed = TRUE)
  expect_match(sql, "ICSF.T_EVT_RDV", fixed = TRUE)
  expect_match(sql, "d.NOSOUSVOLET IN (443, 270)", fixed = TRUE)
  expect_match(sql, "d.ETATDOC = 1", fixed = TRUE)
  expect_match(sql, "d.TYPEEVT = 'H'", fixed = TRUE)
  expect_match(sql, "d.TYPEEVT = 'R'", fixed = TRUE)
  expect_error(
    redsan:::.cora_observation_documents_sql("1", "443); DROP TABLE X"),
    "numeric CORA sub-volet"
  )
})

test_that("generic observation SQL batches 900 IEPs", {
  seen <- integer()
  fake_query <- function(connection, sql) {
    matches <- regmatches(sql, gregexpr("'[0-9]+'", sql))[[1L]]
    seen <<- c(seen, length(matches))
    tibble::tibble(IEP = gsub("'", "", matches, fixed = TRUE),
                   DTDOC = as.Date("2026-01-01"))
  }
  out <- redsan:::.cora_query_observation_documents(
    NULL, as.character(seq_len(1001L)), "443", "H",
    query_fn = fake_query
  )
  expect_identical(seen, c(900L, 101L))
  expect_equal(nrow(out), 1001L)
})

test_that("CORA rows retain compound IDs, provenance and raw text", {
  observations <- tibble::tibble(
    EVTID = c("101", "101", "102"),
    IEP = c("1", "1", "2"),
    NODOCUMENT = c("55", "55", "55"),
    TYPEDOC = c("W", "W", "X"),
    NOSOUSVOLET = c("443", "443", "270"),
    OBS_CODE = c("DIET", "DIET", "KINE"),
    OBS_LABEL = c("Diet", "Diet", "Kiné"),
    CORA_NOEVT = c("5", "5", "7"),
    TYPEEVT = c("H", "H", "R"),
    DTDOC = as.Date(c("2026-01-01", "2026-01-01", "2026-01-02")),
    TEXT = c("Poids 60 kg", "Poids 60 kg", "Marche")
  )
  bundles <- edsan_event_bundles(
    c("101", "102"),
    list(doceds = tibble::tibble(
      EVTID = "101", ELTID = "original", RECTXT = "Existing",
      RECTYPE = "CR", RECTYPE_LABEL = "Compte rendu"
    ))
  )
  joined <- cora_add_observations(bundles, observations)
  first <- joined[["101"]]$sources$doceds
  expect_equal(nrow(first), 2L)
  expect_identical(first$ELTID, c("original", "CORA:W:55"))
  expect_identical(first$RECTXT, c("Existing", "Poids 60 kg"))
  expect_identical(first$RECTYPE[[2L]], "FICHE_DIET")
  expect_identical(first$SOURCE[[2L]], "CORA")
  expect_identical(first$NODOCUMENT[[2L]], "55")
  expect_identical(first$TYPEDOC[[2L]], "W")
  second <- joined[["102"]]$sources$doceds
  expect_identical(second$ELTID, "CORA:X:55")
  expect_identical(second$RECTYPE, "CORA_KINE")
  expect_identical(second$TYPEEVT, "R")
  expect_identical(
    cora_add_observations(joined, observations),
    joined
  )
})

test_that("observations can add a DOCEDS source without EDSaN DOCEDS", {
  bundle <- edsan_event_bundle("101", list(
    biol = tibble::tibble(EVTID = "101", NUMRES = 42)
  ))
  observations <- tibble::tibble(
    EVTID = "101", NODOCUMENT = "7", TYPEDOC = "W",
    OBS_CODE = "DIET", NOSOUSVOLET = "443", TYPEEVT = "H",
    CORA_NOEVT = "8", TEXT = "Dénutrition"
  )
  out <- cora_add_observations(bundle, observations)
  expect_s3_class(out, "edsan_event_bundle")
  expect_identical(out$sources$biol$NUMRES, 42)
  expect_identical(out$sources$doceds$RECTXT, "Dénutrition")
  expect_identical(
    cora_add_observations(out, observations)$sources$doceds,
    out$sources$doceds
  )
})

test_that("no CORA observations preserves existing DOCEDS data", {
  bundle <- edsan_event_bundle("101", list(
    doceds = tibble::tibble(EVTID = "101", ELTID = "D1", RECTXT = "Original")
  ))
  expect_identical(
    cora_add_observations(bundle, redsan:::.cora_empty_observations()),
    bundle
  )
})

test_that("unmapped CORA rows cannot be accidentally joined", {
  bundle <- edsan_event_bundle("101", list(
    doceds = tibble::tibble(EVTID = "101", ELTID = "D1")
  ))
  observations <- tibble::tibble(
    EVTID = NA_character_, NODOCUMENT = "7", TYPEDOC = "W",
    OBS_CODE = "DIET", NOSOUSVOLET = "443", TYPEEVT = "H",
    CORA_NOEVT = "8", TEXT = "Not safely mapped"
  )
  expect_error(cora_add_observations(bundle, observations), "without EVTID")
})

test_that("retrieval joins CORA catalog, CT mapping and documents", {
  fake_map <- function(ids, id_type, env, ks_path) {
    tibble::tibble(EVTID = evtid, IEP = expected_iep)
  }
  fake_catalog <- function(connection, active_only = TRUE) {
    tibble::tibble(NOSOUSVOLET = "443", CODE = "DIET",
                   LIBLONG = "Diététique")
  }
  fake_docs <- function(connection, ieps, subvolets, event_type) {
    expect_identical(ieps, expected_iep)
    expect_identical(subvolets, "443")
    tibble::tibble(
      IEP = returned_iep, NODOCUMENT = "55", TYPEDOC = "W",
      CORA_NOEVT = "7", NOSOUSVOLET = "443", TYPEEVT = "H",
      DTDOC = as.POSIXct("2026-01-01", tz = "UTC"),
      TIMECREATE = as.POSIXct("2026-01-01", tz = "UTC"),
      REDACTEUR = "Clinician", NOUSHEB = "1", NOUSRESP = "2"
    )
  }
  testthat::local_mocked_bindings(
    .cora_diet_id_map = fake_map,
    cora_observation_catalog = fake_catalog,
    .cora_query_observation_documents = fake_docs,
    .cora_query = function(sql, connection) {
      expect_match(sql, "s.NO_SEJOUR IN (", fixed = TRUE)
      tibble::tibble(NO_SEJOUR = returned_iep, ID_RSS = "1001")
    },
    .cora_read_diet_blob = function(...) "Poids 60 kg",
    .package = "redsan"
  )
  evtid <- "123"
  connection <- structure(list(), class = "fake_connection")
  fixtures <- list("000456", "000456    ", "000456\u00a0\u2007", 456000000)
  for (returned_iep in fixtures) {
    expected_iep <- if (is.numeric(returned_iep)) "456000000" else "000456"
    by_evtid <- cora_get_diet(evtid, id_type = "EVTID", connection = connection)
    by_iep <- cora_get_diet(expected_iep, id_type = "IEP", connection = connection)

    expect_identical(by_evtid, by_iep)
    expect_identical(by_evtid$IEP, expected_iep)
    expect_identical(by_evtid$NO_SEJOUR, expected_iep)
    expect_identical(by_evtid$ID_RSS, "1001")
    expect_identical(by_evtid$EVTID, evtid)
    expect_identical(by_evtid$OBS_CODE, "DIET")
    expect_identical(by_evtid$TEXT, "Poids 60 kg")
  }
})

test_that("ambiguous EVTID/IEP mappings fail rather than multiplying data", {
  testthat::local_mocked_bindings(
    .cora_diet_id_map = function(...) {
      tibble::tibble(EVTID = c("1", "2"), IEP = c("42", "42"))
    },
    .package = "redsan"
  )
  expect_error(cora_get_observations(
    c("1", "2"), id_type = "EVTID",
    connection = structure(list(), class = "fake_connection")
  ), "Ambiguous")
})

test_that("PATID and IPP retrieve each CORA stay even without an EVTID", {
  connection <- structure(list(), class = "fake_connection")
  patient_lookups <- 0L
  testthat::local_mocked_bindings(
    edsan_ct = function(ids, from, env, ks_path) {
      patient_lookups <<- patient_lookups + 1L
      expect_identical(ids, c("10", "20"))
      expect_identical(from, "PATID")
      expect_identical(env, "ct-example")
      expect_identical(ks_path, "keystore-example")
      tibble::tibble(PATID = ids, IPP = c("00001234", NA_character_),
                     n_matches = c(1L, 0L))
    },
    .cora_query = function(sql, connection) {
      expect_s3_class(connection, "fake_connection")
      if (grepl("TB_SYNTH_RSS", sql, fixed = TRUE)) {
        expect_match(sql, "r.ID_SEJOUR = s.ID_SEJOUR", fixed = TRUE)
        expect_match(sql, "s.NO_SEJOUR IN ('001', '002')", fixed = TRUE)
        return(tibble::tibble(
          NO_SEJOUR = c("001   ", "001", "001"),
          ID_RSS = c("502", "501", "501")
        ))
      }
      expect_match(sql, "p.IPP_PATIENT IN ('00001234')", fixed = TRUE)
      tibble::tibble(IEP = c("001   ", "002", "001", NA_character_))
    },
    .edsan_ct_legacy_pseudonymize = function(ids, id_type, env, ks_path) {
      expect_identical(ids, c("001", "002"))
      expect_identical(id_type, "IEP")
      expect_identical(env, "ct-example")
      expect_identical(ks_path, "keystore-example")
      tibble::tibble(HIS_ID = ids, EDSAN_ID = c("101", NA_character_))
    },
    cora_observation_catalog = function(connection) {
      tibble::tibble(NOSOUSVOLET = "443", CODE = "DIET", LIBLONG = "Diet")
    },
    .cora_query_observation_documents = function(connection, ieps, subvolets,
                                                 event_type) {
      expect_identical(ieps, c("001", "002"))
      expect_identical(subvolets, "443")
      expect_identical(event_type, c("H", "R"))
      tibble::tibble(
        IEP = ieps, NODOCUMENT = c("51", "52"), TYPEDOC = "W",
        CORA_NOEVT = c("1", "2"), NOSOUSVOLET = "443", TYPEEVT = c("H", "R"),
        DTDOC = as.POSIXct("2026-01-01", tz = "UTC"),
        TIMECREATE = as.POSIXct("2026-01-01", tz = "UTC"),
        REDACTEUR = "Clinician", NOUSHEB = NA_character_,
        NOUSRESP = NA_character_
      )
    },
    .cora_read_diet_blob = function(connection, nodocument, ...) nodocument,
    .package = "redsan"
  )
  out <- cora_get_diet(
    c("10", "20"), "PATID", connection = connection,
    env = "ct-example", ks_path = "keystore-example"
  )
  expect_identical(out$IEP, c("001", "002"))
  expect_identical(out$NO_SEJOUR, out$IEP)
  expect_identical(out$ID_RSS, c("501, 502", NA_character_))
  expect_identical(out$EVTID, c("101", NA_character_))
  expect_identical(out$TEXT, c("51", "52"))

  by_ipp <- cora_get_observations(
    "00001234", "IPP", types = "DIET", connection = connection,
    env = "ct-example", ks_path = "keystore-example"
  )
  expect_identical(by_ipp, out)
  expect_identical(patient_lookups, 1L)
})

test_that("unmatched and ambiguous PATIDs do not query CORA", {
  ipps <- NA_character_
  testthat::local_mocked_bindings(
    edsan_ct = function(...) {
      tibble::tibble(IPP = ipps, n_matches = sum(!is.na(ipps)))
    },
    .cora_connect = function(...) stop("Unexpected CORA connection"),
    .package = "redsan"
  )
  expect_identical(
    cora_get_observations("10", "PATID"),
    redsan:::.cora_empty_observations()
  )
  ipps <- c("00001234", "00005678")
  expect_error(cora_get_observations("10", "PATID"), "Ambiguous EDSaN PATID/IPP")
})

test_that("a PATID without CORA stays returns the standard empty table", {
  testthat::local_mocked_bindings(
    edsan_ct = function(...) tibble::tibble(IPP = "00001234", n_matches = 1L),
    .cora_query = function(sql, connection) tibble::tibble(IEP = character()),
    cora_observation_catalog = function(...) stop("Unexpected catalog lookup"),
    .package = "redsan"
  )
  expect_identical(
    cora_get_observations("10", "PATID", connection = "fake connection"),
    redsan:::.cora_empty_observations()
  )
})

test_that("bundle retrieval opts into CORA only when requested", {
  calls <- character()
  fake_edsan <- function(module, what, query, process) {
    calls <<- c(calls, module)
    tibble::tibble(EVTID = query$EVTID, ELTID = "EDSAN:D1",
                   RECTXT = "Existing", RECTYPE = "CR")
  }
  fake_cora <- function(ids, id_type, types) {
    expect_identical(ids, c("101", "102"))
    expect_identical(id_type, "EVTID")
    expect_identical(types, "DIET")
    tibble::tibble(
      EVTID = "101", NODOCUMENT = "7", TYPEDOC = "W",
      OBS_CODE = "DIET", NOSOUSVOLET = "443", TYPEEVT = "H",
      CORA_NOEVT = "8", TEXT = "Nutrition"
    )
  }
  testthat::local_mocked_bindings(
    edsan_get = fake_edsan,
    cora_get_observations = fake_cora,
    .package = "redsan"
  )
  baseline <- edsan_get_event_bundles(c("101", "102"), modules = "doceds")
  expect_identical(nrow(baseline[["101"]]$sources$doceds), 1L)
  out <- edsan_get_event_bundles(c("101", "102"),
    modules = "doceds", include_observations = "DIET"
  )
  expect_identical(calls, c("doceds", "doceds"))
  expect_identical(nrow(out[["101"]]$sources$doceds), 2L)
  expect_identical(nrow(out[["102"]]$sources$doceds), 1L)
})
