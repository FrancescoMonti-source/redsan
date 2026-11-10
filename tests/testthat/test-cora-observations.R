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
    tibble::tibble(EVTID = ids, IEP = "745068610")
  }
  fake_catalog <- function(connection, active_only = TRUE) {
    tibble::tibble(NOSOUSVOLET = "443", CODE = "DIET",
                   LIBLONG = "Diététique")
  }
  fake_docs <- function(connection, ieps, subvolets, event_type) {
    expect_identical(ieps, "745068610")
    expect_identical(subvolets, "443")
    tibble::tibble(
      IEP = "745068610", NODOCUMENT = "55", TYPEDOC = "W",
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
    .cora_read_diet_blob = function(...) "Poids 60 kg",
    .package = "redsan"
  )
  out <- cora_get_observations(
    "199196450", id_type = "EVTID", types = "DIET",
    connection = structure(list(), class = "fake_connection")
  )
  expect_identical(out$EVTID, "199196450")
  expect_identical(out$OBS_CODE, "DIET")
  expect_identical(out$TEXT, "Poids 60 kg")
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
