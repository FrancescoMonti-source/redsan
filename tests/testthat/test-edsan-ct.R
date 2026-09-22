test_that("real identifiers are classified from their local format", {
  expect_identical(
    redsan:::.edsan_ct_detect_his_types(c("0012345678", "987654321")),
    c("IPP", "IEP")
  )

  expect_error(
    redsan:::.edsan_ct_detect_his_types(c(12345678, 987654321)),
    "must be character"
  )
  expect_error(
    redsan:::.edsan_ct_detect_his_types(c("00123", "IEP-1")),
    "digit strings"
  )
})

test_that("explicit id_type overrides automatic format detection", {
  expect_identical(
    redsan:::.edsan_ct_validate_explicit_his_type(c("00123", "00456"), "IPP"),
    c("IPP", "IPP")
  )

  result <- NULL
  expect_warning(
    result <- redsan:::.edsan_ct_validate_explicit_his_type("00123", "IEP"),
    "id_type"
  )
  expect_identical(result, "IEP")

  result2 <- NULL
  expect_warning(
    result2 <- redsan:::.edsan_ct_validate_explicit_his_type("98765", "IPP"),
    "id_type"
  )
  expect_identical(result2, "IPP")
})

test_that("EDSaN CT batches identifiers by type with comma separators", {
  calls <- list()
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    calls[[length(calls) + 1L]] <<- list(
      api_fct = api_fct,
      api_type = api_type,
      api_query = api_query,
      env = env,
      ks_path = ks_path
    )
    ids <- strsplit(api_query, ",", fixed = TRUE)[[1L]]
    values <- if (api_type == "NIP") paste0("PAT-", ids) else paste0("EVT-", ids)
    stats::setNames(
      lapply(values, function(value) stats::setNames(list(value), api_type)),
      ids
    )
  }

  out <- redsan:::.edsan_ct_translate(
    ids = c("00123", "00456", "98765", "87654"),
    input_types = c("IPP", "IPP", "IEP", "IEP"),
    direction = "his_to_edsan",
    env = "edsan-ct",
    ks_path = "/tmp/keystore",
    call = fake_call
  )

  expect_length(calls, 2L)
  expect_identical(vapply(calls, `[[`, character(1), "api_fct"),
                   rep("getHISToEDSaNCorrespondences", 2L))
  expect_identical(vapply(calls, `[[`, character(1), "api_type"),
                   c("NIP", "CPAGE"))
  expect_identical(vapply(calls, `[[`, character(1), "api_query"),
                   c("00123,00456", "98765,87654"))
  expect_identical(vapply(calls, `[[`, character(1), "ks_path"),
                   rep("/tmp/keystore", 2L))
  expect_identical(out$output_id,
                   c("PAT-00123", "PAT-00456", "EVT-98765", "EVT-87654"))
  expect_identical(out$output_type, c("PATID", "PATID", "EVTID", "EVTID"))
  expect_identical(out$status, rep("matched", 4L))
})

test_that("EDSaN CT splits batches at max_in_ids", {
  calls <- character()
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    calls <<- c(calls, api_query)
    ids <- strsplit(api_query, ",", fixed = TRUE)[[1L]]
    stats::setNames(
      lapply(ids, function(id) list(CPAGE = paste0("IEP-", id))),
      ids
    )
  }

  ids <- as.character(seq_len(8L))
  out <- redsan:::.edsan_ct_translate(
    ids = ids,
    input_types = rep("EVTID", length(ids)),
    direction = "edsan_to_his",
    max_in_ids = 3L,
    call = fake_call
  )

  expect_identical(calls, c("1,2,3", "4,5,6", "7,8"))
  expect_identical(out$input_id, ids)
  expect_identical(out$output_id, paste0("IEP-", ids))
})

test_that("reverse correspondence uses the EDSaN-to-HIS endpoint", {
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    expect_identical(api_fct, "getEDSaNToHISCorrespondences")
    expect_identical(api_type, "CPAGE")
    list("EVT-1" = list(CPAGE = "98765"))
  }

  out <- redsan:::.edsan_ct_translate(
    ids = "EVT-1",
    input_types = "EVTID",
    direction = "edsan_to_his",
    call = fake_call
  )

  expect_identical(out$output_id, "98765")
  expect_identical(out$output_type, "IEP")
})

test_that("missing and multiple correspondences remain explicit in a batch", {
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    list(multiple = list(CPAGE = c("IEP-1", "IEP-2")))
  }

  out <- redsan:::.edsan_ct_translate(
    ids = c("missing", "multiple"),
    input_types = c("EVTID", "EVTID"),
    direction = "edsan_to_his",
    call = fake_call
  )

  expect_identical(out$status, c("not_found", "multiple_matches", "multiple_matches"))
  expect_identical(out$n_matches, c(0L, 2L, 2L))
  expect_true(is.na(out$output_id[[1L]]))
  expect_identical(out$output_id[-1L], c("IEP-1", "IEP-2"))
})

test_that("a NULL backend response is a hard failure, not a missing match", {
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) NULL

  expect_error(
    redsan:::.edsan_ct_translate(
      ids = "00123",
      input_types = "IPP",
      direction = "his_to_edsan",
      call = fake_call
    ),
    "call failed"
  )
})

test_that("an error payload from EDSaN CT is a hard failure, not a missing match", {
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    list(status = 500, message = "Internal Server Error")
  }

  expect_error(
    redsan:::.edsan_ct_translate(
      ids = "00123",
      input_types = "IPP",
      direction = "his_to_edsan",
      call = fake_call
    ),
    "Internal Server Error"
  )
})

test_that("an unrelated non-empty response is a hard failure", {
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    list(unrelated_key = "unexpected")
  }

  expect_error(
    redsan:::.edsan_ct_translate(
      ids = "00123",
      input_types = "IPP",
      direction = "his_to_edsan",
      call = fake_call
    ),
    "unrecognized response shape"
  )
})

test_that("an explicit empty response remains a genuine not_found", {
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) list()

  out <- redsan:::.edsan_ct_translate(
    ids = "00123",
    input_types = "IPP",
    direction = "his_to_edsan",
    call = fake_call
  )

  expect_identical(out$status, "not_found")
})

test_that("public functions validate before contacting the backend", {
  expect_error(suppressWarnings(edsan_pseudonymize(123456)), "must be character")
  expect_error(
    suppressWarnings(edsan_reidentify(123456, id_type = "PATID")),
    "must be character"
  )
  expect_error(suppressWarnings(edsan_reidentify("EVT-1")), "id_type")
})

test_that("edsan_ct exposes every direct direction with semantic columns", {
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    ids <- strsplit(api_query, ",", fixed = TRUE)[[1L]]
    prefix <- switch(
      paste(api_fct, api_type),
      "getHISToEDSaNCorrespondences NIP" = "PAT-",
      "getHISToEDSaNCorrespondences CPAGE" = "EVT-",
      "getEDSaNToHISCorrespondences NIP" = "IPP-",
      "getEDSaNToHISCorrespondences CPAGE" = "IEP-"
    )
    stats::setNames(
      lapply(ids, function(id) stats::setNames(list(paste0(prefix, id)), api_type)),
      ids
    )
  }
  testthat::local_mocked_bindings(
    .edsan_ct_call = fake_call,
    .package = "redsan"
  )

  cases <- list(
    IPP = c("PATID", "PAT-00123"),
    IEP = c("EVTID", "EVT-98765"),
    PATID = c("IPP", "IPP-12345"),
    EVTID = c("IEP", "IEP-67890")
  )
  inputs <- c(IPP = "00123", IEP = "98765", PATID = "12345", EVTID = "67890")

  for (from in names(cases)) {
    out <- edsan_ct(inputs[[from]], from = from)
    expect_identical(
      names(out),
      c(from, cases[[from]][[1L]], "status", "n_matches"),
      info = from
    )
    expect_identical(out[[from]], inputs[[from]], info = from)
    expect_identical(out[[cases[[from]][[1L]]]], cases[[from]][[2L]], info = from)
    expect_identical(out$status, "matched", info = from)
    expect_identical(out$n_matches, 1L, info = from)
  }
})

test_that("edsan_ct preserves identifiers beyond safe numeric precision", {
  large_id <- "9007199254740993"
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    expect_identical(api_query, large_id)
    stats::setNames(list(list(NIP = "0001234567")), large_id)
  }
  testthat::local_mocked_bindings(
    .edsan_ct_call = fake_call,
    .package = "redsan"
  )

  out <- edsan_ct(large_id, from = "PATID")

  expect_identical(out$PATID, large_id)
  expect_identical(out$IPP, "0001234567")
})

test_that("edsan_ct exposes backend and response failures as errors", {
  failures <- list(
    network = list(
      call = function(...) stop("network unavailable"),
      message = "network unavailable"
    ),
    authentication = list(
      call = function(...) stop("authentication failed"),
      message = "authentication failed"
    ),
    null = list(call = function(...) NULL, message = "call failed"),
    http = list(
      call = function(...) list(status = 500, message = "Internal Server Error"),
      message = "Internal Server Error"
    ),
    malformed = list(
      call = function(...) list(unrelated_key = "unexpected"),
      message = "unrecognized response shape"
    )
  )

  for (failure in failures) {
    testthat::local_mocked_bindings(
      .edsan_ct_call = failure$call,
      .package = "redsan"
    )
    expect_error(edsan_ct("00123", from = "IPP"), failure$message)
  }
})

test_that("edsan_ct results join directly by their source identifier", {
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    output <- switch(
      paste(api_fct, api_type),
      "getHISToEDSaNCorrespondences NIP" = "PAT-1",
      "getHISToEDSaNCorrespondences CPAGE" = "EVT-1",
      "getEDSaNToHISCorrespondences NIP" = "IPP-1",
      "getEDSaNToHISCorrespondences CPAGE" = "IEP-1"
    )
    stats::setNames(
      list(stats::setNames(list(output), api_type)),
      api_query
    )
  }
  testthat::local_mocked_bindings(
    .edsan_ct_call = fake_call,
    .package = "redsan"
  )

  cases <- list(
    IPP = c(id = "000123", destination = "PATID", value = "PAT-1"),
    IEP = c(id = "98765", destination = "EVTID", value = "EVT-1"),
    PATID = c(id = "123", destination = "IPP", value = "IPP-1"),
    EVTID = c(id = "456", destination = "IEP", value = "IEP-1")
  )

  for (from in names(cases)) {
    case <- cases[[from]]
    source <- tibble::tibble(!!from := c(case[["id"]], "missing"), value = c(1, 2))
    correspondence <- edsan_ct(case[["id"]], from = from)
    joined <- dplyr::left_join(source, correspondence, by = from)

    expect_identical(
      joined[[case[["destination"]]]],
      c(case[["value"]], NA_character_),
      info = from
    )
    expect_identical(joined[[from]], source[[from]], info = from)
  }
})

test_that("edsan_ct preserves missing, multiple, and duplicate inputs", {
  calls <- character()
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    calls <<- c(calls, api_query)
    list(multiple = list(CPAGE = c("IEP-1", "IEP-2")))
  }
  testthat::local_mocked_bindings(
    .edsan_ct_call = fake_call,
    .package = "redsan"
  )

  out <- edsan_ct(c("missing", "multiple", "multiple"), from = "EVTID")

  expect_identical(calls, "missing,multiple")
  expect_identical(out$EVTID, c("missing", rep("multiple", 4L)))
  expect_identical(out$status, c("not_found", rep("multiple_matches", 4L)))
  expect_identical(out$n_matches, c(0L, rep(2L, 4L)))
  expect_identical(out$IEP, c(NA_character_, "IEP-1", "IEP-2", "IEP-1", "IEP-2"))
})

test_that("edsan_ct validates its explicit public contract", {
  expect_error(edsan_ct(123, from = "PATID"), "must be character")
  expect_error(edsan_ct(character(), from = "PATID"), "one or more")
  expect_error(edsan_ct("1", from = "patid"), "must be one of")
  expect_error(edsan_ct("1", from = c("PATID", "EVTID")), "must be one of")
  expect_error(edsan_ct("1", from = "PATID", identity = NA), "TRUE or FALSE")
  expect_error(edsan_ct("1", from = "PATID", identity = "FALSE"), "TRUE or FALSE")
})

test_that("edsan_ct enriches patient identifiers without changing direct status", {
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    ids <- strsplit(api_query, ",", fixed = TRUE)[[1L]]
    values <- if (identical(api_fct, "getHISToEDSaNCorrespondences")) {
      paste0("PAT-", ids)
    } else {
      paste0("00", ids)
    }
    stats::setNames(
      lapply(values, function(value) stats::setNames(list(value), api_type)),
      ids
    )
  }
  fake_patients <- function(patids, ks_path = NULL) {
    tibble::tibble(PATID = patids, GIVEN_NAME = paste0("Patient ", patids))
  }
  testthat::local_mocked_bindings(
    .edsan_ct_call = fake_call,
    .edsan_patient_rows = fake_patients,
    .package = "redsan"
  )

  from_ipp <- edsan_ct("00123", from = "IPP", identity = TRUE)
  from_patid <- edsan_ct("123", from = "PATID", identity = TRUE)

  expect_identical(
    names(from_ipp),
    c("IPP", "PATID", "status", "n_matches", "GIVEN_NAME")
  )
  expect_identical(from_ipp$GIVEN_NAME, "Patient PAT-00123")
  expect_identical(from_ipp$status, "matched")
  expect_identical(from_ipp$n_matches, 1L)
  expect_identical(from_patid$PATID, "123")
  expect_identical(from_patid$IPP, "00123")
  expect_identical(from_patid$GIVEN_NAME, "Patient 123")
})

test_that("edsan_ct enriches stay identifiers with patient identifiers", {
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    ids <- strsplit(api_query, ",", fixed = TRUE)[[1L]]
    values <- if (identical(api_fct, "getHISToEDSaNCorrespondences")) {
      paste0("EVT-", ids)
    } else {
      paste0("IEP-", ids)
    }
    stats::setNames(
      lapply(values, function(value) stats::setNames(list(value), api_type)),
      ids
    )
  }
  fake_evtid_patid <- function(evtids, get = get_edsan) {
    tibble::tibble(EVTID = evtids, PATID = paste0("PAT-", sub("^EVT-", "", evtids)))
  }
  fake_patid_ipp <- function(patids, env = "edsan-ct", ks_path = NULL) {
    tibble::tibble(PATID = patids, IPP = paste0("00", patids))
  }
  fake_patients <- function(patids, ks_path = NULL) {
    tibble::tibble(PATID = patids, FAMILY_NAME = paste0("Family ", patids))
  }
  testthat::local_mocked_bindings(
    .edsan_ct_call = fake_call,
    .edsan_evtid_patid_map = fake_evtid_patid,
    .edsan_patid_ipp_map = fake_patid_ipp,
    .edsan_patient_rows = fake_patients,
    .package = "redsan"
  )

  from_iep <- edsan_ct("456", from = "IEP", identity = TRUE)
  from_evtid <- edsan_ct("789", from = "EVTID", identity = TRUE)

  expect_identical(
    names(from_iep),
    c("IEP", "EVTID", "status", "n_matches", "PATID", "IPP", "FAMILY_NAME")
  )
  expect_identical(from_iep$EVTID, "EVT-456")
  expect_identical(from_iep$PATID, "PAT-456")
  expect_identical(from_iep$IPP, "00PAT-456")
  expect_identical(from_iep$status, "matched")
  expect_identical(from_evtid$EVTID, "789")
  expect_identical(from_evtid$IEP, "IEP-789")
  expect_identical(from_evtid$PATID, "PAT-789")
})

test_that("identity enrichment preserves direct multiple-match metadata", {
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    list(`00123` = list(NIP = c("PAT-1", "PAT-2")))
  }
  fake_patients <- function(patids, ks_path = NULL) {
    tibble::tibble(PATID = patids, NAME = paste0("Name ", patids))
  }
  testthat::local_mocked_bindings(
    .edsan_ct_call = fake_call,
    .edsan_patient_rows = fake_patients,
    .package = "redsan"
  )

  out <- edsan_ct("00123", from = "IPP", identity = TRUE)

  expect_identical(out$PATID, c("PAT-1", "PAT-2"))
  expect_identical(out$status, rep("multiple_matches", 2L))
  expect_identical(out$n_matches, rep(2L, 2L))
})

test_that("identity payload metadata cannot replace direct-match metadata", {
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    list(`00123` = list(NIP = "PAT-1"))
  }
  fake_patients <- function(patids, ks_path = NULL) {
    tibble::tibble(
      PATID = patids,
      status = "identity_status",
      n_matches = 99L,
      NAME = "Patient"
    )
  }
  testthat::local_mocked_bindings(
    .edsan_ct_call = fake_call,
    .edsan_patient_rows = fake_patients,
    .package = "redsan"
  )

  out <- edsan_ct("00123", from = "IPP", identity = TRUE)

  expect_identical(out$status, "matched")
  expect_identical(out$n_matches, 1L)
  expect_false(any(c("status.x", "status.y", "n_matches.x", "n_matches.y") %in%
                     names(out)))
})

test_that("identity enrichment rejects contradictory identifier fields", {
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    list(`123` = list(NIP = "00123"))
  }
  fake_patients <- function(patids, ks_path = NULL) {
    tibble::tibble(PATID = patids, IPP = "00999", NAME = "Conflict")
  }
  testthat::local_mocked_bindings(
    .edsan_ct_call = fake_call,
    .edsan_patient_rows = fake_patients,
    .package = "redsan"
  )

  expect_error(
    edsan_ct("123", from = "PATID", identity = TRUE),
    "contradictory `IPP`"
  )
})

test_that("identity backend failures remain errors", {
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    list(`123` = list(NIP = "00123"))
  }
  fake_patients <- function(...) stop("identity backend unavailable")
  testthat::local_mocked_bindings(
    .edsan_ct_call = fake_call,
    .edsan_patient_rows = fake_patients,
    .package = "redsan"
  )

  expect_error(
    edsan_ct("123", from = "PATID", identity = TRUE),
    "identity backend unavailable"
  )
})

test_that("legacy CT entry points warn and preserve their result contracts", {
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    ids <- strsplit(api_query, ",", fixed = TRUE)[[1L]]
    values <- if (identical(api_fct, "getHISToEDSaNCorrespondences")) {
      if (identical(api_type, "NIP")) paste0("PAT-", ids) else paste0("EVT-", ids)
    } else {
      paste0("IPP-", ids)
    }
    stats::setNames(
      lapply(values, function(value) stats::setNames(list(value), api_type)),
      ids
    )
  }
  testthat::local_mocked_bindings(
    .edsan_ct_call = fake_call,
    .package = "redsan"
  )

  pseudonymized <- NULL
  expect_warning(
    pseudonymized <- edsan_pseudonymize(c("00123", "456")),
    "use `edsan_ct\\(\\)`"
  )
  expect_identical(
    names(pseudonymized),
    c("HIS_ID", "HIS_TYPE", "EDSAN_ID", "EDSAN_TYPE", "status", "n_matches")
  )
  expect_identical(pseudonymized$HIS_TYPE, c("IPP", "IEP"))
  expect_identical(pseudonymized$EDSAN_ID, c("PAT-00123", "EVT-456"))

  reidentified <- NULL
  expect_warning(
    reidentified <- edsan_reidentify("123", id_type = "PATID"),
    "use `edsan_ct\\(\\)`"
  )
  expect_identical(names(reidentified), c("PATID", "IPP"))
  expect_identical(reidentified$IPP, "IPP-123")
})

test_that("legacy pseudonymization preserves correspondence edge cases", {
  large_iep <- "9007199254740993"
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    if (identical(api_type, "NIP")) {
      return(list(`00123` = list(NIP = c("PAT-1", "PAT-2"))))
    }

    stats::setNames(
      list(list(CPAGE = "EVT-LARGE")),
      large_iep
    )
  }
  testthat::local_mocked_bindings(
    .edsan_ct_call = fake_call,
    .package = "redsan"
  )

  out <- NULL
  expect_warning(
    out <- edsan_pseudonymize(c("00123", large_iep, "00123", "404")),
    "use `edsan_ct\\(\\)`"
  )

  expect_identical(
    out$HIS_ID,
    c("00123", "00123", large_iep, "00123", "00123", "404")
  )
  expect_identical(
    out$HIS_TYPE,
    c("IPP", "IPP", "IEP", "IPP", "IPP", "IEP")
  )
  expect_identical(
    out$EDSAN_ID,
    c("PAT-1", "PAT-2", "EVT-LARGE", "PAT-1", "PAT-2", NA_character_)
  )
  expect_identical(
    out$status,
    c(
      "multiple_matches", "multiple_matches", "matched",
      "multiple_matches", "multiple_matches", "not_found"
    )
  )
  expect_identical(out$n_matches, c(2L, 2L, 1L, 2L, 2L, 0L))
})

test_that("legacy reidentification preserves correspondence edge cases", {
  large_patid <- "9007199254740993"
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    list(
      `9007199254740993` = list(NIP = "0000123"),
      `123` = list(NIP = c("0001", "0002"))
    )
  }
  testthat::local_mocked_bindings(
    .edsan_ct_call = fake_call,
    .package = "redsan"
  )

  out <- NULL
  expect_warning(
    out <- edsan_reidentify(
      c(large_patid, "123", large_patid, "404"),
      id_type = "PATID"
    ),
    "use `edsan_ct\\(\\)`"
  )

  expect_identical(
    out$PATID,
    c(large_patid, "123", "123", large_patid, "404")
  )
  expect_identical(
    out$IPP,
    c("0000123", "0001", "0002", "0000123", NA_character_)
  )
})
