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

test_that("EDSaN CT dispatches NIP and CPAGE correctly", {
  calls <- list()
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    calls[[length(calls) + 1L]] <<- list(
      api_fct = api_fct,
      api_type = api_type,
      api_query = api_query,
      env = env,
      ks_path = ks_path
    )
    if (api_type == "NIP") {
      stats::setNames(list(list(NIP = "PAT-1")), api_query)
    } else {
      stats::setNames(list(list(CPAGE = "EVT-1")), api_query)
    }
  }

  out <- redsan:::.edsan_ct_translate(
    ids = c("00123", "98765"),
    input_types = c("IPP", "IEP"),
    direction = "his_to_edsan",
    env = "edsan-ct",
    ks_path = "/tmp/keystore",
    call = fake_call
  )

  expect_identical(vapply(calls, `[[`, character(1), "api_fct"),
                   rep("getHISToEDSaNCorrespondences", 2L))
  expect_identical(vapply(calls, `[[`, character(1), "api_type"),
                   c("NIP", "CPAGE"))
  expect_identical(vapply(calls, `[[`, character(1), "ks_path"),
                   rep("/tmp/keystore", 2L))
  expect_identical(out$output_id, c("PAT-1", "EVT-1"))
  expect_identical(out$output_type, c("PATID", "EVTID"))
  expect_identical(out$status, c("matched", "matched"))
})

test_that("reverse correspondence uses the EDSaN-to-HIS endpoint", {
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    expect_identical(api_fct, "getEDSaNToHISCorrespondences")
    expect_identical(api_type, "CPAGE")
    stats::setNames(list(list(CPAGE = "98765")), api_query)
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

test_that("missing and multiple correspondences remain explicit", {
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    if (api_query == "missing") return(list())
    stats::setNames(list(list(CPAGE = c("IEP-1", "IEP-2"))), api_query)
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

test_that("an unrecognized response shape is a hard failure, not a missing match", {
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
  expect_error(edsan_pseudonymize(123456), "must be character")
  expect_error(edsan_reidentify(123456, id_type = "PATID"), "must be character")
  expect_error(edsan_reidentify("EVT-1"), "id_type")
})
