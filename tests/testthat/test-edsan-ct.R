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

test_that("EDSaN CT batches identifiers by type", {
  calls <- list()
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    calls[[length(calls) + 1L]] <<- list(
      api_fct = api_fct,
      api_type = api_type,
      api_query = api_query,
      env = env,
      ks_path = ks_path
    )
    ids <- strsplit(api_query, " OR ", fixed = TRUE)[[1L]]
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
                   c("00123 OR 00456", "98765 OR 87654"))
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
    ids <- strsplit(api_query, " OR ", fixed = TRUE)[[1L]]
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

  expect_identical(calls, c("1 OR 2 OR 3", "4 OR 5 OR 6", "7 OR 8"))
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
  expect_error(edsan_pseudonymize(123456), "must be character")
  expect_error(edsan_reidentify(123456, id_type = "PATID"), "must be character")
  expect_error(edsan_reidentify("EVT-1"), "id_type")
})
