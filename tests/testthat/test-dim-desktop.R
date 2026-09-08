test_that("DIM_KEYSTORE_PATH overrides the active d2imr keystore", {
  old <- Sys.getenv("DIM_KEYSTORE_PATH", unset = NA_character_)
  on.exit({
    if (is.na(old)) Sys.unsetenv("DIM_KEYSTORE_PATH") else Sys.setenv(DIM_KEYSTORE_PATH = old)
  }, add = TRUE)

  path <- tempfile("dim-keystore-")
  file.create(path)
  Sys.setenv(DIM_KEYSTORE_PATH = path)

  expect_identical(redsan:::.dim_keystore_path(), path)
  expect_identical(redsan:::.edsan_ct_resolve_keystore_path(), path)
})

test_that("CORA IEP to IPP mapping is explicit and preserves misses", {
  calls <- character()
  fake_query <- function(sql) {
    calls <<- c(calls, sql)
    tibble::tibble(
      IEP = c("12345", "67890"),
      IPP = c("00111", "00222")
    )
  }

  out <- redsan:::.edsan_cora_iep_ipp_map(
    c("12345", "67890", "99999"),
    query = fake_query
  )

  expect_length(calls, 1L)
  expect_match(calls[[1L]], "CORA_REC.TB_SEJOUR")
  expect_match(calls[[1L]], "CORA_REC.TB_PATIENT")
  expect_match(calls[[1L]], "p.ID_PATIENT = s.ID_PATIENT")
  expect_identical(out$IEP, c("12345", "67890", "99999"))
  expect_identical(out$IPP[1:2], c("00111", "00222"))
  expect_true(is.na(out$IPP[[3L]]))
})

test_that("desktop EVTID to PATID bridge composes EDSaN CT and CORA", {
  calls <- list()

  fake_translate <- function(ids, input_types, direction, env, ks_path) {
    calls[[length(calls) + 1L]] <<- list(
      ids = ids,
      input_types = input_types,
      direction = direction,
      env = env,
      ks_path = ks_path
    )

    if (identical(direction, "edsan_to_his")) {
      return(tibble::tibble(
        input_id = ids,
        output_id = c("12345", "67890")
      ))
    }

    tibble::tibble(
      input_id = ids,
      output_id = c("PAT-1", "PAT-2")
    )
  }

  fake_query <- function(sql) {
    tibble::tibble(
      IEP = c("12345", "67890"),
      IPP = c("00111", "00222")
    )
  }

  out <- redsan:::.edsan_evtid_patid_via_cora(
    c("EVT-1", "EVT-2"),
    env = "edsan-ct",
    ks_path = "/tmp/dim-keystore",
    query = fake_query,
    translate = fake_translate
  )

  expect_length(calls, 2L)
  expect_identical(calls[[1L]]$direction, "edsan_to_his")
  expect_identical(calls[[1L]]$input_types, c("EVTID", "EVTID"))
  expect_identical(calls[[2L]]$direction, "his_to_edsan")
  expect_identical(calls[[2L]]$ids, c("00111", "00222"))
  expect_identical(calls[[2L]]$input_types, c("IPP", "IPP"))
  expect_identical(out$EVTID, c("EVT-1", "EVT-2"))
  expect_identical(out$PATID, c("PAT-1", "PAT-2"))
})

test_that("legacy EVTID to PATID lookup remains injectable", {
  old <- Sys.getenv("DIM_KEYSTORE_PATH", unset = NA_character_)
  on.exit({
    if (is.na(old)) Sys.unsetenv("DIM_KEYSTORE_PATH") else Sys.setenv(DIM_KEYSTORE_PATH = old)
  }, add = TRUE)
  Sys.setenv(DIM_KEYSTORE_PATH = "/configured/dim/keystore")

  fake_get <- function(...) {
    tibble::tibble(EVTID = "EVT-1", PATID = "PAT-1")
  }

  out <- redsan:::.edsan_evtid_patid_map("EVT-1", get = fake_get)
  expect_identical(out$EVTID, "EVT-1")
  expect_identical(out$PATID, "PAT-1")
})
