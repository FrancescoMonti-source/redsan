test_that("EVTID to PATID lookup uses lightweight PMSI idtriplets", {
  calls <- list()

  fake_get <- function(module, what, query, batch_ids_key, fields, ...) {
    calls[[length(calls) + 1L]] <<- list(
      module = module,
      what = what,
      query = query,
      batch_ids_key = batch_ids_key,
      fields = fields,
      dots = list(...)
    )

    tibble::tibble(
      EVTID = c("567964816", "567964817"),
      PATID = c("703518850", "703518851")
    )
  }

  out <- redsan:::.edsan_evtid_patid_map(
    c("567964816", "567964817"),
    get = fake_get
  )

  expect_length(calls, 1L)
  expect_identical(calls[[1L]]$module, "pmsi")
  expect_identical(calls[[1L]]$what, "idtriplets")
  expect_identical(calls[[1L]]$query, list(EVTID = c("567964816", "567964817")))
  expect_identical(calls[[1L]]$batch_ids_key, "EVTID")
  expect_identical(calls[[1L]]$fields, c("PATID", "EVTID"))
  expect_length(calls[[1L]]$dots, 0L)

  expect_identical(out$EVTID, c("567964816", "567964817"))
  expect_identical(out$PATID, c("703518850", "703518851"))
})

test_that("EVTID to PATID lookup preserves missing identifiers", {
  fake_get <- function(...) {
    tibble::tibble(EVTID = "567964816", PATID = "703518850")
  }

  out <- redsan:::.edsan_evtid_patid_map(
    c("567964816", "missing"),
    get = fake_get
  )

  expect_identical(out$EVTID, c("567964816", "missing"))
  expect_identical(out$PATID[[1L]], "703518850")
  expect_true(is.na(out$PATID[[2L]]))
})
