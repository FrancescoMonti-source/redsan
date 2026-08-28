test_that("EDSaN CT deduplicates requests and restores repeated inputs", {
  calls <- character()
  fake_call <- function(api_fct, api_type, api_query, env, ks_path) {
    calls <<- c(calls, api_query)
    ids <- strsplit(api_query, ",", fixed = TRUE)[[1L]]
    stats::setNames(
      lapply(ids, function(id) list(CPAGE = paste0("IEP-", id))),
      ids
    )
  }

  ids <- c("10", "10", "20", "10", "20")
  out <- redsan:::.edsan_ct_translate(
    ids = ids,
    input_types = rep("EVTID", length(ids)),
    direction = "edsan_to_his",
    call = fake_call
  )

  expect_identical(calls, "10,20")
  expect_identical(out$input_id, ids)
  expect_identical(out$output_id, paste0("IEP-", ids))
  expect_identical(out$input_index, seq_along(ids))
})
