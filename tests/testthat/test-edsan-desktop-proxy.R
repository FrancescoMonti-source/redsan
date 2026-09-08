test_that("interactive desktop EDSaN calls bypass Podsan proxy by default", {
  old <- getOption("redsan.edsan_ct_proxy")
  on.exit(options(redsan.edsan_ct_proxy = old), add = TRUE)
  options(redsan.edsan_ct_proxy = NULL)

  cfg <- redsan:::.edsan_ct_proxy_config()
  expect_s3_class(cfg, "request")
  expect_identical(cfg$options$proxy, "")
})
