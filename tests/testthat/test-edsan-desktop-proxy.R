test_that("interactive desktop EDSaN calls bypass Podsan proxy by default", {
  old <- getOption("redsan.edsan_ct_proxy")
  on.exit(options(redsan.edsan_ct_proxy = old), add = TRUE)
  options(redsan.edsan_ct_proxy = NULL)

  cfg <- redsan:::.edsan_ct_desktop_proxy_config()
  expect_s3_class(cfg, "request")
  expect_identical(cfg$options$proxy, "")
})

test_that("keystore-backed EDSaN calls use the d2imr proxy configuration", {
  skip_if_not_installed("d2imr")
  skip_if_not_installed("httr")

  expected <- httr::config(proxy = "http://proxy.test", proxyport = 8080L)
  testthat::local_mocked_bindings(
    d2im_wsc.proxy_config = function() expected,
    .package = "d2imr"
  )

  cfg <- redsan:::.edsan_ct_d2imr_proxy_config()
  expect_identical(cfg, expected)
})

test_that("keystore patient lookups do not use the desktop proxy override", {
  skip_if_not_installed("httr")

  keystore_proxy <- httr::config(proxy = "http://podman-proxy.test")
  desktop_proxy <- httr::config(proxy = "")
  used_proxy <- NULL

  testthat::local_mocked_bindings(
    .edsan_ct_keystore_auth = function(env, ks_path) {
      list(url = "https://edsan.test/REST", usr = "user", pwd = "password")
    },
    .edsan_ct_d2imr_proxy_config = function() keystore_proxy,
    .edsan_ct_desktop_proxy_config = function() desktop_proxy,
    .edsan_ct_http_get = function(url, usr, pwd, accept, proxy) {
      used_proxy <<- proxy
      stop("intercepted request")
    },
    .package = "redsan"
  )

  expect_error(
    redsan:::.edsan_ct_patient_call("123", ks_path = "/test/keystore"),
    "intercepted request"
  )
  expect_identical(used_proxy, keystore_proxy)
})
