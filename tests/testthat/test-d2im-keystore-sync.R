test_that("legacy D2IM sync wrapper delegates to d2imr", {
  skip_if_not_installed("d2imr")
  testthat::local_mocked_bindings(
    .d2imr_sync_fn = function() function(python = NULL) "added-key",
    .package = "redsan"
  )

  expect_warning(
    expect_identical(sync_d2im_keystore("mock-python"), "added-key"),
    "deprecated"
  )
})
