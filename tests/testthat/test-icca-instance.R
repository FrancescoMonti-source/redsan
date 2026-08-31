test_that("ICCA instance names map to the expected keystore prefixes", {
  expect_identical(redsan:::.icca_instance_prefix("adult"), "iccaadu")
  expect_identical(redsan:::.icca_instance_prefix("ped"), "iccaped")
  expect_error(redsan:::.icca_instance_prefix("other"), "arg")
})

test_that("public ICCA accessors expose adult and pediatric instance selection", {
  expect_true("instance" %in% names(formals(query_icca)))
  expect_true("instance" %in% names(formals(get_icca)))
  expect_true("instance" %in% names(formals(icca_catalog)))
  expect_true("instance" %in% names(formals(icca_describe)))
  expect_true("instance" %in% names(formals(icca_relations)))

  expect_identical(eval(formals(query_icca)$instance), c("adult", "ped"))
  expect_identical(eval(formals(get_icca)$instance), c("adult", "ped"))
})

test_that("explicit ICCA connections remain independent of instance", {
  fake_connection <- structure(list(), class = "fake_connection")
  out <- query_icca(
    "SELECT 1 AS value",
    connection = fake_connection,
    instance = "ped"
  )
  expect_s3_class(out, "tbl_df")
})
