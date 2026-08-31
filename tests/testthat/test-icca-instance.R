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
  expect_identical(eval(formals(icca_catalog)$instance), c("adult", "ped"))
  expect_identical(eval(formals(icca_describe)$instance), c("adult", "ped"))
  expect_identical(eval(formals(icca_relations)$instance), c("adult", "ped"))
})
