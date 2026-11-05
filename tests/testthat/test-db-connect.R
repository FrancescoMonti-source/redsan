test_that("db_connect returns the CORA connection and forwards options", {
  connection <- structure(list(), class = "fake_connection")
  seen <- NULL
  testthat::local_mocked_bindings(
    .cora_connect = function(...) {
      seen <<- list(...)
      connection
    },
    .icca_connect = function(...) stop("Unexpected ICCA connection"),
    .cora_disconnect = function(...) stop("Caller owns the connection"),
    .package = "redsan"
  )

  expect_identical(db_connect("cora"), connection)
  expect_identical(seen, list())

  expect_identical(
    db_connect("cora", ojdbc_jar = "/tmp/ojdbc.jar", ks_path = "/tmp/keystore"),
    connection
  )
  expect_identical(
    seen,
    list(ojdbc_jar = "/tmp/ojdbc.jar", ks_path = "/tmp/keystore")
  )
})

test_that("db_connect forwards ICCA instance and driver options", {
  connection <- structure(list(), class = "fake_connection")
  seen <- NULL
  testthat::local_mocked_bindings(
    .icca_connect = function(...) {
      seen <<- list(...)
      connection
    },
    .cora_connect = function(...) stop("Unexpected CORA connection"),
    .icca_disconnect = function(...) stop("Caller owns the connection"),
    .package = "redsan"
  )

  expect_identical(db_connect("icca"), connection)
  expect_identical(seen, list())
  for (instance in c("adult", "ped")) {
    expect_identical(db_connect("icca", instance = instance), connection)
    expect_identical(seen, list(instance = instance))
  }

  options <- list(instance = "ped", driver = "custom", database = "reporting",
                  tds_version = "7.3")
  expect_identical(do.call(db_connect, c(list(source = "icca"), options)), connection)
  expect_identical(seen, options)
})

test_that("db_connect rejects unsupported sources before connecting", {
  testthat::local_mocked_bindings(
    .cora_connect = function(...) stop("Unexpected database connection"),
    .icca_connect = function(...) stop("Unexpected database connection"),
    .package = "redsan"
  )

  for (source in list("edsan-ct", "unknown", "", "c", NULL, character(),
                      NA_character_, c("cora", "icca"), 1L, list("cora"))) {
    expect_error(db_connect(source), "source.*cora.*icca.*database")
  }
})

test_that("db_connect preserves source-specific errors", {
  testthat::local_mocked_bindings(
    .cora_connect = function(...) stop("CORA setup failed"),
    .icca_connect = function(instance = c("adult", "ped")) match.arg(instance),
    .package = "redsan"
  )

  expect_error(db_connect("cora"), "CORA setup failed")
  expect_error(db_connect("icca", instance = "unknown"), "arg")
  expect_error(db_connect("icca", unsupported = TRUE), "unused argument")
})
