test_that("CORA connection descriptor yields distinct host and port pairs", {
  descriptor <- paste0(
    "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=oracle-p7)(PORT=1521))",
    "(ADDRESS=(PROTOCOL=TCP)(HOST=oracle-p8)(PORT=1522)))"
  )
  out <- redsan:::.check_connections_endpoints(descriptor)
  expect_identical(out$host, c("oracle-p7", "oracle-p8"))
  expect_identical(out$port, c(1521L, 1522L))
})

test_that("CT probe uses a synthetic identifier only", {
  testthat::local_mocked_bindings(
    edsan_ct = function(ids, from, env, ks_path) {
      expect_identical(ids, "0")
      expect_identical(from, "EVTID")
      tibble::tibble(EVTID = ids, IEP = NA_character_, status = "not_found")
    },
    .package = "redsan"
  )
  expect_match(
    redsan:::.check_connections_ct("edsan-ct", NULL), "synthetic"
  )
})

test_that("connection diagnostic reports each layer independently", {
  testthat::local_mocked_bindings(
    .check_connections_ct = function(...) "CT probe succeeded",
    .cora_keystore_value = function(...) paste0(
      "(ADDRESS=(HOST=oracle-p7)(PORT=1521))",
      "(ADDRESS=(HOST=oracle-p8)(PORT=1522))"
    ),
    .check_connections_dns = function(host) paste("resolved", host),
    .check_connections_tcp = function(host, port, timeout) "TCP accepted",
    .check_connections_oracle = function(ks_path) "Oracle SELECT 1 succeeded",
    .package = "redsan"
  )

  without_jdbc <- check_connections()
  expect_identical(
    without_jdbc$check,
    c("correspondence", "configuration", "DNS oracle-p7", "DNS oracle-p8",
      "TCP oracle-p7:1521", "TCP oracle-p8:1522", "JDBC")
  )
  expect_identical(without_jdbc$status[[7L]], "skipped")
  expect_true(all(without_jdbc$status[-7L] == "ok"))

  with_jdbc <- check_connections(oracle_query = TRUE)
  expect_identical(with_jdbc$status[[7L]], "ok")
})

test_that("failures are reported without masking independent checks", {
  testthat::local_mocked_bindings(
    .check_connections_ct = function(...) stop("CT unavailable"),
    .cora_keystore_value = function(...) "(ADDRESS=(HOST=oracle-p7)(PORT=1521))",
    .check_connections_dns = function(host) stop("DNS failure"),
    .check_connections_tcp = function(host, port, timeout) stop("TCP failure"),
    .package = "redsan"
  )
  x <- check_connections()
  expect_identical(
    x$status, c("failed", "ok", "failed", "failed", "skipped")
  )
  expect_match(x$detail[[1L]], "CT unavailable")
  expect_match(x$detail[[3L]], "DNS failure")
  expect_match(x$detail[[4L]], "TCP failure")
})

test_that("missing CORA configuration skips network and JDBC probes", {
  testthat::local_mocked_bindings(
    .check_connections_ct = function(...) "CT is reachable",
    .cora_keystore_value = function(...) stop("No CORA keys"),
    .check_connections_tcp = function(...) stop("Unexpected TCP probe"),
    .package = "redsan"
  )
  x <- check_connections(oracle_query = TRUE)
  expect_identical(x$status, c("ok", "failed", "skipped", "skipped"))
})

test_that("diagnostic redacts request URLs and patient identifier-like strings", {
  err <- simpleError(
    "Timeout for https://hospital.invalid/api/987654321 and 987654321"
  )
  msg <- redsan:::.check_connections_safe_error(err)
  expect_false(grepl("hospital.invalid|987654321", msg))
  expect_match(msg, "Timeout")
})

test_that("connection probe rejects invalid timeout and Oracle options", {
  expect_error(check_connections(timeout = 0), "timeout")
  expect_error(check_connections(oracle_query = NA), "oracle_query")
})
