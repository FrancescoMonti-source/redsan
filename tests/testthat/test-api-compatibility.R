test_that("normalizer aliases warn and delegate", {
  testthat::local_mocked_bindings(
    doceds_normalize = function(data) list(kind = "doceds", data = data),
    pmsi_normalize = function(data, source_policy = c("c_over_dw", "all")) {
      list(kind = "pmsi", data = data, source_policy = source_policy)
    },
    biol_normalize = function(data) list(kind = "biol", data = data),
    viro_normalize = function(data) list(kind = "viro", data = data),
    .package = "redsan"
  )

  expect_warning(
    expect_identical(process_doceds("x")$kind, "doceds"),
    "use `doceds_normalize\\(\\)`"
  )
  expect_warning(
    expect_identical(process_pmsi("x", source_policy = "all")$source_policy, "all"),
    "use `pmsi_normalize\\(\\)`"
  )
  expect_warning(
    expect_identical(process_biol("x")$kind, "biol"),
    "use `biol_normalize\\(\\)`"
  )
  expect_warning(
    expect_identical(process_viro("x")$kind, "viro"),
    "use `viro_normalize\\(\\)`"
  )
})

test_that("CORA and ICCA exported queries retain legacy behavior", {
  connection <- structure(list(), class = "fake_connection")
  testthat::local_mocked_bindings(
    .cora_execute = function(connection, sql) {
      expect_s3_class(connection, "fake_connection")
      expect_identical(sql, "SELECT 1 AS n FROM dual")
      tibble::tibble(n = 1L)
    },
    .icca_execute = function(connection, sql, params) {
      expect_s3_class(connection, "fake_connection")
      expect_identical(sql, "SELECT ? AS n")
      expect_identical(params, list(7L))
      tibble::tibble(n = 7L)
    },
    .package = "redsan"
  )

  canonical_cora <- cora_query(
    "SELECT 1 AS n FROM dual",
    connection = connection
  )
  legacy_cora <- NULL
  expect_warning(
    legacy_cora <- query_cora(
      "SELECT 1 AS n FROM dual",
      connection = connection
    ),
    "use `cora_query\\(\\)`"
  )
  expect_identical(legacy_cora, canonical_cora)

  canonical_icca <- icca_query(
    "SELECT ? AS n",
    params = 7L,
    connection = connection
  )
  legacy_icca <- NULL
  expect_warning(
    legacy_icca <- query_icca(
      "SELECT ? AS n",
      params = 7L,
      connection = connection,
      instance = "ped"
    ),
    "use `icca_query\\(\\)`"
  )
  expect_identical(legacy_icca, canonical_icca)

  canonical_get <- icca_get(character(), source = "DAR.PatientVentilation")
  legacy_get <- NULL
  expect_warning(
    legacy_get <- get_icca(character(), source = "DAR.PatientVentilation"),
    "use `icca_get\\(\\)`"
  )
  expect_identical(legacy_get, canonical_get)
})

test_that("the domain-first API is exported", {
  expected <- c(
    "edsan_ct", "edsan_get", "edsan_source_catalog",
    "edsan_reference_catalog", "edsan_event_bundle", "edsan_event_bundles",
    "edsan_get_event_bundle", "edsan_get_event_bundles",
    "edsan_render_event_bundle", "doceds_normalize", "pmsi_normalize",
    "biol_normalize", "viro_normalize", "cora_query", "icca_get", "icca_query"
  )

  expect_true(all(vapply(expected, function(name) {
    is.function(getExportedValue("redsan", name))
  }, logical(1))))
})
