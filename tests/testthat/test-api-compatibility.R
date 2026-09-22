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

test_that("CORA and ICCA aliases warn and delegate", {
  testthat::local_mocked_bindings(
    cora_query = function(sql, connection = NULL, ojdbc_jar = NULL) sql,
    icca_query = function(sql, params = NULL, connection = NULL,
                          instance = c("adult", "ped")) {
      list(sql = sql, params = params, instance = instance)
    },
    icca_get = function(evtids, source = "encounter", link = "auto",
                        connection = NULL, instance = c("adult", "ped"),
                        env = "edsan-ct", ks_path = NULL) {
      list(evtids = evtids, source = source, link = link, instance = instance)
    },
    .package = "redsan"
  )

  expect_warning(
    expect_identical(query_cora("SELECT 1"), "SELECT 1"),
    "use `cora_query\\(\\)`"
  )
  legacy_query <- NULL
  expect_warning(
    legacy_query <- query_icca("SELECT 1", params = 1, instance = "ped"),
    "use `icca_query\\(\\)`"
  )
  expect_identical(legacy_query$params, 1)
  expect_identical(legacy_query$instance, "ped")

  legacy_get <- NULL
  expect_warning(
    legacy_get <- get_icca("E1", source = "assessment", link = "direct"),
    "use `icca_get\\(\\)`"
  )
  expect_identical(legacy_get$evtids, "E1")
  expect_identical(legacy_get$source, "assessment")
  expect_identical(legacy_get$link, "direct")
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
