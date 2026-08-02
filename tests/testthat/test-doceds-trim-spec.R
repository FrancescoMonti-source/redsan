test_that("the spec reports the rules that actually run", {
  spec <- doceds_trim_spec()

  # The whole reason this function exists is that a consumer must not keep its
  # own copy of these values. If it reported anything other than what the
  # trimmer reads, it would be worse than reporting nothing: a caller would
  # record a threshold that never applied and have no way to notice.
  expect_identical(spec$preamble_rule, .DOCEDS_PREAMBLE_RULE)
  expect_identical(spec$preamble_limit_chars, .DOCEDS_PREAMBLE_LIMIT)
  expect_identical(spec$boilerplate_rule, .DOCEDS_BOILERPLATE_RULE)
  expect_identical(spec$near_total_share, .DOCEDS_NEAR_TOTAL_SHARE)
  expect_identical(spec$near_total_min_chars, .DOCEDS_NEAR_TOTAL_MIN_CHARS)
  expect_identical(
    spec$boilerplate_families,
    names(.DOCEDS_BOILERPLATE_PATTERNS)
  )
})

test_that("the spec identifies the installed rules", {
  spec <- doceds_trim_spec()

  expect_identical(spec$package, "redsan")
  expect_identical(spec$version, as.character(utils::packageVersion("redsan")))
})

test_that("every named family is a family that fires on something", {
  # A family reported here that matches nothing is not ward-specific, it is
  # wrong — the same rule the corpus measurement applies. This cannot check the
  # corpus, but it can check that each name resolves to a usable pattern rather
  # than to a leftover entry.
  for (family in doceds_trim_spec()$boilerplate_families) {
    pattern <- .DOCEDS_BOILERPLATE_PATTERNS[[family]]
    expect_true(is.character(pattern) && nzchar(pattern), info = family)
    expect_silent(grepl(pattern, "Compte rendu d'hospitalisation", perl = TRUE))
  }
})
