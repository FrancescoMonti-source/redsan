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
  expect_identical(
    spec$inline_rules,
    c(field = .DOCEDS_FIELD_PATTERN, rule_run = .DOCEDS_RULE_RUN_PATTERN)
  )
})

test_that("the spec identifies the installed rules", {
  spec <- doceds_trim_spec()

  expect_identical(spec$package, "redsan")
  expect_identical(spec$version, as.character(utils::packageVersion("redsan")))
  expect_identical(spec$digest, .doceds_rules_digest())
  expect_match(spec$digest, "^[0-9a-f]{32}$")
})

test_that("the rule names carry no version", {
  # The version is derived. A digit glued to a name here is the failure this
  # design exists to remove: it can only ever be wrong in one direction, by
  # staying put while the rules move.
  spec <- doceds_trim_spec()

  expect_false(grepl("-v[0-9]+$", spec$preamble_rule))
  expect_false(grepl("-v[0-9]+$", spec$boilerplate_rule))
})

# The digest is only worth anything if it is derived rather than declared, so
# these test the derivation against an environment the test controls, not
# against the package's own constants.
fake_rules <- function(...) {
  env <- new.env(parent = emptyenv())
  values <- list(...)
  for (name in names(values)) assign(name, values[[name]], envir = env)
  env
}

test_that("a rule that changes changes the digest", {
  before <- .doceds_rules_digest(fake_rules(
    .DOCEDS_A = "one",
    .DOCEDS_B = 3000L
  ))
  after <- .doceds_rules_digest(fake_rules(
    .DOCEDS_A = "one",
    .DOCEDS_B = 2000L
  ))

  expect_false(identical(before, after))
})

test_that("a rule that is added changes the digest", {
  # The point of deriving the set from the namespace. Adding a pattern and
  # forgetting to register it anywhere must not leave the identity claiming the
  # rules are what they were.
  before <- .doceds_rules_digest(fake_rules(.DOCEDS_A = "one"))
  after <- .doceds_rules_digest(fake_rules(
    .DOCEDS_A = "one",
    .DOCEDS_NEW_FAMILY = "^whatever"
  ))

  expect_false(identical(before, after))
})

test_that("the digest describes the rules, not how they are written down", {
  # Defining the same constants in a different order is not a different rule
  # set, and a digest that said so would cry wolf on every reordering.
  one <- .doceds_rules_digest(fake_rules(.DOCEDS_A = "x", .DOCEDS_B = "y"))
  other <- .doceds_rules_digest(fake_rules(.DOCEDS_B = "y", .DOCEDS_A = "x"))

  expect_identical(one, other)

  # And something that is not a rule does not enter it.
  expect_identical(
    one,
    .doceds_rules_digest(fake_rules(
      .DOCEDS_A = "x",
      .DOCEDS_B = "y",
      helper_count = 42L
    ))
  )
})

test_that("the digest is stable across calls", {
  expect_identical(.doceds_rules_digest(), .doceds_rules_digest())
})

test_that("the digest reads the bytes, not how they happen to be flagged", {
  # This is the defect that made the first version of the digest worthless.
  # Hashing the R objects made it depend on each string's encoding flag, which
  # R sets from the locale: the patterns carry accented characters, marked
  # `unknown` in a UTF-8 locale and `UTF-8` elsewhere, and the two hashed
  # differently. The same rules on two machines reported themselves as different
  # rules — the one failure a provenance field must not have.
  accented <- "compte[\\h-]*rendu d'hospitalisation réalisée"

  native <- accented
  Encoding(native) <- "unknown"
  marked <- enc2utf8(accented)
  expect_identical(Encoding(marked), "UTF-8")

  expect_identical(
    .doceds_rules_digest(fake_rules(.DOCEDS_A = native)),
    .doceds_rules_digest(fake_rules(.DOCEDS_A = marked))
  )

  # And it still tells two genuinely different patterns apart.
  expect_false(identical(
    .doceds_rules_digest(fake_rules(.DOCEDS_A = marked)),
    .doceds_rules_digest(fake_rules(.DOCEDS_A = paste0(marked, "s")))
  ))
})

test_that("the separators keep two different rule sets apart", {
  # Without a separator that cannot occur in a pattern, `AB` + `C` and `A` +
  # `BC` flatten to the same text and hash the same, which would let a rule
  # change go unreported.
  expect_false(identical(
    .doceds_rules_digest(fake_rules(.DOCEDS_A = "AB", .DOCEDS_B = "C")),
    .doceds_rules_digest(fake_rules(.DOCEDS_A = "A", .DOCEDS_B = "BC"))
  ))

  expect_true(all(
    vapply(.doceds_digest_separators(), nchar, integer(1)) == 1L
  ))
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
