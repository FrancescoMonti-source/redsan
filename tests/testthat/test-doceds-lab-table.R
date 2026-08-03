lab_document <- function(tail = "La fonction renale reste stable sur les six derniers mois.") {
  paste(
    "Compte rendu de consultation.",
    "",
    "Derniers résultats (par groupe)",
    "",
    "Examen Date Valeur",
    "",
    " Bilan hemato",
    "Hématies 22/11/18 4.73      T/l   ",
    "Hémoglobine 22/11/18 14.8      g/dl   ",
    "Poly. Basophiles 22/11/18 0.9       %   ",
    " Urée/Créatinine/Ionogramme",
    "Urée 22/11/18 6.3       mmol/l   ",
    "Créatininémie 22/11/18 107       µmol/l   ",
    "",
    tail,
    sep = "\n"
  )
}

test_that("a pasted results table is removed and the prose around it is not", {
  # This is an evidence-scope policy, not deduplication. For quality control of
  # the coding attached to the current EVTID, `biol` is the authoritative
  # model-visible source of biology: it is dated as the warehouse dated it,
  # while a pasted table is whatever the author copied on the day of writing.
  # The fixture's values are from 2018 and the rule removes them whether or not
  # `biol` carries them, which is the intended behaviour and the reason the
  # trade-off is written down in README.md rather than left to be inferred from
  # this test: a stale value that cannot be reliably dated is a worse basis for
  # a code than no value.
  cleaned <- trim_doceds_text(lab_document())

  expect_match(cleaned$text, "Compte rendu de consultation.", fixed = TRUE)
  expect_match(cleaned$text, "fonction renale reste stable", fixed = TRUE)
  for (removed in c("Derniers résultats", "Examen Date Valeur", "Bilan hemato",
                    "Hémoglobine", "Créatininémie", "Poly. Basophiles")) {
    expect_false(grepl(removed, cleaned$text, fixed = TRUE), info = removed)
  }
  expect_true("lab_table" %in% cleaned$boilerplate_families)
})

test_that("pasted results tables can be retained explicitly", {
  text <- lab_document()
  kept <- trim_doceds_text(text, remove_lab_tables = FALSE)

  expect_identical(kept$text, text)
  expect_false(kept$boilerplate_removed)
  expect_false("lab_table" %in% kept$boilerplate_families)
})

test_that("the table ends where the table ends", {
  # A sentence right after the last result line must survive. The run stops at
  # the first line that is neither an indented group title nor a result.
  cleaned <- trim_doceds_text(
    lab_document("Creatininemie en hausse, majoration du furosemide a 80 mg/j.")
  )

  expect_match(cleaned$text, "majoration du furosemide", fixed = TRUE)
})

test_that("a clinical sentence is not read as a result line", {
  # An analyte with a value inside prose is not a table row: a result line is
  # analyte, date, value, unit, and nothing else.
  text <- paste(
    "La creatininemie du 22/11/18 etait a 107 umol/L, en hausse.",
    "L'hemoglobine reste stable.",
    sep = "\n"
  )

  cleaned <- trim_doceds_text(text)

  expect_identical(cleaned$text, text)
  expect_false(cleaned$boilerplate_removed)
})

test_that("the retired families are gone and every survivor is reachable", {
  patterns <- .DOCEDS_BOILERPLATE_PATTERNS

  expect_false("results_pagination" %in% names(patterns))
  expect_false("emergency_letterhead" %in% names(patterns))
  # Every entry must be a usable pattern: a NULL left behind by an edit would
  # otherwise sit in the list and fail only when a document reaches it.
  expect_true(all(vapply(patterns, function(p) {
    is.character(p) && length(p) == 1L && nzchar(p)
  }, logical(1))))
})
