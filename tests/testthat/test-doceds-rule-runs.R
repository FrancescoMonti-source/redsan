# A form draws a blank rule after each heading so the doctor has somewhere to
# write. The heading is a useful structural cue and stays; the rule is nothing
# and goes. Combining macrons are zero-width, so this is invisible in a console
# and still costs a token per character.

test_that("the blank rule after a form heading goes, the heading stays", {
  macrons <- paste(rep("̄", 40L), collapse = " ")
  text <- paste0(
    "¤ Examen clinique ", macrons, "\n",
    "Auscultation pulmonaire libre, pas de souffle cardiaque.\n",
    "¤ Traitements en cours • ", macrons, "\n",
    "Furosemide 40 mg/j, ramipril 5 mg/j."
  )

  cleaned <- trim_doceds_text(text)

  expect_match(cleaned$text, "¤ Examen clinique", fixed = TRUE)
  expect_match(cleaned$text, "¤ Traitements en cours", fixed = TRUE)
  expect_match(cleaned$text, "Furosemide 40 mg/j", fixed = TRUE)
  expect_false(grepl("̄", cleaned$text, fixed = TRUE))
  expect_true(cleaned$rule_runs_standalone_chars > 100L)
})

test_that("dotted and underlined fills go too", {
  text <- paste(
    "Date de la demande : ....................................",
    "Signature : ______________________",
    "Le patient est adresse pour bilan.",
    sep = "\n"
  )

  cleaned <- trim_doceds_text(text)

  # The heading keeps its own spacing: the fill went, the typography did not.
  expect_match(cleaned$text, "Date de la demande :", fixed = TRUE)
  expect_match(cleaned$text, "Le patient est adresse", fixed = TRUE)
  expect_false(grepl("....", cleaned$text, fixed = TRUE))
  expect_false(grepl("___", cleaned$text, fixed = TRUE))
})

test_that("punctuation that is not furniture survives", {
  text <- paste(
    "Le bilan est en attente... le patient sera revu.",
    "Tension 130-80, pouls 60-70 par minute.",
    "Score de 1-2-3-4 sur l'echelle.",
    sep = "\n"
  )

  cleaned <- trim_doceds_text(text)

  # Three dots are punctuation, four are furniture. Hyphens separated by
  # content are never a rule.
  expect_match(cleaned$text, "en attente...", fixed = TRUE)
  expect_match(cleaned$text, "130-80", fixed = TRUE)
  expect_match(cleaned$text, "1-2-3-4", fixed = TRUE)
  expect_identical(cleaned$rule_runs_standalone_chars, 0L)
})

test_that("the department letterhead is removed whatever the staff list", {
  variants <- c(
    "chefs de clinique",
    "chefs de clinique assistants : Dr X Dr Y",
    "chefs de clinique assistants : Dr X Dr Y attaches :"
  )
  for (tail in variants) {
    text <- paste(
      paste(
        "CENTRE HOSPITALIER UNIVERSITAIRE DE ROUEN Hopitaux de Rouen",
        "Service de nephrologie, dialyse, transplantation renale",
        "Pr A Dr B Dr C Dr D", tail
      ),
      "La creatininemie est a 210 umol/L.",
      sep = "\n"
    )

    cleaned <- trim_doceds_text(text)

    expect_match(cleaned$text, "creatininemie est a 210", fixed = TRUE, info = tail)
    expect_false(grepl("CENTRE HOSPITALIER", cleaned$text, fixed = TRUE), info = tail)
    expect_true(
      "establishment_letterhead" %in% cleaned$boilerplate_families,
      info = tail
    )
  }
})
