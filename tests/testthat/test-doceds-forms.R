test_that("a checkbox run is removed whichever side its identifiers sit", {
  boxes <- paste(rep("formcheckbox", 18L), collapse = " ")
  # Identifiers ahead of the boxes, which is the shape the first version of the
  # rule could not see: it keyed its close on a trailing block of digits.
  leading <- paste("BT Ne(e) 3 1 0 3 1 9 6 0", boxes, "CHU")
  trailing <- paste(boxes, "CHU de Rouen Dr X 7 6 0 7 8 0 2 3 9 1 0")

  for (case in list(leading, trailing)) {
    text <- paste(case, "Insuffisance renale chronique stade 3.", sep = "\n")
    cleaned <- trim_doceds_text(text)

    expect_match(cleaned$text, "Insuffisance renale", fixed = TRUE)
    expect_false(grepl("formcheckbox", cleaned$text, fixed = TRUE))
    expect_true("form_noise" %in% cleaned$boilerplate_families)
  }
})

test_that("a lone checkbox is not a run", {
  text <- "Consentement signe formcheckbox le patient accepte la procedure."

  cleaned <- trim_doceds_text(text)

  expect_identical(cleaned$text, text)
  expect_false(cleaned$boilerplate_removed)
})

test_that("a macro button goes but its answer stays", {
  text <- paste(
    "Poids 72 kg",
    "Dialyse MACROBUTTON NoMacro Non",
    "Diurese residuelle MACROBUTTON NoMacro Oui",
    sep = "\n"
  )

  cleaned <- trim_doceds_text(text)

  expect_false(grepl("MACROBUTTON", cleaned$text, fixed = TRUE))
  expect_false(grepl("NoMacro", cleaned$text, fixed = TRUE))
  # The control is furniture, the answer is a finding.
  expect_match(cleaned$text, "Dialyse Non", fixed = TRUE)
  expect_match(cleaned$text, "Diurese residuelle Oui", fixed = TRUE)
})

test_that("the other Word remnants go without taking their neighbours", {
  text <- paste(
    "Ne Erreur ! Signet non defini. IF =\"F\" \"E\" le : 12/03/1955 sexe : F",
    "La kaliemie est a 5.1 mmol/L.",
    sep = "\n"
  )

  cleaned <- trim_doceds_text(text)

  expect_false(grepl("Signet non defini", cleaned$text, fixed = TRUE))
  expect_false(grepl("IF =", cleaned$text, fixed = TRUE))
  # `sexe : F` keeps the space French puts before a colon. It used to come back
  # as `sexe: F`, because the tidy-up after the substitutions rewrote `\h+`
  # before punctuation everywhere in the document — including on lines nothing
  # had been removed from. The spacing is now decided per removed span, so text
  # no rule touched is returned exactly as it arrived.
  expect_match(cleaned$text, "sexe : F", fixed = TRUE)
  expect_match(cleaned$text, "kaliemie est a 5.1", fixed = TRUE)
})

test_that("the remaining letterheads are bounded at both ends", {
  cases <- list(
    list(
      family = "pathology_letterhead",
      text = paste(
        "CENTRE HOSPITALIER Pavillon - Hopital Charles Nicolle",
        "Service d'anatomie et cytologie pathologiques Tel.: Fax:",
        "Suite du compte rendu : page 1 Concernant :: BH",
        "Fragment de parenchyme renal avec glomerulosclerose.",
        sep = "\n"
      ),
      removed = "cytologie pathologiques",
      kept = "glomerulosclerose"
    ),
    list(
      family = "results_header",
      text = paste(
        "2024 - Page 1 Examens biologiques",
        "Patient : Ne(e) le : Adresse : Age : Tel : Sexe :",
        "Hemoglobine 10.8 g/dL",
        sep = "\n"
      ),
      removed = "Examens biologiques",
      kept = "Hemoglobine 10.8"
    ),
    list(
      family = "admissions_notice",
      text = paste(
        "Courrier a presenter au bureau des admissions du CHU - Hopitaux de Rouen",
        "lors de toute convocation, veuillez vous presenter muni(e) de votre",
        "carte vitale et de cette lettre.",
        "La fonction renale est stable.",
        sep = "\n"
      ),
      removed = "bureau des admissions",
      kept = "fonction renale est stable"
    )
  )

  for (case in cases) {
    cleaned <- trim_doceds_text(case$text)
    expect_false(
      grepl(case$removed, cleaned$text, fixed = TRUE),
      info = case$family
    )
    expect_match(cleaned$text, case$kept, fixed = TRUE, info = case$family)
    expect_true(
      case$family %in% cleaned$boilerplate_families,
      info = case$family
    )
  }
})
