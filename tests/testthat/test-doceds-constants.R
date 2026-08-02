questionnaire <- function(constants = c("Taille : 1m80", "Poids : 144 kg",
                                        "TA : 130/80")) {
  paste(
    c(
      "Consultation infirmiere de pre-dialyse.",
      "",
      "1) Traitement",
      " FORMCHECKBOX ",
      " Oui",
      " FORMCHECKBOX ",
      " Non",
      "",
      "2) Auto-surveillance",
      " FORMCHECKBOX ",
      " Oui",
      " FORMCHECKBOX ",
      " Non",
      "",
      "Etat General",
      constants,
      "",
      " FORMCHECKBOX ",
      " Asthenie",
      " FORMCHECKBOX ",
      " Dyspnee"
    ),
    collapse = "\n"
  )
}

test_that("a measured constant survives a CRLF document too", {
  # This corpus is LF throughout, so a line rule that forgets `\r` costs nothing
  # here and everything on the first export that is not. `[^\r\n]*` cannot
  # consume the carriage return and `$` matches after it, so no line matches end
  # to end, the protection silently stops applying, and the questionnaire loses
  # its measurements with the furniture.
  cleaned <- trim_doceds_text(
    gsub("\n", "\r\n", questionnaire(), fixed = TRUE)
  )

  expect_match(cleaned$text, "Poids : 144 kg", fixed = TRUE)
  expect_match(cleaned$text, "TA : 130/80", fixed = TRUE)
  expect_false(grepl("FORMCHECKBOX", cleaned$text, fixed = TRUE))
})

test_that("a measured constant survives the form run around it", {
  # The questionnaires are one long checkbox run with the answers typed between
  # the boxes. Removing the run is right — an extraction that loses which boxes
  # were ticked turns a checklist into a list of symptoms the patient appears
  # to have — but the values typed into it are measurements no other source
  # carries. Found by auditing the removed spans of the whole corpus.
  cleaned <- trim_doceds_text(questionnaire())

  expect_match(cleaned$text, "Taille : 1m80", fixed = TRUE)
  expect_match(cleaned$text, "Poids : 144 kg", fixed = TRUE)
  expect_match(cleaned$text, "TA : 130/80", fixed = TRUE)
  # The form itself still goes.
  expect_false(grepl("FORMCHECKBOX", cleaned$text, fixed = TRUE))
  expect_false(grepl("Asthenie", cleaned$text, fixed = TRUE))
  expect_true("form_noise" %in% cleaned$boilerplate_families)
})

test_that("an empty constant label leaves with the form", {
  # The value is the discriminator. A printed label with nothing after it says
  # nothing, and keeping it would put the whole form back one line at a time.
  cleaned <- trim_doceds_text(
    questionnaire(c("Taille :", "Poids :", "TA :"))
  )

  expect_false(grepl("Taille", cleaned$text, fixed = TRUE))
  expect_false(grepl("Poids", cleaned$text, fixed = TRUE))
})

test_that("every recorded constant is recognised", {
  filled <- c(
    "Taille : 1m80", "Taille: 180 cm", "Poids : 78,5 kg", "IMC : 32",
    "TA : 130/80", "PA : 12/8", "FC : 72/min", "Pouls : 68",
    "Temperature : 37,2", "T° : 38.5", "SpO2 : 96%", "Sat : 94 %",
    "Saturation : 92", "Diurese : 1200 ml", "Frequence cardiaque : 72",
    "Frequence respiratoire : 18", "Perimetre abdominal : 110 cm",
    "Surface corporelle : 1,9", "Poids 144 kg"
  )
  for (line in filled) {
    expect_true(
      grepl(.DOCEDS_CONSTANT_LINE_PATTERN, line, perl = TRUE),
      info = line
    )
  }
})

test_that("a laboratory line is not mistaken for a constant", {
  # Analytes reach the model as citable `biol` records, so they are deliberately
  # absent from the constant list. Matching them here would punch a hole through
  # every pasted results table and put the duplication back.
  #
  # The first two are the ones that made the rule: `saturation` and `diurese`
  # are vital signs *and* analytes, and the audit found them 3,300 times inside
  # removed tables. What separates the two readings is the date the table puts
  # after the label — a vital sign never reads `Poids 12/05/24`.
  lab <- c(
    "Coeff. de saturation 13/12/21 0.34      %",
    "        Diurèse 04/10/23 3100      ml/24h",
    "Poids 12/05/24 78 kg",
    "Creatinine 12/05/24 180 umol/L",
    "Glycemie 12/05/24 5,4 mmol/L",
    "Saturation de la transferrine 25 %",
    "Hemoglobine 11,2 g/dL",
    "Bilan lipidique",
    "secretariat@chu-rouen.fr : 0232888888"
  )
  for (line in lab) {
    expect_false(
      grepl(.DOCEDS_CONSTANT_LINE_PATTERN, line, perl = TRUE),
      info = line
    )
  }
})

test_that("a pasted results table is still removed whole", {
  text <- paste(
    "Derniers resultats",
    "  Bilan hepatique",
    "ASAT 12/05/24 32 UI/L",
    "ALAT 12/05/24 28 UI/L",
    "  Bilan martial",
    "Ferritine 12/05/24 210 ug/L",
    "La fonction renale reste stable.",
    sep = "\n"
  )

  cleaned <- trim_doceds_text(text)

  expect_match(cleaned$text, "fonction renale reste stable", fixed = TRUE)
  expect_false(grepl("Ferritine", cleaned$text, fixed = TRUE))
  expect_true("lab_table" %in% cleaned$boilerplate_families)
})

test_that("subtracting a protected span splits the interval around it", {
  intervals <- data.frame(
    start = 10L, end = 100L, family = "form_noise", join = "\n"
  )
  protected <- data.frame(
    start = 40L, end = 50L, family = "constant", join = "\n"
  )

  result <- .subtract_intervals(intervals, protected)

  expect_identical(result$start, c(10L, 51L))
  expect_identical(result$end, c(39L, 100L))
  expect_identical(unique(result$family), "form_noise")
  # Both halves close the same way the whole span would have.
  expect_identical(unique(result$join), "\n")
  expect_identical(sum(result$removed_chars), 30L + 50L)
})

test_that("a protected span covering an interval removes it entirely", {
  intervals <- data.frame(
    start = 10L, end = 20L, family = "form_noise", join = "\n"
  )
  protected <- data.frame(
    start = 1L, end = 30L, family = "constant", join = "\n"
  )

  result <- .subtract_intervals(intervals, protected)

  expect_identical(nrow(result), 0L)
  # Same columns whatever happens: these frames are rbound across documents,
  # and a missing column there is an error rather than an empty row.
  expect_identical(
    names(result),
    c("start", "end", "family", "join", "removed_chars")
  )
})

test_that("a protected constant shelters from the inline rules too", {
  # Every destructive rule goes through the same protection now. When the field
  # and rule-run rules were substitutions applied after the cut, a protected
  # line could still be edited by them, and nothing recorded it.
  cleaned <- trim_doceds_text(
    paste(
      "Formulaire de consultation",
      "Poids : 144 kg ....................",
      "Signature ....................",
      sep = "\n"
    )
  )

  expect_match(cleaned$text, "Poids : 144 kg", fixed = TRUE)
})
