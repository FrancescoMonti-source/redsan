# The corpus carries two template conventions at once: square-bracket
# redactions inserted by de-identification, and Word merge fields that were
# never filled in. Both reach the model as noise.

test_that("an unfilled merge field is header material like a placeholder", {
  text <- paste(
    "Rouen le 12/03/2025",
    "«ADRESSE_1»",
    "«ADRESSE_2»",
    "«CODE_POSTAL» «LIBELLE_COMMUNE»",
    "COMPTE-RENDU D'HOSPITALISATION :",
    "Date d'entrée : 10/03/2025    Date de sortie : 14/03/2025",
    "",
    "Patient hospitalisé pour une pyélonéphrite aiguë.",
    sep = "\n"
  )

  cleaned <- trim_doceds_text(text)

  expect_match(cleaned$text, "pyélonéphrite aiguë", fixed = TRUE)
  # Before merge fields were recognised the run stopped at «ADRESSE_1» and the
  # whole block survived, which is how this layout reached the model intact.
  for (removed in c("ADRESSE_1", "CODE_POSTAL", "COMPTE-RENDU", "Date d'entrée")) {
    expect_false(grepl(removed, cleaned$text, fixed = TRUE), info = removed)
  }
  expect_true("letter_header" %in% cleaned$boilerplate_families)
})

test_that("a Word IF field is removed without needing a placeholder", {
  text <- paste(
    "Compte rendu de consultation.",
    "IF  <>\" \" \"N° de dossier : \" \"\" \\*   ",
    "N° de dossier : ",
    "La kaliémie est à 5.1 mmol/L.",
    sep = "\n"
  )

  cleaned <- trim_doceds_text(text)

  expect_match(cleaned$text, "kaliémie est à 5.1", fixed = TRUE)
  expect_false(grepl("N° de dossier", cleaned$text, fixed = TRUE))
  expect_true("word_field" %in% cleaned$boilerplate_families)
})

test_that("a merge field inside a sentence goes without leaving a gap", {
  text <- "Le suivi se poursuit à «LIBELLE_COMMUNE» tous les trois mois."

  cleaned <- trim_doceds_text(text)

  expect_identical(
    cleaned$text,
    "Le suivi se poursuit à tous les trois mois."
  )
  expect_false(cleaned$boilerplate_removed)
})

# Guillemets are also how French quotes anything, and a clinician quotes to
# report exactly the finding that matters: the histology verdict, the wound, the
# patient's own words. The field rule keys on the shape of what is inside them.
test_that("a quotation between guillemets is not a merge field", {
  quoted <- c(
    "Biopsie : « C4d positif sans évidence de rejet ».",
    "Plaie « pied diabétique » suivie en consultation.",
    "Le patient dit « je vais te crever » à l'infirmière.",
    "Passage en « soins palliatifs » décidé en réunion."
  )

  for (one in quoted) {
    expect_identical(trim_doceds_text(one)$text, one, info = one)
  }
})

test_that("an unfilled merge field is recognised by name", {
  cleaned <- trim_doceds_text(
    "Adresse : «Adresse_1», «Code_postal», «Libellé_Titre_civilité»"
  )

  expect_identical(cleaned$text, "Adresse :,,")
  # The inline rules are spans like every other rule now, so they are counted
  # and auditable rather than applied as a substitution nothing was watching.
  expect_true(cleaned$placeholders_standalone_chars > 0L)
  expect_identical(
    cleaned$net_removed_chars,
    as.integer(nchar("Adresse : «Adresse_1», «Code_postal», «Libellé_Titre_civilité»") -
      nchar(cleaned$text))
  )
})

test_that("the correspondence and bizone blocks are bounded", {
  cases <- list(
    list(
      family = "correspondence_block",
      text = paste(
        "Adressez votre courrier : CHU de Rouen - Service de néphrologie",
        "Hôpital de Bois-Guillaume - Fax 00 00 00 00 00",
        "Cadre de santé hémodialyse : Monsieur X",
        "Secrétariat - Accueil prise de rendez-vous",
        "Le patient est vu en hôpital de jour.",
        sep = "\n"
      ),
      removed = "Adressez votre courrier",
      kept = "Le patient est vu en"
    ),
    list(
      family = "ald_prescription",
      text = paste(
        "Prescriptions sans rapport avec l'affection de longue durée",
        "(liste ou hors liste) (maladies intercurrentes)",
        "La clairance est estimee a 28 mL/min.",
        sep = "\n"
      ),
      removed = "affection de longue",
      kept = "clairance est estimee"
    )
  )

  for (case in cases) {
    cleaned <- trim_doceds_text(case$text)
    expect_false(
      grepl(case$removed, cleaned$text, fixed = TRUE),
      info = case$family
    )
    expect_match(cleaned$text, case$kept, fixed = TRUE, info = case$family)
    expect_true(case$family %in% cleaned$boilerplate_families, info = case$family)
  }
})

notice_block <- function(times) {
  notice <- paste(
    "Les donnees personnelles recueillies lors de votre prise en charge font",
    "l'objet d'un traitement informatique et d'un stockage. Vous pouvez",
    "exercer votre droit d'opposition www.chu-rouen.fr/rgpd"
  )
  paste(rep(notice, times), collapse = "\n")
}

test_that("a document that is mostly boilerplate is still trimmed", {
  # A consultation letter is fifty lines of letterhead around one paragraph, so
  # a removed share above four fifths is the normal case and not a rule running
  # away. Refusing to trim those was putting the letterhead back in the prompt.
  text <- paste(
    notice_block(8L),
    "Insuffisance renale chronique stade 3, sur nephropathie diabetique.",
    sep = "\n"
  )
  expect_true(nchar(text) >= .DOCEDS_NEAR_TOTAL_MIN_CHARS)

  cleaned <- trim_doceds_text(text)

  expect_false(cleaned$near_total_match_detected)
  expect_identical(cleaned$text, "Insuffisance renale chronique stade 3, sur nephropathie diabetique.")
  expect_true(cleaned$removed_share > 0.9)
})

test_that("the guard still catches a document with nothing left in it", {
  text <- paste(notice_block(8L), "RAS", sep = "\n")

  cleaned <- trim_doceds_text(text)

  expect_true(cleaned$near_total_match_detected)
  expect_match(cleaned$text, "droit d'opposition", fixed = TRUE)
  expect_false(cleaned$boilerplate_removed)
  expect_identical(cleaned$boilerplate_removed_chars, 0L)
})

test_that("a short record that is mostly form is trimmed as usual", {
  # The guard exempts short records on purpose: this one is 90 percent notice,
  # and abandoning its trim would protect nothing.
  text <- paste(
    "Compte rendu remis au patient a sa sortie d'hospitalisation",
    "Stable.",
    sep = "\n"
  )

  cleaned <- trim_doceds_text(text)

  expect_false(cleaned$near_total_match_detected)
  expect_identical(cleaned$text, "Stable.")
})
