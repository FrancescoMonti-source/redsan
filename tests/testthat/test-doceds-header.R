nicolle_letter <- function() {
  paste(
    "Rouen le 14/10/2015",
    "Madame le Docteur [DOCTOR] ",
    "",
    "Chir.Thoracique et Cardiovasculaire",
    "Pavillon Derocque",
    "CHU CH.NICOLLE [ADDRESS2]",
    "COMPTE-RENDU D'HOSPITALISATION :",
    "",
    "[LASTNAME]  [LASTNAME]  -  Né(e) le : [1949]",
    "",
    "Patiente hospitalisée pour une dyspnée d'aggravation progressive.",
    sep = "\n"
  )
}

test_that("the administrative block after the letter date is removed", {
  cleaned <- trim_doceds_text(nicolle_letter())

  expect_match(cleaned$text, "dyspnée d'aggravation", fixed = TRUE)
  # The date line itself is kept: `\K` starts the removal after it, so the rule
  # can only ever cut what follows a line it positively identified.
  expect_match(cleaned$text, "Rouen le 14/10/2015", fixed = TRUE)
  for (removed in c("Madame le Docteur", "Chir.Thoracique", "Pavillon Derocque",
                    "CH.NICOLLE", "COMPTE-RENDU D'HOSPITALISATION")) {
    expect_false(grepl(removed, cleaned$text, fixed = TRUE), info = removed)
  }
  expect_true("letter_header" %in% cleaned$boilerplate_families)
})

test_that("the header run stops at the first line that is not header-shaped", {
  text <- paste(
    "Rouen le 14/10/2015",
    "Madame le Docteur [DOCTOR]",
    "La patiente decrit une orthopnee depuis trois semaines.",
    "Pavillon Derocque",
    sep = "\n"
  )

  cleaned <- trim_doceds_text(text)

  expect_match(cleaned$text, "orthopnee depuis trois semaines", fixed = TRUE)
  # The service line after the clinical sentence survives, because the run has
  # already stopped. Reaching it would mean the rule can jump over prose.
  expect_match(cleaned$text, "Pavillon Derocque", fixed = TRUE)
})

test_that("a letterhead that opens the document is removed without a date", {
  # One rule for every department. Writing one family per service was producing
  # near-identical patterns for nephrology, pathology, emergency and the
  # molecular biology platform, each with its own closing phrase to get wrong.
  cases <- list(
    nephrology = c(
      "CENTRE HOSPITALIER UNIVERSITAIRE DE ROUEN",
      "Hopitaux de Rouen",
      "Service de nephrologie, dialyse, transplantation renale"
    ),
    pathology = c(
      "CENTRE HOSPITALIER Pavillon - Hopital Charles Nicolle",
      "Service d'anatomie et cytologie pathologiques",
      "Tel.: Professeur: Chef de service Fax:"
    )
  )
  for (name in names(cases)) {
    text <- paste(
      c(cases[[name]], "La creatininemie est a 210 umol/L."),
      collapse = "\n"
    )
    cleaned <- trim_doceds_text(text)

    expect_match(cleaned$text, "creatininemie est a 210", fixed = TRUE, info = name)
    expect_false(grepl("CENTRE HOSPITALIER", cleaned$text, fixed = TRUE), info = name)
    expect_true("document_header" %in% cleaned$boilerplate_families, info = name)
  }
})

test_that("a document header needs a site, not just a doctor", {
  # The two guards that keep the rule off clinical text: it only fires at the
  # very start, and the run must name an establishment somewhere.
  safe <- list(
    opens_by_naming_a_doctor = c(
      "Le Docteur Martin m'a adresse ce patient.",
      "Il presente une dyspnee d'aggravation progressive."
    ),
    two_doctor_lines_then_prose = c(
      "Docteur Martin",
      "Docteur Dupont",
      "Le patient presente une dyspnee."
    ),
    prose_mentioning_a_service = c(
      "Le patient a ete transfere dans le service de reanimation medicale.",
      "La degradation hemodynamique a impose un remplissage."
    )
  )
  for (name in names(safe)) {
    text <- paste(safe[[name]], collapse = "\n")
    cleaned <- trim_doceds_text(text)

    expect_identical(cleaned$text, text, info = name)
    expect_false(cleaned$boilerplate_removed, info = name)
  }
})

test_that("a Word field is recognised wherever it sits on its line", {
  # Anchoring on the line start meant a field preceded by a long run of text
  # was invisible. `IF <>` followed by `\*` is field syntax, not French.
  text <- paste0(
    strrep("x", 140L),
    " IF  <>\" \" \"N° de dossier : \" \"\"  \\*\nCreatinine 180."
  )

  cleaned <- trim_doceds_text(text)

  expect_match(cleaned$text, "Creatinine 180.", fixed = TRUE)
  expect_false(grepl("N° de dossier", cleaned$text, fixed = TRUE))
  expect_true("word_field" %in% cleaned$boilerplate_families)
})

test_that("a sentence naming the patient is not a header line", {
  # De-identification puts `[PATIENT]` inside ordinary prose, so a header rule
  # that accepts any short line containing a field swallows the opening of
  # every letter. Found by auditing the removed spans of the whole corpus for
  # clinical narrative; these are the sentences it was taking.
  sentences <- c(
    "Nous avons inscrit [PATIENT]  [LASTNAME] sur la liste d'attente.",
    "Cet enfant a ete adresse en hopital de jour pour bilan pregreffe.",
    "Nous avons revu le dossier de [PATIENT] pour organiser sa greffe.",
    "Lors de la matinee d'information, nous avons vu [PATIENT] , 24 ans."
  )
  for (sentence in sentences) {
    text <- paste(
      "Rouen le, 12/03/2025",
      "Monsieur le Docteur [DOCTOR]",
      "",
      sentence,
      sep = "\n"
    )

    cleaned <- trim_doceds_text(text)

    expect_match(
      cleaned$text,
      trim_doceds_text(sentence)$text,
      fixed = TRUE,
      info = sentence
    )
  }
})

test_that("a clinical sentence containing an administrative word survives", {
  # The header rule used to read its keyword from anywhere in a line under 100
  # characters, so any sentence that mentioned a service, a hospital or a report
  # was a header line — and was removed whenever it followed a letter date or
  # opened a letterhead. A label puts the administrative word at the front; a
  # sentence reaches it through ordinary prose, and that is the difference the
  # rule now keys on.
  sentences <- c(
    "Patient adresse au service des urgences pour detresse respiratoire.",
    "Transfert dans le service de cardiologie.",
    "Le patient a ete vu en consultation dans le service.",
    "Retour a domicile apres passage aux urgences du centre hospitalier.",
    "Sortie du service le 12 mars avec un traitement par IEC.",
    "Adresse au CHU pour une insuffisance renale aigue.",
    "Compte rendu remis a la famille apres l'entretien.",
    "Reprise du suivi en hopital de jour tous les trois mois.",
    "Le chef de service a valide la sortie du patient."
  )

  for (sentence in sentences) {
    # After a letter date, where `letter_header` would take it.
    after_date <- trim_doceds_text(
      paste("Rouen le 14/10/2015", sentence, "Evolution favorable.", sep = "\n")
    )
    expect_match(after_date$text, sentence, fixed = TRUE, info = sentence)

    # And under a letterhead, where `document_header` would.
    under_header <- trim_doceds_text(
      paste(
        "CENTRE HOSPITALIER UNIVERSITAIRE DE ROUEN",
        "Service de Nephrologie",
        sentence,
        "Evolution favorable.",
        sep = "\n"
      )
    )
    expect_match(under_header$text, sentence, fixed = TRUE, info = sentence)
  }
})

test_that("the letterhead lines the corpus actually carries are still cut", {
  # The other side of the same rule. Anchoring the keyword must not cost the
  # labels it was written for, including the ones that chain two labels on one
  # line and the ones whose head is a compound like "Chefs de Clinique".
  header <- paste0("(?i)^", .DOCEDS_HEADER_LINE, "\\r?$")
  lines <- c(
    "Service de Nephrologie, Dialyse, Transplantation Renale,",
    "Pavillon Derocque",
    "CHU CH.NICOLLE [ADDRESS2]",
    "CHU Rouen - Site de Bois-Guillaume - [ADDRESS1]",
    "COMPTE-RENDU D'HOSPITALISATION :",
    "Secretariat : 02 32 88 89 90",
    "Chefs de Clinique Assistants :",
    "Attaches :",
    "Centre Hospitalier Universitaire de Rouen",
    # The second line of the commonest letterhead in the corpus, and the two
    # shapes it takes. Anchoring the keyword lost 6,655 documents until the
    # `N°` in front of FINESS was allowed for.
    "N°FINESS",
    "N° FINESS : 760 000 158",
    "Chef de service : Pr [DOCTOR]",
    "Hypertension arterielle, Lithiase Renale & Unite de Surveillance Continue",
    "Pole Visceral - Service d'Urologie, Andrologie, Transplantation Renale",
    "Departement Reanimation Anesthesie Medecine peri-operatoire (DREAM)",
    "Chir.Thoracique et Cardiovasculaire",
    "Tel.: Professeur: Chef de service Fax:"
  )

  for (line in lines) {
    expect_true(grepl(header, line, perl = TRUE), info = line)
  }
})

test_that("two families overlapping are each priced at their own length", {
  # `family_chars` is what decides whether a rule earns the risk it carries.
  # Read off the merged spans it cannot: the merge keeps one interval carrying
  # both labels and a single length, so a family that only ever fires inside
  # another one's span is reported as worth the whole union — exactly as
  # valuable as the rule containing it.
  text <- paste(
    "Rouen le 12/05/2024",
    "Service de Nephrologie, Dialyse",
    "[PATIENT] [LASTNAME] Ne(e) le : [1963]",
    "Le patient a ete hospitalise pour insuffisance renale.",
    sep = "\n"
  )

  cleaned <- trim_doceds_text(text)
  chars <- cleaned$boilerplate_family_standalone_chars

  expect_true(all(c("letter_header", "identity_line") %in% names(chars)))
  expect_identical(nrow(cleaned$boilerplate_intervals), 1L)
  # The identity line sits inside the header run, so it is the shorter of the
  # two and must not inherit the run's length.
  expect_lt(chars[["identity_line"]], chars[["letter_header"]])
  expect_identical(
    chars[["letter_header"]],
    cleaned$boilerplate_intervals$removed_chars[[1L]]
  )
})

test_that("a treatment line that names a doctor survives", {
  # Also from the corpus audit. A title counts only at the head of a line: a
  # recipient opens with it, a treatment line mentions it in passing.
  text <- paste(
    "Rouen le, 12/03/2025",
    "Monsieur le Docteur [DOCTOR]",
    "· MYCOPHENOLATE MOFETIL 750 mg suspendu (Vu avec le Dr [DOCTOR])",
    "· PREDNISONE 20 mg Matin (Vu avec Dr [DOCTOR]: majoration habituelle)",
    sep = "\n"
  )

  cleaned <- trim_doceds_text(text)

  expect_match(cleaned$text, "MYCOPHENOLATE MOFETIL 750 mg suspendu", fixed = TRUE)
  expect_match(cleaned$text, "PREDNISONE 20 mg Matin", fixed = TRUE)
  expect_match(cleaned$text, "majoration habituelle", fixed = TRUE)
  # The recipient line above them still goes.
  expect_false(grepl("Monsieur le Docteur", cleaned$text, fixed = TRUE))
})

test_that("a French word beginning with Pr or Dr is not a doctor's title", {
  # PCRE counts an accented letter as a non-word character, so `pr\b` matched
  # the opening of "prevoir" only when the next character was accented —
  # "prévoir", "prévention", "présente", "précaution", "prélèvement",
  # "drépanocytose". Every such line following a letter date was removed as a
  # recipient. The corpus audit found it on a line of insulin instructions.
  openings <- c(
    "Prévoir diminution insuline avec la majoration de la TRULICITY",
    "Prévention de la recidive par colchicine 1 mg par jour",
    "Présente une anemie normocytaire arégénérative a 9 g/dL",
    "Précaution : adapter la posologie a la fonction renale",
    "Prélèvement effectue a jeun le matin de la consultation",
    "Drépanocytose homozygote suivie depuis l'enfance"
  )
  for (opening in openings) {
    text <- paste("Rouen le, 12/09/2023", opening, sep = "\n")

    cleaned <- trim_doceds_text(text)

    expect_match(cleaned$text, opening, fixed = TRUE, info = opening)
  }
})

test_that("a real title is still recognised", {
  text <- paste(
    "Rouen le, 12/09/2023",
    "Pr Martin - Transplantation renale",
    "Dr Dupont",
    "La creatinine est stable a 180 umol/L.",
    sep = "\n"
  )

  cleaned <- trim_doceds_text(text)

  expect_match(cleaned$text, "creatinine est stable", fixed = TRUE)
  expect_false(grepl("Pr Martin", cleaned$text, fixed = TRUE))
  expect_false(grepl("Dr Dupont", cleaned$text, fixed = TRUE))
})

test_that("an older letter date with a two-digit year is recognised", {
  # When the marker was not recognised the preamble rule looked further down
  # and cut everything before the next one, clinical text included.
  text <- paste(
    "CENTRE HOSPITALIER UNIVERSITAIRE DE ROUEN",
    "Rouen le 23.01.08",
    "Merci de realiser une scintigraphie DMSA pour ce candidat au don d'un rein.",
    sep = "\n"
  )

  cleaned <- trim_doceds_text(text)

  expect_match(cleaned$text, "scintigraphie DMSA", fixed = TRUE)
  expect_match(cleaned$text, "candidat au don d'un rein", fixed = TRUE)
})

test_that("a clinical sentence is never mistaken for a header line", {
  text <- paste(
    "Rouen le 14/10/2015",
    paste(
      "La patiente a ete transferee dans le service de reanimation medicale",
      "le lendemain devant une degradation hemodynamique rapidement",
      "progressive malgre le remplissage."
    ),
    sep = "\n"
  )

  cleaned <- trim_doceds_text(text)

  expect_match(cleaned$text, "service de reanimation medicale", fixed = TRUE)
  expect_match(cleaned$text, "remplissage", fixed = TRUE)
})

test_that("a line made only of placeholders and labels is removed whole", {
  text <- paste(
    "Compte rendu de suivi.",
    "De : [LASTNAME]           [LASTNAME]     Né(e) le : [1949]      HC[FILENUM] IF  <>\" \" \"N° de dossier : \" \"\" \\*   ",
    "HC[FILENUM]",
    "[LASTNAME] [LASTNAME]",
    "La creatinine est stable a 180 umol/L.",
    sep = "\n"
  )

  cleaned <- trim_doceds_text(text)

  expect_match(cleaned$text, "creatinine est stable", fixed = TRUE)
  expect_false(grepl("N° de dossier", cleaned$text, fixed = TRUE))
  expect_false(grepl("[FILENUM]", cleaned$text, fixed = TRUE))
  expect_true("identity_line" %in% cleaned$boilerplate_families)
})

test_that("a sentence mentioning a redacted name keeps its content", {
  text <- "Le suivi est assure par le [DOCTOR] en consultation de nephrologie."

  cleaned <- trim_doceds_text(text)

  # The line is prose, so it is not cut; only the placeholder itself goes, and
  # it does not leave a gap behind.
  expect_identical(
    cleaned$text,
    "Le suivi est assure par le en consultation de nephrologie."
  )
  # What the rule cut: the placeholder and the horizontal space it sat in. One
  # space is written back where it stood, so the net loss is one less — which is
  # `net_removed_chars`, and is why the two are separate numbers.
  expect_identical(cleaned$placeholders_standalone_chars, 10L)
  expect_identical(cleaned$net_removed_chars, 9L)
  expect_false(cleaned$boilerplate_removed)
})

test_that("a bracketed year survives, because it is a date not a name", {
  text <- "Cure de hernie inguinale en [2015], sans complication."

  cleaned <- trim_doceds_text(text)

  expect_match(cleaned$text, "[2015]", fixed = TRUE)
  expect_identical(cleaned$placeholders_standalone_chars, 0L)
})
