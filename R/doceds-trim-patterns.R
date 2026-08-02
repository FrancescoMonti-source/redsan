# The recognition rules for DOCEDS boilerplate.
#
# Every pattern here names a layout that exists in the Rouen corpus and was
# measured against the whole of it before being admitted. A family that matches
# nothing is not ward-specific, it is wrong, and gets deleted rather than kept
# in case. Nothing here removes text on its own: each pattern contributes
# candidate spans, which `trim_doceds_text()` subtracts protected constants from
# and applies in one pass. See `tools/README.md` for how each was priced.
#
# The patterns are PCRE and carry accented French inside string literals on
# purpose. They are meant to be read by a person checking whether a rule can
# reach clinical prose, and `\uxxxx` escapes would make that impossible.

# The name of a rule set, and only its name. What version of it ran is derived
# from the rules themselves — see `.doceds_rules_digest()` — because a version
# in a string here is a fact somebody has to remember, and the one thing it must
# never do is stay the same while the rules change.
.DOCEDS_PREAMBLE_RULE <- "rouen-bois-guillaume"
# How early the letter date has to appear to be a frame boundary rather than a
# date inside prose. Measured rather than chosen: three quarters of the
# documents carry the marker at all, and among those it sits at 583 characters
# at the median, 1,385 at the 95th percentile and 1,550 at the 99.5th — with a
# hard cluster around 1,380, which is one letterhead template repeated. At 3000
# the limit is twice that last figure and excludes 7 documents in the whole
# corpus, so it is inert. If it ever has to move, move it down: set too high it
# trims a document that is not a letter, set too low it only leaves noise in
# the prompt.
.DOCEDS_PREAMBLE_LIMIT <- 3000L
# Above this share of a document, the trim is abandoned rather than applied.
#
# This is a diagnostic for one failure — a rule that ran away on a layout nobody
# has seen and matched essentially the whole document — and **not** a safety
# margin. A document losing 99.4 percent is not clinically different from one
# losing 99.6, so no guarantee about clinical text can rest on which side of
# this line a document falls. What keeps clinical text is that every rule is
# anchored and bounded, that measured constants are subtracted before anything
# is removed, and that the prose audit reads what the rules actually took. This
# number only catches the case where those have already failed completely.
#
# Measured against the corpus rather than chosen: half the documents lose under
# half, and a quarter lose more than four fifths, because a consultation letter
# is fifty lines of letterhead around one paragraph. Trimming those hard is
# right — at 0.75 it was firing on a quarter of documents and putting a
# megabyte of letterhead back into the prompt. Past 0.95 what survives is
# twenty to eighty characters, meaning the document was letterhead and nothing
# else, which is still not a rule running away. So the threshold sits above
# everything the corpus produces and the distribution is reported instead:
# `doceds_boilerplate$removed_share` is what makes a future layout visible.
.DOCEDS_NEAR_TOTAL_SHARE <- 0.995
.DOCEDS_NEAR_TOTAL_MIN_CHARS <- 1000L
# The letter date line, without anchors: it marks the boundary between the
# administrative frame and the body, and both the preamble rule and the header
# family are built from it.
.DOCEDS_LETTER_DATE <- paste0(
  "\\h*(?:bois(?:[\\h-]+)guillaume|rouen)\\h*,?\\h+le",
  "\\h*[,.;:-]?\\h*",
  "(?:",
  # A two-digit year is common in the older letters: "Rouen le 23.01.08". The
  # anchor is the place name and "le" at the start of the line, so accepting it
  # cannot match a date inside prose. Refusing it meant the rule looked further
  # down for a marker and cut everything before that one as preamble, clinical
  # text included.
  "(?:0?[1-9]|[12][0-9]|3[01])",
  "([./-])(?:0?[1-9]|1[0-2])\\1(?:(?:19|20)[0-9]{2}|[0-9]{2})",
  "|",
  "(?:1er|0?[1-9]|[12][0-9]|3[01])\\h+",
  "(?:janvier|f[eé]vrier|mars|avril|mai|juin|juillet|",
  "ao[uû]t|septembre|octobre|novembre|d[eé]cembre)",
  "\\h+(?:19|20)[0-9]{2}",
  ")\\b"
)
.DOCEDS_BODY_START_PATTERN <- paste0(
  "(?im)^",
  .DOCEDS_LETTER_DATE,
  "[^\\r\\n]*\\r?$"
)

# De-identification replaces names, addresses and file numbers upstream, so an
# all-capitals token in square brackets is never clinical content. The shape is
# matched rather than an inventory of labels, because the producing pipeline
# publishes no list. A bracketed year is excluded on purpose: `[1949]` is a
# redacted date that kept the only part that still carries meaning.
.DOCEDS_PLACEHOLDER_PATTERN <- "\\[[A-Z][A-Z0-9_]{2,19}\\]"
.DOCEDS_YEAR_PLACEHOLDER_PATTERN <- "\\[(?:19|20)[0-9]{2}\\]"
# The letters are also produced from Word templates whose merge fields were
# never filled in, and those use guillemets rather than square brackets:
# «adresse_1», «code_postal». An unfilled merge field is not a redaction, it is
# a template that leaked, and it is never clinical content either.
# Two renderings of the same thing: the guillemets Word shows on screen, and
# the field code itself when the extraction never resolved it.
# Guillemets are also how French quotes anything, so no shape rule is safe here.
# Of 118,357 guillemet spans in the corpus, 117,894 are one of ten field names
# and every one of the remaining 463 is clinical — `« C4d positif sans évidence
# de rejet »`, `« pied diabétique »`, `« escarre »`, `« soins palliatifs »`, and
# the patient's own words in a psychiatric note. "No spaces inside" was tried
# and is not enough: a one-word quotation is the commonest kind, and `«douleur»`
# has exactly the shape of a field name.
# So this is an inventory, not a pattern, and like the preamble rule it is
# site-specific: another site's templates need their own list, obtained by
# pulling every `«...»` span from its corpus and reading the distinct ones.
# The `MERGEFIELD` form needs no inventory — that keyword is Word's own and
# cannot occur in French prose.
.DOCEDS_MERGEFIELD_NAMES <- paste0(
  "(?:nom|pr[eé]nom|libell?[eé]_commune|code_postal|adresse_[123]",
  "|libell?[eé]_titre_civilit[eé]|titre_civilit[eé]|copie_[àa])"
)
.DOCEDS_MERGEFIELD_PATTERN <- paste0(
  "(?:(?i:«\\h*",
  .DOCEDS_MERGEFIELD_NAMES,
  "\\h*»)",
  "|(?i:\\bmergefield\\h+[A-Za-z0-9_]{1,40}))"
)
# A Word MacroButton renders as nothing and extracts as its own name. Unlike a
# checkbox it sits in front of a value that does mean something — "macrobutton
# nomacro oui" is a yes — so the control is removed and the answer is kept.
# Case-insensitive only here: the placeholder shape depends on capitals, so the
# flag cannot be raised over the whole alternation.
.DOCEDS_MACROBUTTON_PATTERN <- "(?i:\\bmacrobutton\\h+nomacro\\b)"
# Two more things Word leaves behind mid-sentence: the message it prints where
# a bookmark no longer resolves, and the inline `IF ="f" "e"` form of a field,
# which carries neither the `<>` nor the `\*` that the block rule keys on.
.DOCEDS_BROKEN_BOOKMARK_PATTERN <-
  "(?i:erreur\\h*!\\h*signet\\h+non\\h+d[eé]fini\\.?)"
.DOCEDS_INLINE_IF_PATTERN <- paste0(
  "(?i:\\bif\\b\\h*=\\h*\"[^\"\\r\\n]{0,20}\"",
  "(?:\\h*\"[^\"\\r\\n]{0,20}\")+)"
)
.DOCEDS_FIELD_PATTERN <- paste0(
  "(?:",
  .DOCEDS_PLACEHOLDER_PATTERN,
  "|",
  .DOCEDS_MERGEFIELD_PATTERN,
  "|",
  .DOCEDS_MACROBUTTON_PATTERN,
  "|",
  .DOCEDS_BROKEN_BOOKMARK_PATTERN,
  "|",
  .DOCEDS_INLINE_IF_PATTERN,
  ")"
)

# PCRE counts an accented letter as a non-word character here, so `\b` fires in
# the middle of a French word: `pr\b` matches the opening of "prévoir",
# "prévention", "présente", "précaution" and "prélèvement", and `dr\b` that of
# "drépanocytose". Any abbreviation that can prefix a French word needs a
# boundary that treats accents as letters. This one cost a line of insulin
# instructions, removed as if it were a consultant's name.
.DOCEDS_WORD_START <- "(?<![A-Za-zÀ-ÿ])"
.DOCEDS_WORD_END <- "(?![A-Za-zÀ-ÿ])"

# A vital sign or a body measurement: a label the form printed, followed by a
# value someone typed. Both sit on the same line, and the value is what tells
# them apart — an empty `Taille :` is furniture and leaves with the rest of the
# form, a filled `Taille : 1m80` is a measurement no other source carries.
# Laboratory analytes are deliberately absent: they reach the model as citable
# `biol` records, and matching them here would punch holes through every pasted
# results table.
# `fr` is absent on purpose: it is also the top-level domain every address on
# the letterhead ends with, and `...@chu-rouen.fr : 0232` has the same shape as
# a respiratory rate. The spelled-out form covers it without the collision.
.DOCEDS_CONSTANT_SHORT <- "(?:ta|pa|fc|imc|bmi|t°|sp[o0]2|sa[o0]2|sat)"
.DOCEDS_CONSTANT_LONG <- paste0(
  "(?:taille|poids|temp[eé]rature|pouls|saturation|diur[eè]se",
  "|fr[eé]quence\\h+(?:cardiaque|respiratoire)",
  "|p[eé]rim[eè]tre\\h+(?:abdominal|cr[aâ]nien)",
  "|surface\\h+corporelle)"
)
# A date is not a measurement. `saturation` and `diurèse` are also analytes, and
# in a pasted results table the label is followed by the sampling date:
# `Coeff. de saturation 13/12/21 0.34 %`. Without this the protection punched
# 3,300 holes through the tables in twenty thousand documents and put back the
# duplication `lab_table` exists to remove. A vital sign never reads
# `Poids 12/05/24`, so rejecting a date after the label separates the two
# without a list of exceptions.
.DOCEDS_NOT_A_DATE <- "(?![0-3]?[0-9]/[01]?[0-9]/)"
# A two-letter abbreviation needs its colon. Without one, `ta` and `pa` occur
# inside ordinary words and numbers often enough to protect half the corpus.
.DOCEDS_CONSTANT_VALUE <- paste0(
  "(?:",
  .DOCEDS_WORD_START,
  .DOCEDS_CONSTANT_SHORT,
  .DOCEDS_WORD_END,
  "\\h*:\\h*",
  .DOCEDS_NOT_A_DATE,
  "[<>]?[0-9]",
  "|",
  .DOCEDS_WORD_START,
  .DOCEDS_CONSTANT_LONG,
  .DOCEDS_WORD_END,
  "\\h*:?\\h*",
  .DOCEDS_NOT_A_DATE,
  "[<>]?[0-9]",
  ")"
)
# `\r?` before the close, like every other line rule here. Without it the whole
# protection silently disappears on a CRLF export: `[^\r\n]*` cannot consume the
# `\r`, and `$` matches after it, so no line ever matches end to end and every
# measured constant loses its shelter. This corpus is LF throughout, so the
# omission cost nothing — which is exactly why it would have surfaced as
# unexplained data loss on the first export that was not.
.DOCEDS_CONSTANT_LINE_PATTERN <- paste0(
  "(?im)^[^\\r\\n]*",
  .DOCEDS_CONSTANT_VALUE,
  "[^\\r\\n]*\\r?$"
)

# A line that decomposes end to end into fields, address labels and
# punctuation, and nothing else. Containing a field is not enough and must
# never be treated as enough: de-identification put `[PATIENT]` inside ordinary
# sentences, so "contains a placeholder" matched clinical narrative such as
# "Nous avons inscrit [PATIENT] sur la liste d'attente de transplantation
# rénale". Requiring the whole line to decompose is what separates a banner
# from a sentence that happens to name someone.
.DOCEDS_IDENTITY_BODY <- paste0(
  "(?:\\h*(?:",
  .DOCEDS_FIELD_PATTERN,
  "|",
  .DOCEDS_YEAR_PLACEHOLDER_PATTERN,
  "|\\bhc\\b|\\bif\\b|\\bde\\b|\\bcopies?\\b|\\bdossier\\b",
  "|",
  .DOCEDS_WORD_START,
  "[àa]",
  .DOCEDS_WORD_END,
  "|n[eé]\\(e\\)\\h+le|n°\\h*de\\h*dossier|date\\h+de\\h+naissance",
  "|[-–—,;:./()\"'*\\\\<>]",
  ")\\h*)+"
)

# The blank rule a form draws after a heading, so the doctor has somewhere to
# write: forty combining macrons after "¤ Examen clinique", a row of dots after
# "Date :", an underline across the page. Zero-width or not, every one costs a
# token. Four is the threshold because "..." is punctuation and "...." is
# furniture. A run must be unbroken by anything but spaces, so a hyphenated
# phrase or a numeric range is untouched.
.DOCEDS_RULE_RUN_PATTERN <- paste0(
  "(?:[_.\\-–—¯‾̄·•=~*]\\h*){4,}"
)

# A separator a letterhead uses to chain two labels on one line — "Pôle Viscéral
# - Service d'Urologie", "CHU Rouen - Site de Bois-Guillaume". Nothing else may
# stand before an administrative word, so the word has to open the line or open
# a segment of it.
.DOCEDS_LABEL_SEPARATOR <- "(?:[^\\r\\n]{0,60}?[-–—,;&/]\\h*)?"

# One line of the administrative block that follows the letter date: a
# recipient, a service or site, the document title, or a patient identity line.
# The lookahead caps a header line at 100 characters, which bounds the damage
# but does not tell a label from a sentence — `.DOCEDS_LABEL_PREFIX` is what
# does that. The identity
# alternative is the strict whole-line test, never "contains a field": that
# weaker form swallowed the opening sentences of letters, because
# de-identification leaves `[PATIENT]` in the middle of ordinary prose.
.DOCEDS_HEADER_LINE <- paste0(
  "\\h*(?=[^\\r\\n]{0,100}\\r?$)",
  "(?:",
  "|",
  # A title only counts at the head of the line. A recipient line opens with it
  # — "Dr Martin - Transplantation rénale" — while a clinical line mentions it
  # in passing, and accepting it anywhere removed treatment lines such as
  # "PREDNISONE 20 mg Matin (Vu avec Dr X: majoration de la posologie)".
  "(?:madame|monsieur|mme|mr|m\\.)?\\h*(?:le\\h+|la\\h+)?",
  "(?:docteur|dr|professeur|pr)",
  .DOCEDS_WORD_END,
  "[^\\r\\n]*",
  # The administrative word has to open the line, or open a segment of it after
  # one of the separators a letterhead chains labels with. It may no longer be
  # read from anywhere in a short line: the previous form allowed
  # `[^\r\n]{0,80}?` in front of it, which is any text at all, and removed
  # "Patient adressé au service des urgences pour détresse respiratoire.",
  # "Transfert dans le service de cardiologie." and "Le patient a été vu en
  # consultation dans le service." whenever one followed a letter date. A
  # clinical sentence puts its service at the end, reached through ordinary
  # prose; a label puts it at the front.
  # `chefs de clinique`, `attachés :` and `tél.:` are spelled out because they
  # are label heads in their own right and would otherwise need the free-text
  # prefix back to be reachable.
  "|",
  .DOCEDS_LABEL_SEPARATOR,
  "(?:",
  "service\\b|pavillon\\b|\\bchu\\b|h[oô]pita(?:l|ux)|centre\\h+hospitalier",
  "|plateforme\\b|adresse\\h+postale|n[°o]?\\h*finess\\b",
  "|clinique\\b|unit[eé]\\b|secr[eé]tariat|d[eé]partement|p[oô]le\\b",
  "|chefs?\\h+de\\h+clinique|attach[eé]s?\\h*:|praticiens?\\h+hospitaliers?",
  "|chefs?\\h+de\\h+service",
  "|chir\\.|cedex\\b|t[eé]l[eé]?phone|t[eé]l\\.|\\bfax\\b",
  "|adressez\\h+votre\\h+courrier",
  "|e[\\h-]*mail\\b|courriel\\b",
  # A document title, not the words. "Compte rendu remis à la famille après
  # l'entretien." opens with the same two words and is a sentence, so the title
  # has to be followed by what a title is followed by: its punctuation, or the
  # preposition that names the document type.
  "|compte[\\h-]*rendu(?:\\h*[:–—-]|\\h+(?:d['’]|de\\b|provisoire|d[eé]finitif))",
  "|n[eé]\\(e\\)\\h+le|n°\\h*de\\h*dossier|date\\h+de\\h+naissance",
  "|date\\h+d['’]entr[eé]e|date\\h+de\\h+sortie",
  ")[^\\r\\n]*",
  "|",
  .DOCEDS_IDENTITY_BODY,
  ")"
)

# One line of a pasted results table: blank, the column headings, an indented
# group title with no digit in it, or a result at the margin — analyte, date,
# value, unit. The indent on group titles is what keeps a following clinical
# sentence out of the run.
.DOCEDS_LAB_LINE <- paste0(
  "(?:",
  "\\h*",
  "|\\h*examen\\h+date\\h+valeur\\h*",
  "|\\h+[A-Za-zÀ-ÿ][^\\r\\n0-9]{2,50}",
  "|[^\\h\\r\\n][^\\r\\n]{0,60}?",
  "[0-3]?[0-9]/[01]?[0-9]/(?:[0-9]{2}|[0-9]{4})",
  "\\h+[<>]?[0-9][^\\r\\n]{0,30}",
  ")"
)

# A name, not a version. See `.DOCEDS_PREAMBLE_RULE`.
.DOCEDS_BOILERPLATE_RULE <- "approved-boilerplate-families"
.DOCEDS_BOILERPLATE_PATTERNS <- list(
  rgpd = paste0(
    "(?is)(?:",
    "(?:compte\\s+rendu\\s+remis\\s+au\\s+patient",
    "[\\s\\S]{0,180}?)?",
    "les\\s+donn[eé]es\\s+personnelles\\s+recueillies\\s+lors\\s+de",
    "\\s+votre\\s+prise\\s+en\\s+charge",
    "|(?:dans\\s+le\\s+)?cadre\\s+des\\s+activit[eé]s\\s+de\\s+recherche",
    "\\s+et\\s+du\\s+stockage\\s+de\\s+vos\\s+donn[eé]es",
    "\\s+non\\s+nominatives",
    ")[\\s\\S]{0,4000}?",
    "(?:votre\\s+droit\\s+d['’]opposition|",
    "https?://(?:www\\.)?bit\\.ly/2qqs0e9)",
    "(?:\\s|[.,;:])*",
    "(?:(?:https?://)?www\\.chu-rouen\\.fr/",
    "(?:recherche-)?rgpd/?(?:\\s|[.,;:])*)*"
  ),
  patient_documents = paste0(
    "(?is)documents\\s+donn[eé]s\\s+au\\s+patient",
    "[\\s\\S]{0,2500}?",
    "d[eé]cision\\s+d['’]?orientation",
    "(?:\\s*(?:<date>|",
    "(?:0?[1-9]|[12][0-9]|3[01])[./-](?:0?[1-9]|1[0-2])",
    "(?:[./-](?:19|20)[0-9]{2})?))?"
  ),
  patient_report = paste0(
    "(?is)compte\\s+rendu\\s+remis\\s+au\\s+patient",
    "(?:\\s+[àa]\\s+sa\\s+sortie\\s+d['’]hospitalisation)?"
  ),
  # The optional `sexe masculin` prefix used to sit here with a 600 character
  # gap before the real anchor. That gap reached back across the body of a
  # letter to catch an identity banner, taking the clinical narrative between
  # them with it. Banners are `identity_line`'s job; this family starts where
  # its own phrase starts.
  contact_header = paste0(
    "(?is)",
    "pour\\s+le\\s+suivi\\s+sp[eé]cifique\\s+de\\s+votre\\s+patient",
    "[\\s\\S]{0,1800}?",
    "copie\\s+adress[eé]e\\s+[àa]"
  ),
  medical_phone = paste0(
    "(?is)num[eé]ro\\s+t[eé]l[eé]phonique\\s+r[eé]serv[eé]",
    "\\s+aux\\s+m[eé]decins[\\s\\S]{0,500}?",
    "avis\\s+m[eé]dical\\s+ou\\s+une\\s+hospitalisation"
  ),
  # A run of checkboxes is its own end: the extraction keeps `formcheckbox` but
  # loses whether the box was ticked, so a checklist states nothing at all and
  # cannot support anything. Keying the close on a trailing block of digits, as
  # the first version did, missed every form that carries its identifiers ahead
  # of the boxes instead of after them — which is most of them, and the largest
  # single repetition in the corpus.
  # Possessive, because a document with dozens of boxes made PCRE exhaust its
  # backtracking budget: the match then fails silently and the document is not
  # trimmed at all, which is a worse outcome than either answer. Narrowing the
  # gap to tell an annotated questionnaire from an empty form was tried and
  # abandoned — it cost an eighth of the yield and still removed the annotation
  # it was meant to save.
  form_noise = paste0(
    "(?is)(?:(?:[0-9]\\h*){6,}[^\\r\\n]{0,20}?)?",
    "formcheckbox",
    "(?:[\\s\\S]{0,150}?formcheckbox){3,}+",
    "(?:[\\s\\S]{0,200}?(?:[0-9]\\h*){9,})?"
  ),
  secretariat_header = paste0(
    "(?is)secr[eé]tariat\\s+consultation",
    "[\\s\\S]{0,1800}?",
    "n[°o]?\\s*finess\\s*(?:[0-9]\\s*){6,}"
  ),
  # The letter date is not always the start of the body. In the Nicolle layout
  # it opens the document and the administrative block follows it: recipient,
  # service, site, title, patient identity. `\K` drops the date line itself from
  # the match, so the rule only ever removes what comes after a line it has
  # positively identified, and the run stops at the first line that is not
  # header-shaped.
  letter_header = paste0(
    "(?im)^",
    .DOCEDS_LETTER_DATE,
    "[^\\r\\n]*\\r?\\n\\K",
    "(?:",
    .DOCEDS_HEADER_LINE,
    "\\r?\\n){1,}+"
  ),
  # The same block when it opens the document instead of following a date. One
  # rule instead of one per department: nephrology, pathology, emergency and the
  # molecular biology platform were all producing their own near-identical
  # family. Two guards keep it off clinical text: the run must be at the very
  # start, and it must name an establishment somewhere, so a report opening
  # "Le Docteur X m'a adressé ce patient" is a single header-shaped line with no
  # site in it and matches nothing.
  document_header = paste0(
    "(?im)\\A",
    "(?=(?:[^\\r\\n]*\\r?\\n){0,12}?[^\\r\\n]*",
    "(?:centre\\h+hospitalier|\\bchu\\b|h[oô]pitaux|h[oô]pital|pavillon",
    "|plateforme|secr[eé]tariat|service\\h+d))",
    # A real letterhead runs to about fifty lines, half of them blank: site,
    # ward, one line per consultant, a column of phone numbers, an e-mail. A
    # counted bound is the wrong instrument twice over — twelve lines stopped a
    # third of the way in, and sixty made PCRE refuse to compile, because a
    # counted repetition expands the group that many times. What makes the rule
    # safe is stopping at the first line that is not header-shaped, and the
    # removed-share guard behind it. Possessive, so it never backtracks.
    "(?:",
    .DOCEDS_HEADER_LINE,
    "\\r?\\n){2,}+"
  ),
  # A whole line that decomposes into de-identification placeholders, address
  # labels and punctuation, with at least one placeholder present. This is what
  # a mail-merge field leaves behind once the names are redacted, and it carries
  # no clinical content by construction. The line must match end to end, so a
  # sentence that merely mentions a redacted name is untouched.
  identity_line = paste0(
    "(?im)^(?=[^\\r\\n]*(?:\\[|«))",
    .DOCEDS_IDENTITY_BODY,
    "\\r?$"
  ),
  # What a Word IF field leaves in the plain-text extraction: the field
  # syntax itself, `IF <>" "` and the `\*` formatting switch. This is the most
  # widespread single artifact in the corpus, and it survives the identity rule
  # because the line often carries no placeholder at all. Empty labels that
  # trail it are taken with it, since the field is what would have filled them.
  word_field = paste0(
    "(?im)\\bif\\b\\h*<>[^\\r\\n]{0,240}?\\\\\\*[^\\r\\n]*",
    "(?:\\r?\\n\\h*(?:n°\\h*de\\h*dossier|n[eé]\\(e\\)\\h+le|de)\\h*:?\\h*)*"
  ),
  # The correspondence block of the nephrology letterhead: where to write, who
  # the ward managers are, how to book. Bounded by its own opening and closing
  # phrases.
  correspondence_block = paste0(
    "(?is)adressez\\s+votre\\s+courrier\\s*:",
    "[\\s\\S]{0,900}?",
    "prise\\s+de\\s+rendez[\\s-]*vous"
  ),
  # The bizone prescription form label. A form heading, never a finding.
  ald_prescription = paste0(
    "(?is)prescriptions?\\s+(?:sans\\s+rapport\\s+avec",
    "|relatives?\\s+au\\s+traitement\\s+de)",
    "\\s+l['’]affection\\s+de\\s+longue\\s+dur[eé]e",
    "[\\s\\S]{0,140}?",
    "\\(\\s*maladies\\s+intercurrentes\\s*\\)"
  ),
  # The repeated page furniture of a paginated results table pasted into a
  # document. The results themselves are not touched; only the header that
  # restarts on every page.
  # The department letterhead, which fragments into a dozen near-identical
  # variants because the list of consultants changes. Both boundaries are
  # administrative, so the block is taken whole however long the staff list.
  # Site-specific, like the preamble rule: another hospital needs its own.
  establishment_letterhead = paste0(
    "(?is)centre\\s+hospitalier\\s+universitaire\\s+de\\s+rouen",
    "[\\s\\S]{0,400}?",
    "chefs?\\s+de\\s+clinique",
    "(?:[\\s\\S]{0,120}?assistants?\\s*:(?:\\s*dr",
    .DOCEDS_WORD_END,
    ")*)?",
    "(?:[\\s\\S]{0,80}?attach[eé]s?\\s*:)?"
  ),
  # The pathology and molecular biology letterheads, which both close on an
  # identity line whose labels were never filled in.
  pathology_letterhead = paste0(
    "(?is)(?:centre\\s+hospitalier[\\s\\S]{0,200}?)?",
    "(?:service\\s+d['’]anatomie\\s+et\\s+cytologie\\s+pathologiques",
    "|plateforme\\s+r[eé]gionale\\s+de\\s+pathologie)",
    "[\\s\\S]{0,700}?",
    "(?:concernant\\s*:+|nom\\s*:+\\s*naissance",
    "|num[eé]ro\\s+d?['’]?\\s*hospitalisation\\s*:)",
    "[^\\r\\n]{0,60}"
  ),
  # The page furniture of a printed results report: the page number and the
  # patient banner whose fields carry no values.
  results_header = paste0(
    "(?is)(?:19|20)[0-9]{2}\\s*-\\s*page\\s+[0-9]+",
    "[\\s\\S]{0,80}?patient\\s*:",
    "[\\s\\S]{0,160}?sexe\\s*:[^\\r\\n]{0,40}"
  ),
  # A results table pasted into a document, removed as a matter of evidence
  # scope rather than as deduplication.
  #
  # The policy: for quality control of the coding attached to this EVTID, the
  # structured `biol` table is the authoritative model-visible source of
  # biology. It reaches the model as citable records naming the analyte and its
  # reference range, with the sampling date the warehouse recorded. A table
  # pasted into a letter has weaker temporal attribution — it is whatever the
  # author copied on the day they wrote — and can carry historical values that
  # must not be read as supporting a code for the current stay.
  #
  # This is deliberately **not** row-level deduplication, and the difference is
  # worth being exact about: in roughly a fifth of the documents this family
  # fires on, the newest value in the table is more than a month older than the
  # document and is not in `biol` for that stay, so those rows are removed
  # without an equivalent elsewhere. That is the intended trade-off — a stale
  # value that cannot be dated reliably is a worse basis for a code than no
  # value — and not an oversight. Matching analyte, date and value against the
  # bundle to keep unmatched rows is a different design and is not in scope.
  #
  # Recognised as a run of lines rather than a bounded span, because a table is
  # as long as it is: a group title is indented and carries no digit, a result
  # line starts at the margin and is analyte, date, value, unit. The run ends at
  # the first line that is neither.
  # Either heading opens it: a long table is paginated, and every page after
  # the first restarts at the column headings without repeating the title.
  lab_table = paste0(
    "(?im)^\\h*(?:derniers\\h+r[eé]sultats[^\\r\\n]*",
    "|examen\\h+date\\h+valeur\\h*)\\r?\\n",
    "(?:",
    .DOCEDS_LAB_LINE,
    "\\r?\\n){2,}+"
  ),
  # The letter a patient is told to bring to the admissions desk.
  admissions_notice = paste0(
    "(?is)courrier\\s+[àa]\\s+pr[eé]senter\\s+au\\s+bureau\\s+des\\s+admissions",
    "[\\s\\S]{0,600}?",
    "carte\\s+vitale\\s+et\\s+de\\s+cette\\s+lettre\\.?"
  )
  # `results_pagination` and `emergency_letterhead` used to sit here and fired
  # on zero documents out of 62,444. A family that never matches is not
  # ward-specific, it is wrong: the first was subsumed by `results_header`, and
  # the emergency letterhead turned out to have the same shape as every other
  # one, which `document_header` already handles.
)
