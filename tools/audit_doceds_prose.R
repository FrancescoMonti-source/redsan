# Does any trimming rule remove clinical narrative?
#
# This file is deliberately outside R/: it is an audit workflow, not part of the
# redsan API. It never modifies the input documents.
#
# The other two scripts ask what noise still reaches the model and what each
# rule earns. Neither can answer the question that decides whether a rule may
# ship at all: does it take prose with it. Synthetic tests cannot answer it
# either — they only contain what their author already thought of, and every
# defect found during this work was a shape nobody had thought of. This one
# reads every span the rules actually removed from a real corpus and looks for
# narrative inside it.
#
# It found seven defects, five in rules that had been running for weeks:
# a header criterion that accepted any line merely containing a merge field,
# a 600-character reach-back, a two-digit year the date rule rejected, a
# doctor's title accepted anywhere in a line rather than at its head, and a
# preamble rule that cut everything before the date marker instead of the
# header block in front of it.
#
#   source("tools/audit_doceds_prose.R")
#   audit <- audit_doceds_prose(docs)
#   audit$by_family                       # `with_prose` is what gets read
#   show_doceds_prose_hits(audit)         # read every one that is not

# The document types redsancoding never shows its language model. The trimmer
# does not care about RECTYPE and this filter is no part of it: it is here so
# the measured population is the one the recorded baseline was taken on, and so
# the two scripts stay comparable with each other. redsancoding owns the policy
# — see `find_cim10_evidence()` there — and this is a hand-kept copy of it.
DOCEDS_MODEL_EXCLUDED_RECTYPES <- c("OPROOM", "BT")

# The corpus both audits describe. Repeated in measure_doceds_families.R
# because these scripts are sourced independently; keep the two in step, since
# the RECTYPE exclusion is what makes their numbers comparable.
doceds_audit_corpus <- function(text, sample_size, seed) {
  if (is.data.frame(text)) {
    if (!"RECTXT" %in% names(text)) {
      stop("`text` must be a character vector or carry a RECTXT column.")
    }
    if ("RECTYPE" %in% names(text)) {
      keep <- is.na(text$RECTYPE) |
        !as.character(text$RECTYPE) %in% DOCEDS_MODEL_EXCLUDED_RECTYPES
      text <- text[keep, , drop = FALSE]
    }
    text <- text$RECTXT
  }
  text <- as.character(text)
  text <- text[!is.na(text) & nzchar(trimws(text))]
  if (!is.null(sample_size) && length(text) > sample_size) {
    set.seed(seed)
    text <- text[sample(length(text), sample_size)]
  }
  text
}

#' Turns of phrase that only occur when a clinician is narrating
#'
#' Not a vocabulary list. Drug names, anatomy and diagnoses all appear in
#' letterheads, consent forms and laboratory panels, so matching them flags
#' hundreds of correct removals and buries the real hits. What never appears in
#' boilerplate is a sentence with a subject and a tense: someone observing,
#' deciding, or reporting a change. Grammar is the discriminator.
#'
#' This constant is the only French-specific part of the audit. On another
#' corpus, replace it and the rest of the file works unchanged.
# Several alternatives are deliberately stems rather than whole words, so that
# one of them covers `réalisé`, `réalisée` and `réalisation`. A trailing `\b`
# would undo exactly that: it demands a word boundary where the stem stops, in
# the middle of the very inflections it was truncated to reach, and the marker
# then fires only on the form nobody writes. It used to sit here, and it cost
# `a été réalisée`, `a été débutée`, `a été arrêté`, `pas de signes`, `absence
# de complications` and `bien tolérée` — half the list, and the commonest half.
# There is no closing boundary now: for a gate whose failure mode is missing
# prose, matching a little too eagerly costs a span to read, and matching too
# strictly costs the guarantee.
DOCEDS_PROSE_MARKERS <- paste0(
  "(?i)\\b(?:",
  # someone is the subject of a clinical verb
  "le\\s+patient\\s+(?:a|est|pr[eé]sente|se|ne)",
  "|la\\s+patiente\\s+(?:a|est|pr[eé]sente|se|ne)",
  "|on\\s+(?:note|retrouve|observe|constate)",
  "|il\\s+(?:existe|persiste|s'agit|n'y\\s+a)",
  "|nous\\s+(?:avons|proposons|retenons)",
  # something was done, or reasoned about
  "|(?:a|ont)\\s+[eé]t[eé]\\s+(?:r[eé]alis|introduit|d[eé]but|major|arr[eê]t",
  "|hospitalis|adress)",
  "|en\\s+raison\\s+d(?:e|'un)",
  "|au\\s+d[eé]cours\\s+de",
  "|devant\\s+(?:une|un|la|le)",
  # a course over time, which no template ever describes
  "|(?:pas|absence)\\s+d(?:e|')\\s*(?:signe|argument|complication|r[eé]cidive)",
  "|(?:majoration|introduction|diminution|arr[eê]t)\\s+d(?:u|e\\s+la|es)",
  "|bien\\s+tol[eé]r[eé]|s'am[eé]liore|s'aggrave",
  ")"
)

# Every span the trimming removed from one document, in the coordinates of that
# document — block families, the preamble, and the two rules that act inside a
# line. The projection reports them, so nothing is reconstructed here.
#
# Reconstruction is what this function used to do, and it was wrong twice. It
# rebuilt the preamble as `1:removed_prefix_chars`, but the preamble does not
# start at 1 — the walk-back stops at the first line that is not header-shaped —
# so the audit read the kept opening instead of the cut frame and cleared a rule
# it had never looked at. And it only ever saw `boilerplate_intervals`, so the
# placeholder and fill-run rules, which were substitutions applied after the
# cut, were invisible to it: that is how a rule reading every French quotation
# as a Word merge field survived an audit whose whole purpose is catching it.
.doceds_removed_spans <- function(one) {
  redsan::trim_doceds_text(one)$removed_intervals[
    , c("start", "end", "family"),
    drop = FALSE
  ]
}

#' Look for clinical narrative inside every span the rules removed
#'
#' @param text A character vector of raw `RECTXT`, or a DOCEDS data frame.
#' @param sample_size Documents to inspect. `NULL` for all of them. Twenty
#'   thousand is enough to give every family thousands of spans; the rare
#'   families are the reason to go higher, not the common ones.
#' @param seed Sampling seed.
#' @param prose Regex describing clinical narrative. See `DOCEDS_PROSE_MARKERS`.
#' @param context Characters of surrounding text stored with each hit, so a hit
#'   can be judged without going back to the corpus.
#'
#' @return A list. `by_family` is one row per family label with the spans it
#'   removed and how many of them contained narrative. **The target for
#'   `with_prose` is as low as reasonably possible, not zero**: it is a reading
#'   list, every hit has to be classed as a correct removal or a defect, and the
#'   residue has to be accepted deliberately rather than tuned away. Chasing the
#'   count to zero by narrowing the markers is how the gate goes blind — see the
#'   baseline in `tools/README.md`, where 35 of 39 hits are one patient-advice
#'   template that is right to remove.
#'   Labels containing `+` are spans that two families both claimed;
#'   blaming the wrong one of the two costs a day, so they are kept separate.
#'   `hits` carries every offending span with its context. `warnings` counts
#'   documents where the regex engine gave up: those are trimmed silently and
#'   incompletely, so a non-zero count invalidates the audit rather than merely
#'   annoying it. Hits are patient text; keep the result local.
audit_doceds_prose <- function(text, sample_size = 20000L, seed = 2L,
                               prose = DOCEDS_PROSE_MARKERS, context = 120L) {
  text <- doceds_audit_corpus(text, sample_size, seed)
  families <- vector("list", length(text))
  flagged <- vector("list", length(text))
  hits <- list()
  warned <- 0L

  for (i in seq_along(text)) {
    one <- text[[i]]
    ok <- TRUE
    intervals <- withCallingHandlers(
      .doceds_removed_spans(one),
      warning = function(cond) {
        ok <<- FALSE
        invokeRestart("muffleWarning")
      }
    )
    if (!ok) warned <- warned + 1L
    if (!nrow(intervals)) next

    cut <- substring(one, intervals$start, intervals$end)
    prose_found <- grepl(prose, cut, perl = TRUE)
    families[[i]] <- intervals$family
    flagged[[i]] <- intervals$family[prose_found]

    for (j in which(prose_found)) {
      marker <- regmatches(cut[[j]], regexpr(prose, cut[[j]], perl = TRUE))
      hits[[length(hits) + 1L]] <- data.frame(
        doc = i,
        family = intervals$family[[j]],
        start = intervals$start[[j]],
        end = intervals$end[[j]],
        marker = if (length(marker)) marker else NA_character_,
        before = substr(
          one, max(1L, intervals$start[[j]] - context),
          intervals$start[[j]] - 1L
        ),
        cut = cut[[j]],
        after = substr(
          one, intervals$end[[j]] + 1L, intervals$end[[j]] + context
        ),
        stringsAsFactors = FALSE
      )
    }
  }

  all_spans <- table(unlist(families))
  with_prose <- table(unlist(flagged))
  by_family <- data.frame(
    family = names(all_spans),
    spans = as.integer(all_spans),
    with_prose = as.integer(with_prose[names(all_spans)]),
    stringsAsFactors = FALSE
  )
  by_family$with_prose[is.na(by_family$with_prose)] <- 0L
  by_family$pct <- round(100 * by_family$with_prose / by_family$spans, 2)
  by_family <- by_family[order(-by_family$with_prose, -by_family$spans), ]
  rownames(by_family) <- NULL

  list(
    documents = length(text),
    spans = sum(by_family$spans),
    by_family = by_family,
    hits = if (length(hits)) do.call(rbind, hits) else NULL,
    warnings = warned
  )
}

#' Read the flagged spans
#'
#' Every hit is either a rule to fix or a marker to narrow, and telling the two
#' apart needs the text. Prints what preceded the cut, the cut itself, and what
#' followed, because a boundary is only wrong relative to its neighbours.
#'
#' @param audit The result of `audit_doceds_prose()`.
#' @param n Hits to print.
#' @param chars Characters of the removed span to print.
show_doceds_prose_hits <- function(audit, n = 10L, chars = 900L) {
  if (is.null(audit$hits)) {
    cat("no removed span contained clinical narrative\n")
    return(invisible(NULL))
  }
  hits <- audit$hits
  for (i in seq_len(min(n, nrow(hits)))) {
    cat(sprintf(
      "--- %s, document %d, %d to %d, matched \"%s\" ---\n",
      hits$family[[i]], hits$doc[[i]], hits$start[[i]], hits$end[[i]],
      hits$marker[[i]]
    ))
    cat("BEFORE: ", hits$before[[i]], "\n", sep = "")
    cat("CUT:    ", substr(hits$cut[[i]], 1L, chars), "\n", sep = "")
    cat("AFTER:  ", hits$after[[i]], "\n\n", sep = "")
  }
  invisible(hits)
}

#' What a family swallows, line by line
#'
#' A family's yield says how much it removed, never what. A rule with a
#' permissive alternative — a line of any words with no digits, say — earns its
#' characters from the table it was written for and then quietly takes whatever
#' else has that shape. Listing the distinct lines it removed shows this in one
#' screen: two dozen laboratory panel titles repeated thousands of times is a
#' rule doing its job, and a long tail of sentences is not.
#'
#' Sorted by frequency, but read the longest ones first — a swallowed sentence
#' is longer than the labels around it and occurs once, so it sits at the
#' bottom of a frequency ranking and at the top of a length ranking.
#'
#' @param text A character vector of raw `RECTXT`, or a DOCEDS data frame.
#' @param family Family label to inspect, matched as a substring so combined
#'   labels like `document_header+identity_line` are included.
#' @param pattern Optional regex selecting which removed lines to report, for
#'   isolating one alternative of the family's rule.
#' @param sample_size Documents to inspect. `NULL` for all of them.
#' @param seed Sampling seed.
#'
#' @return A data frame of distinct lines and their counts.
doceds_removed_lines <- function(text, family, pattern = NULL,
                                 sample_size = 20000L, seed = 2L) {
  text <- doceds_audit_corpus(text, sample_size, seed)
  collected <- vector("list", length(text))
  for (i in seq_along(text)) {
    intervals <- .doceds_removed_spans(text[[i]])
    keep <- grepl(family, intervals$family, fixed = TRUE)
    if (!any(keep)) next
    intervals <- intervals[keep, , drop = FALSE]
    lines <- unlist(strsplit(
      substring(text[[i]], intervals$start, intervals$end), "\r?\n"
    ))
    if (!is.null(pattern)) {
      lines <- lines[grepl(pattern, lines, perl = TRUE)]
    }
    collected[[i]] <- trimws(lines)
  }
  lines <- unlist(collected)
  lines <- lines[nzchar(lines)]
  counts <- sort(table(lines), decreasing = TRUE)
  data.frame(
    line = names(counts),
    n = as.integer(counts),
    chars = nchar(names(counts)),
    stringsAsFactors = FALSE
  )
}
