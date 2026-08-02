# What every DOCEDS trimming rule is actually worth, on real documents.
#
# This file is deliberately outside R/: it is an audit workflow, not part of the
# redsan API. It never modifies the input documents.
#
# The exploration in explore_doceds_repetition.R answers "what noise is still
# reaching the model". This one answers the other half: "does the rule I just
# wrote fire, and what does it earn". Reading the first to answer the second is
# indirect and misleads — a family can be absent from a candidate list because
# it works, or because the sample changed, or because nobody looked far enough
# down. A family reported here with zero documents is broken, full stop.
#
#   source("tools/measure_doceds_families.R")
#   yields <- measure_doceds_families(docs$RECTXT)
#   yields$families
#   yields$removed_share
#
# Neither this nor the exploration can say whether a rule takes clinical text
# with it. That is audit_doceds_prose.R, and it is the one that decides whether
# a rule may ship.

# The document types redsancoding never shows its language model. The trimmer
# does not care about RECTYPE and this filter is no part of it: it is here so
# the measured population is the one the recorded baseline was taken on, and so
# the two scripts stay comparable with each other. redsancoding owns the policy
# — see `find_cim10_evidence()` there — and this is a hand-kept copy of it.
DOCEDS_MODEL_EXCLUDED_RECTYPES <- c("OPROOM", "BT")

# The corpus both audits describe. Repeated in audit_doceds_prose.R because
# these scripts are sourced independently; keep the two in step, since the
# RECTYPE exclusion is what makes their numbers comparable.
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

#' Price every trimming rule against a corpus of documents
#'
#' @param text A character vector of raw `RECTXT`, or a DOCEDS data frame.
#' @param sample_size Documents to inspect. `NULL` for all of them.
#' @param seed Sampling seed.
#'
#' @return A list. `families` is one row per boilerplate family with the
#'   documents it fired on and `standalone_chars`, what it would remove on its
#'   own, ordered by yield. Those figures overlap between families and are not
#'   additive on purpose: the question each family answers is what would stay
#'   behind if it alone were dropped. `inline_rules` covers the two rules that
#'   act within a line rather than cutting blocks, plus the preamble.
#'   `removed_share` is the per-document distribution, which is what
#'   makes a changed layout visible. `overall` totals the corpus, and its
#'   `removed` is the exact net difference. Representative text is
#'   patient-derived; keep the result local.
measure_doceds_families <- function(text, sample_size = 3000L, seed = 1L) {
  text <- doceds_audit_corpus(text, sample_size, seed)
  trimmed <- lapply(text, redsan::trim_doceds_text)
  before <- sum(nchar(text))
  after <- sum(vapply(trimmed, function(row) nchar(row$text), integer(1)))

  patterns <- names(redsan:::.DOCEDS_BOILERPLATE_PATTERNS)
  counts <- table(unlist(lapply(trimmed, `[[`, "boilerplate_families")))
  chars <- redsan::doceds_family_chars(
    lapply(trimmed, `[[`, "boilerplate_family_standalone_chars")
  )
  families <- data.frame(
    family = patterns,
    documents = as.integer(counts[patterns]),
    standalone_chars = as.integer(chars[patterns]),
    stringsAsFactors = FALSE
  )
  families[is.na(families)] <- 0L
  families$share <- families$standalone_chars / before
  families <- families[order(-families$standalone_chars), ]
  rownames(families) <- NULL

  total <- function(field) {
    sum(vapply(trimmed, `[[`, integer(1), field))
  }
  list(
    overall = list(
      documents = length(text),
      chars_in = before,
      chars_out = after,
      removed = (before - after) / before
    ),
    families = families,
    inline_rules = data.frame(
      rule = c("fields", "rule_runs", "preamble"),
      chars = c(
        total("placeholders_standalone_chars"),
        total("rule_runs_standalone_chars"),
        total("removed_prefix_chars")
      ),
      stringsAsFactors = FALSE
    ),
    removed_share = stats::quantile(
      vapply(trimmed, `[[`, numeric(1), "removed_share"),
      c(0.5, 0.75, 0.9, 0.95, 0.99, 1)
    ),
    near_total_match_detected = sum(vapply(
      trimmed, `[[`, logical(1), "near_total_match_detected"
    ))
  )
}

#' Show what a rule removed from one document, in place
#'
#' Prints the document with removed spans marked, so a boundary can be judged
#' rather than guessed at from normalized text. This is the check to run before
#' trusting a new family: a regex written from the exploration output is written
#' from lowercased, whitespace-collapsed text and cannot see line structure.
#'
#' @param one A single raw `RECTXT`.
#' @param context Characters of surrounding text to show around each removal.
show_doceds_removals <- function(one, context = 80L) {
  trimmed <- redsan::trim_doceds_text(one)
  # Every span that was actually removed, the preamble and the inline rules
  # among them, reported by the projection in original coordinates. This used to
  # read `boilerplate_intervals` and re-derive the preamble beside it, which
  # showed neither the placeholders nor the fill runs and got the preamble's
  # start wrong — it began at `removed_prefix_start`, not at 1, so on a document
  # opening with prose the helper displayed the kept opening as though it had
  # been cut. Displaying a boundary that was never examined is the one failure
  # this helper exists to prevent.
  intervals <- trimmed$removed_intervals
  cat(sprintf(
    "%d chars, %.1f%% removed, families: %s\n\n",
    nchar(one), 100 * trimmed$removed_share,
    if (nrow(intervals)) {
      paste(unique(intervals$family), collapse = ", ")
    } else {
      "(none)"
    }
  ))
  for (i in seq_len(nrow(intervals))) {
    cat(sprintf(
      "--- %s, %d to %d ---\n",
      intervals$family[[i]], intervals$start[[i]], intervals$end[[i]]
    ))
    cat("BEFORE: ", substr(
      one, max(1L, intervals$start[[i]] - context), intervals$start[[i]] - 1L
    ), "\n", sep = "")
    cat("CUT:    ", substr(
      one, intervals$start[[i]], min(intervals$end[[i]], intervals$start[[i]] + 300L)
    ), "\n", sep = "")
    cat("AFTER:  ", substr(
      one, intervals$end[[i]] + 1L, intervals$end[[i]] + context
    ), "\n\n", sep = "")
  }
  invisible(trimmed)
}
