# What the text closes with where a span was removed. A block cut takes whole
# lines and closes with a newline; an inline field is lifted out of the middle of
# a sentence and closes with the space that held it, so the words do not run
# together; a run of fill characters closes with nothing. Every destructive rule
# in this file is one of these three, which is what lets them share a pipeline
# instead of some cutting spans and others substituting tokens afterwards.
.DOCEDS_JOINS <- c(block = "\n", field = " ", rule = "")

# When two spans merge, the widest separator wins: a merged span containing a
# block cut is a block cut.
.widest_join <- function(joins) {
  .DOCEDS_JOINS[[min(match(joins, .DOCEDS_JOINS))]]
}

.regex_intervals <- function(text, pattern, family, join = .DOCEDS_JOINS[["block"]]) {
  matches <- gregexpr(pattern, text, perl = TRUE)[[1L]]
  if (identical(matches[[1L]], -1L)) {
    return(data.frame(
      start = integer(),
      end = integer(),
      family = character(),
      join = character()
    ))
  }
  lengths <- attr(matches, "match.length")
  data.frame(
    start = as.integer(matches),
    end = as.integer(matches + lengths - 1L),
    family = rep(family, length(matches)),
    join = rep(join, length(matches))
  )
}

.merge_intervals <- function(intervals) {
  if (!nrow(intervals)) {
    # Same columns empty as full: these frames are rbound across documents, and
    # a frame that loses `removed_chars` only because it happens to be empty is
    # an error at the join rather than an empty row.
    if (is.null(intervals$removed_chars)) {
      intervals$removed_chars <- integer()
    }
    return(intervals)
  }
  intervals <- intervals[order(intervals$start, intervals$end), , drop = FALSE]
  merged <- list()
  current <- intervals[1L, , drop = FALSE]
  if (nrow(intervals) > 1L) {
    for (i in seq.int(2L, nrow(intervals))) {
      candidate <- intervals[i, , drop = FALSE]
      if (candidate$start[[1L]] <= current$end[[1L]] + 1L) {
        current$end[[1L]] <- max(current$end[[1L]], candidate$end[[1L]])
        current$family[[1L]] <- paste(
          unique(c(
            strsplit(current$family[[1L]], "\\+", fixed = FALSE)[[1L]],
            candidate$family[[1L]]
          )),
          collapse = "+"
        )
        current$join[[1L]] <- .widest_join(
          c(current$join[[1L]], candidate$join[[1L]])
        )
      } else {
        merged[[length(merged) + 1L]] <- current
        current <- candidate
      }
    }
  }
  merged[[length(merged) + 1L]] <- current
  result <- do.call(rbind, merged)
  result$removed_chars <- result$end - result$start + 1L
  rownames(result) <- NULL
  result
}

# Cut `protected` out of every interval, splitting one in two when a protected
# span sits inside it. Both frames are in original coordinates and `protected`
# is sorted, so a single forward pass per interval is enough.
.subtract_intervals <- function(intervals, protected) {
  if (!nrow(intervals) || !nrow(protected)) {
    return(intervals)
  }
  protected <- .merge_intervals(protected)
  pieces <- list()
  for (i in seq_len(nrow(intervals))) {
    start <- intervals$start[[i]]
    end <- intervals$end[[i]]
    family <- intervals$family[[i]]
    join <- intervals$join[[i]]
    for (j in seq_len(nrow(protected))) {
      if (protected$end[[j]] < start) {
        next
      }
      if (protected$start[[j]] > end) {
        break
      }
      if (protected$start[[j]] > start) {
        pieces[[length(pieces) + 1L]] <- data.frame(
          start = start,
          end = protected$start[[j]] - 1L,
          family = family,
          join = join
        )
      }
      start <- protected$end[[j]] + 1L
      if (start > end) break
    }
    if (start <= end) {
      pieces[[length(pieces) + 1L]] <- data.frame(
        start = start,
        end = end,
        family = family,
        join = join
      )
    }
  }
  # Same columns whatever happens, including when a protected span covered
  # every interval: these frames are rbound across documents downstream, and a
  # missing column there is an error rather than an empty row.
  if (!length(pieces)) {
    return(data.frame(
      start = integer(),
      end = integer(),
      family = character(),
      join = character(),
      removed_chars = integer()
    ))
  }
  result <- do.call(rbind, pieces)
  result$removed_chars <- result$end - result$start + 1L
  rownames(result) <- NULL
  result
}

# The single cut. Every rule in the file has already contributed its spans in
# the coordinates of the original document, so this runs once and is the only
# place the string is edited. Each surviving piece is joined to the next by the
# separator of the span that was removed between them, which is what lets a
# block cut and an inline field share this function: the block closes with a
# newline, the field with the space it sat in. A separator is only ever written
# between two surviving pieces, so a removal at either end of the document
# leaves no leading or trailing filler.
.remove_text_intervals <- function(text, intervals) {
  if (!nrow(intervals)) {
    return(text)
  }
  intervals <- .merge_intervals(intervals)
  pieces <- character()
  joins <- character()
  cursor <- 1L
  for (i in seq_len(nrow(intervals))) {
    # `.merge_intervals()` has already fused anything adjacent, so a kept piece
    # is missing only when the document opens with a removal.
    if (cursor < intervals$start[[i]]) {
      pieces <- c(pieces, substr(text, cursor, intervals$start[[i]] - 1L))
      joins <- c(joins, intervals$join[[i]])
    }
    cursor <- intervals$end[[i]] + 1L
  }
  if (cursor <= nchar(text)) {
    pieces <- c(pieces, substr(text, cursor, nchar(text)))
  } else if (length(joins)) {
    joins <- joins[-length(joins)]
  }
  if (!length(pieces)) {
    return("")
  }
  trimws(paste0(pieces, c(joins, "")[seq_along(pieces)], collapse = ""))
}

# The two inline rules, as spans in the coordinates of the original document
# rather than as substitutions applied afterwards. A placeholder that survives
# inside prose is a redacted name and only costs context, so it goes with the
# horizontal space that held it and the sentence closes over a single space; a
# run of fill characters closes over nothing.
#
# These used to be `gsub()` calls on the already-trimmed text, and that placed
# them outside everything that makes the rest of the trimming safe: the prose
# audit reconstructs intervals and so never saw them, protected constants could
# not shelter from them, and they ran after the near-total-match check, so a
# document that check had rescued was edited anyway. That is how the guillemet
# rule deleted `« C4d positif sans évidence de rejet »` with nothing watching.
.inline_intervals <- function(text) {
  fields <- .regex_intervals(
    text,
    paste0("\\h*", .DOCEDS_FIELD_PATTERN, "\\h*"),
    "field",
    .DOCEDS_JOINS[["field"]]
  )
  # A placeholder lifted from between two words leaves the space that held them
  # apart; one lifted from in front of a full stop, or off the end of a line,
  # must leave nothing. Decided per occurrence, from what actually sits on each
  # side, because the alternative is a global tidy-up afterwards — and a global
  # rewrite of `\h+` before punctuation reaches lines no rule touched, including
  # the protected ones: it turned `Poids : 144 kg` into `Poids: 144 kg`.
  # Judged on what follows the span, not on what precedes it: the span has
  # already eaten its own leading `\h*`, and when two placeholders sit side by
  # side the character before the second one was consumed by the first. Reading
  # it anyway made `[PATIENT]  [LASTNAME]` close up into `inscritsur`. Adjacent
  # spans are fused by `.merge_intervals()` before any of this is applied, so a
  # pair contributes one separator, not two.
  # `substring()`, not `substr()`: with one string and several positions the
  # latter answers for the first position only and recycles that answer over
  # every span, so one placeholder at the end of a line decided the spacing for
  # all of them.
  if (nrow(fields)) {
    after <- substring(text, fields$end + 1L, fields$end + 1L)
    before <- substring(text, pmax(fields$start - 1L, 1L), pmax(fields$start - 1L, 1L))
    fields$join[
      fields$start == 1L |
        !nzchar(after) |
        grepl("[\\s.,;:!?)]", after, perl = TRUE) |
        grepl("[\\r\\n]", before, perl = TRUE)
    ] <- .DOCEDS_JOINS[["rule"]]
  }
  rbind(
    fields,
    .regex_intervals(
      text,
      .DOCEDS_RULE_RUN_PATTERN,
      "rule_run",
      .DOCEDS_JOINS[["rule"]]
    )
  )
}

# Horizontal whitespace left at the end of a line by a removal. Idempotent and
# content-free: it is the one edit that is not accounted for as an interval,
# because there is nothing to account for.
.tidy_spacing <- function(text, changed) {
  if (!changed) {
    return(text)
  }
  gsub("(?m)\\h+$", "", text, perl = TRUE)
}

# Where the header block that precedes the letter date begins, walking back
# from the end of `prefix` while the lines are header-shaped. Done line by line
# rather than with one anchored repetition, because `regexpr` retries such a
# pattern from every position: on a long document that is quadratic, and PCRE
# answered by exhausting its match limit and reporting no match at all, which
# silently skipped the trim.
.header_run_start <- function(prefix) {
  lines <- strsplit(prefix, "\n", fixed = TRUE)[[1L]]
  if (!length(lines)) {
    return(1L)
  }
  # The families embed this pattern after `(?im)`; used on its own it needs the
  # flag restated, or a letterhead in capitals matches nothing.
  header_line <- paste0("(?i)^", .DOCEDS_HEADER_LINE, "\\r?$")
  last <- length(lines)
  while (last >= 1L && grepl(header_line, lines[[last]], perl = TRUE)) {
    last <- last - 1L
  }
  if (last >= length(lines)) {
    return(nchar(prefix) + 1L)
  }
  if (last < 1L) {
    return(1L)
  }
  # The marker sits at a line start, so `prefix` ends with a newline and every
  # line costs its own characters plus that separator.
  sum(nchar(lines[seq_len(last)]) + 1L) + 1L
}

#' Remove the administrative frame from one DOCEDS document
#'
#' Removes the letterhead, the recognised boilerplate families, the Word field
#' residue and the fill-character runs from one document's text, and reports
#' exactly what it took.
#'
#' @details
#' The administrative rules remove letterheads, correspondence blocks, RGPD
#' notices, unfilled identity banners, page furniture, and the placeholders and
#' fill runs a Word template leaves behind. They are meant not to touch what a
#' clinician wrote, and the instruments under `tools/` exist to keep checking
#' that boundary.
#'
#' The optional `lab_table` family is different. With
#' `remove_lab_tables = TRUE` it removes recognised pasted laboratory tables,
#' which can contain clinician-authored clinical values. This is an explicit
#' evidence-scope policy, not part of the administrative-frame guarantee. It is
#' enabled by default and can be disabled by the caller.
#'
#' Every rule contributes candidate spans in the coordinates of the **original**
#' document rather than editing the string. Lines carrying a measured constant —
#' `TA : 130/80`, `Poids : 144 kg` — are subtracted from those spans first, so a
#' family that swallowed a vital sign gives it back. What survives is applied in
#' a single pass, which is what makes the removals auditable, order-independent
#' and reportable as `removed_intervals`.
#'
#' Each span carries the separator the text closes with: a newline for a block
#' cut, the space it occupied for an inline field, nothing for a fill run. When
#' spans merge, the widest separator wins.
#'
#' `near_total_match_detected` is a **diagnostic for one failure** — a rule that
#' ran away on a layout nobody has seen and matched essentially the whole
#' document — and not a safety margin. A document losing 99.4 percent is not
#' clinically different from one losing 99.6, so no guarantee about clinical
#' text rests on which side of it a document falls. When it fires, nothing is
#' removed and the original text is returned.
#'
#' The rules are site-specific to the Rouen corpus they were measured against.
#'
#' @param text One document's text, normally one `RECTXT` value. Length 0, `NA`
#'   and `""` return the input unchanged with zeroed counts. A longer vector is
#'   an error rather than a silent `NA`: map over it with [lapply()].
#' @param remove_lab_tables Whether to remove recognised pasted laboratory
#'   tables. Defaults to `TRUE`. Set `FALSE` when that source content must remain
#'   visible.
#'
#' @return A list. `text` is what survives; `net_removed_chars` is the exact
#'   difference in characters between the input and it, and is the **only
#'   total** in the list. Every other count is standalone — measured against its
#'   own rule's matches over the whole document — so counts overlap each other
#'   and must not be summed. A placeholder inside a letterhead is counted both
#'   by `placeholders_standalone_chars` and by the family that cuts the
#'   letterhead.
#'
#'   The remaining elements are `marker_count` and `marker_within_limit` for the
#'   letter-date frame boundary; `preamble_removed`, `removed_prefix_chars` and
#'   `removed_prefix_start` for the leading letterhead; `boilerplate_removed`,
#'   `boilerplate_removed_chars`, `boilerplate_families` and
#'   `boilerplate_family_standalone_chars` for the block families;
#'   `placeholders_standalone_chars` and `rule_runs_standalone_chars` for the
#'   two inline rules; `removed_share`; `near_total_match_detected`; and two
#'   span tables, `boilerplate_intervals` and `removed_intervals`. The latter is
#'   everything actually removed, after protection and after the near-total
#'   check, and is what an audit should read.
#'
#' @seealso [process_doceds()] for the table this text comes from.
#'
#' @examples
#' letter <- paste(
#'   "CENTRE HOSPITALIER UNIVERSITAIRE",
#'   "Rouen, le 12 mars 2024",
#'   "Le patient decrit une dyspnee d'effort depuis trois semaines.",
#'   sep = "\n"
#' )
#' trimmed <- trim_doceds_text(letter)
#' trimmed$text
#' trimmed$net_removed_chars
#'
#' @export
trim_doceds_text <- function(text, remove_lab_tables = TRUE) {
  remove_lab_tables <- .doceds_remove_lab_tables(remove_lab_tables)
  text <- as.character(text)
  if (length(text) > 1L) {
    stop(
      "`text` must be one document's text; use lapply() over a column.",
      call. = FALSE
    )
  }
  empty <- list(
    text = if (length(text) == 1L) text else NA_character_,
    net_removed_chars = 0L,
    marker_count = 0L,
    marker_within_limit = FALSE,
    preamble_removed = FALSE,
    removed_prefix_chars = 0L,
    removed_prefix_start = NA_integer_,
    boilerplate_removed = FALSE,
    boilerplate_removed_chars = 0L,
    boilerplate_family_standalone_chars = stats::setNames(integer(), character()),
    placeholders_standalone_chars = 0L,
    rule_runs_standalone_chars = 0L,
    removed_share = 0,
    near_total_match_detected = FALSE,
    boilerplate_families = character(),
    boilerplate_intervals = data.frame(
      start = integer(),
      end = integer(),
      family = character(),
      join = character(),
      removed_chars = integer()
    ),
    removed_intervals = data.frame(
      start = integer(),
      end = integer(),
      family = character(),
      join = character(),
      removed_chars = integer()
    )
  )
  if (length(text) != 1L || is.na(text) || !nzchar(text)) {
    return(empty)
  }

  body_matches <- gregexpr(
    .DOCEDS_BODY_START_PATTERN,
    text,
    perl = TRUE
  )[[1L]]
  marker_found <- !identical(body_matches[[1L]], -1L)
  starts <- if (marker_found) as.integer(body_matches) else integer()
  first <- if (marker_found) starts[[1L]] else NA_integer_
  within_limit <- marker_found && first <= .DOCEDS_PREAMBLE_LIMIT
  # Only the header block that actually precedes the date goes, not everything
  # before it. The original rule assumed a document opens with its frame, which
  # is false whenever the date line sits after some content: a cytology report
  # whose "Rouen le" appears a thousand characters in lost its technique
  # paragraph. Walking back through header-shaped lines and stopping at the
  # first that is not cannot cross prose.
  preamble_start <- if (within_limit && first > 1L) {
    .header_run_start(substr(text, 1L, first - 1L))
  } else {
    first
  }
  removed_prefix <- if (within_limit) first - preamble_start else 0L

  patterns <- .doceds_boilerplate_patterns(remove_lab_tables)
  by_family <- lapply(
    names(patterns),
    function(family) {
      .regex_intervals(
        text,
        patterns[[family]],
        family
      )
    }
  )
  names(by_family) <- names(patterns)
  boilerplate <- do.call(
    rbind,
    by_family
  )
  boilerplate <- .merge_intervals(boilerplate)
  # A line carrying a measured constant survives whatever family claimed it.
  # The nurse questionnaires are one long `formcheckbox` run with the answers
  # typed between the boxes, and removing the run whole took `Poids : 144 kg`
  # and `TA : 130/80` with the furniture. Removing the run is still right — an
  # extraction that loses which boxes were ticked turns a checklist into a list
  # of symptoms the patient appears to have — so the fix is to spare the lines
  # that carry a value rather than to keep the form. The preamble needs no such
  # treatment: its walk-back already stops at any line that is not
  # header-shaped, and a constant line is not.
  constants <- .regex_intervals(
    text,
    .DOCEDS_CONSTANT_LINE_PATTERN,
    "constant"
  )
  if (nrow(constants)) {
    boilerplate <- .merge_intervals(
      .subtract_intervals(boilerplate, constants)
    )
  }
  # What each family removed on its own, measured before the families are merged
  # into one another. The merge is lossy for this purpose: it keeps one span
  # carrying both labels and a single length, and crediting that length to each
  # label reports a family as worth the union of everything it happened to touch
  # — a rule that only ever fires inside another rule's span then looks exactly
  # as valuable as the rule containing it, which is the reading that decides
  # whether a family earns its risk. Measured per family an overlap is counted
  # twice, once at each family's own length, and that is the honest answer:
  # those characters really would leave if either rule stood alone. So these
  # figures can sum above `boilerplate_removed_chars`, and should.
  family_chars <- vapply(
    by_family,
    function(iv) {
      if (!nrow(iv)) {
        return(0L)
      }
      own <- .merge_intervals(iv)
      if (nrow(constants)) {
        own <- .subtract_intervals(own, constants)
      }
      if (!nrow(own)) {
        return(0L)
      }
      as.integer(sum(own$end - own$start + 1L))
    },
    integer(1)
  )
  family_chars <- family_chars[family_chars > 0L]

  # The inline rules join the block families here rather than running as
  # substitutions after the cut: same coordinates, same protection from a
  # measured constant, same near-total check, same single application, and
  # visible to the same audit.
  inline <- .inline_intervals(text)
  if (nrow(constants) && nrow(inline)) {
    inline <- .subtract_intervals(inline, constants)
  }

  columns <- c("start", "end", "family", "join")
  removals <- rbind(
    boilerplate[, columns, drop = FALSE],
    inline[, columns, drop = FALSE]
  )
  if (removed_prefix > 0L) {
    removals <- rbind(
      data.frame(
        start = preamble_start,
        end = first - 1L,
        family = "preamble",
        join = .DOCEDS_JOINS[["block"]]
      ),
      removals
    )
  }

  # A rule that runs away on a layout nobody has seen would otherwise empty a
  # document silently. This is a diagnostic for that one failure, not a general
  # safety margin: at 99.5 percent it fires only on a match that took
  # essentially the whole document, and a document losing 99.4 percent is not
  # clinically different from one losing 99.6. What keeps clinical text is the
  # rules being anchored and bounded, and the prose audit that reads what they
  # removed — never this number. When it does fire nothing is removed at all,
  # inline rules included: a document reaching the model whole is a recoverable
  # mistake, one reaching it gutted is not. The event is recorded rather than
  # warned about, because a batch of five hundred stays should not print five
  # hundred warnings.
  # Measured on the merged intervals, not the raw ones. The preamble covers the
  # start of the document and a header family usually covers part of the same
  # span, so summing them unmerged counted those characters twice and fired on
  # documents whose real removal was two thirds.
  removed_chars <- if (nrow(removals)) {
    merged <- .merge_intervals(removals)
    sum(merged$end - merged$start + 1L)
  } else {
    0L
  }
  near_total_match_detected <- nrow(removals) > 0L &&
    nchar(text) >= .DOCEDS_NEAR_TOTAL_MIN_CHARS &&
    removed_chars > .DOCEDS_NEAR_TOTAL_SHARE * nchar(text)
  if (near_total_match_detected) {
    removals <- removals[0L, , drop = FALSE]
    boilerplate <- boilerplate[0L, , drop = FALSE]
    inline <- inline[0L, , drop = FALSE]
    family_chars <- family_chars[0L]
    removed_prefix <- 0L
  }

  # Every rule has contributed spans in the coordinates of the original
  # document, so the string is edited exactly once and the audited coordinates
  # stay comparable with the source record.
  kept <- .remove_text_intervals(text, removals)
  # Only an inline removal can leave a gap in front of punctuation, so only an
  # inline removal earns the tidy-up. Running it after a block cut instead
  # rewrote typography nothing had touched: `Poids : 144 kg`, a protected
  # constant, came back as `Poids: 144 kg`.
  final <- .tidy_spacing(kept, nrow(inline) > 0L)
  inline_chars <- function(label) {
    own <- inline[inline$family == label, , drop = FALSE]
    if (!nrow(own)) {
      return(0L)
    }
    own <- .merge_intervals(own)
    as.integer(sum(own$end - own$start + 1L))
  }

  list(
    text = final,
    # The exact net difference between the original document and what the model
    # sees. Every other count here is a component and can overlap another; this
    # one is the arithmetic truth and is what `doceds_trimmed_chars` reports.
    net_removed_chars = as.integer(nchar(text) - nchar(final)),
    marker_count = length(starts),
    marker_within_limit = within_limit,
    preamble_removed = removed_prefix > 0L,
    removed_prefix_chars = as.integer(removed_prefix),
    # Where that prefix begins. It is not always 1: the walk-back stops at the
    # first line that is not header-shaped, so a document opening with prose
    # keeps it. An audit that assumes 1 reads the wrong span — the kept opening
    # instead of the cut frame — and clears a rule it never looked at.
    removed_prefix_start = if (removed_prefix > 0L) {
      as.integer(preamble_start)
    } else {
      NA_integer_
    },
    boilerplate_removed = nrow(boilerplate) > 0L,
    boilerplate_removed_chars = if (nrow(boilerplate)) {
      as.integer(sum(boilerplate$removed_chars))
    } else {
      0L
    },
    boilerplate_family_standalone_chars = family_chars,
    # Standalone like the families, and for the same reason: measured against
    # this rule's own matches over the whole document, so a placeholder sitting
    # inside a letterhead is counted here and again under the family that cuts
    # the letterhead. Most placeholders do sit inside one, which is why this
    # figure is well above what the inline rules contribute on their own to the
    # final text. Every per-rule count in this list overlaps every other;
    # `net_removed_chars` is the only total.
    placeholders_standalone_chars = inline_chars("field"),
    rule_runs_standalone_chars = inline_chars("rule_run"),
    removed_share = if (nchar(text)) {
      1 - nchar(final) / nchar(text)
    } else {
      0
    },
    near_total_match_detected = near_total_match_detected,
    boilerplate_families = if (nrow(boilerplate)) {
      unique(unlist(strsplit(boilerplate$family, "\\+")))
    } else {
      character()
    },
    boilerplate_intervals = boilerplate,
    # Everything that was actually removed, in the coordinates of the original
    # document: block families, the preamble, and the two inline rules, after
    # protected constants were subtracted and after the near-total check. This
    # is what the prose audit reads. It used to reconstruct the spans from
    # `boilerplate_intervals` plus a hand-rebuilt preamble, which meant the
    # inline rules were invisible to it and the preamble was rebuilt wrongly
    # more than once. Nothing has to be reconstructed now.
    removed_intervals = .merge_intervals(removals)
  )
}

#' Total removed characters per boilerplate family
#'
#' Sums the per-family character counts of several trimmed documents into one
#' named vector, ordered by size.
#'
#' @details
#' The totals are standalone and overlapping, like the counts they come from: a
#' span two families both matched is credited to both, so these figures do not
#' add up to anything and must not be summed. They answer "what would this
#' family alone remove", which is what pricing a rule needs.
#'
#' This cannot be recovered from the merged spans. A span two families matched
#' carries both labels and one length, and splitting the label while copying the
#' length credits each family with the union of everything it overlapped.
#'
#' @param per_document A list of `boilerplate_family_standalone_chars` vectors,
#'   one per document, as returned by [trim_doceds_text()].
#'
#' @return A named integer vector of characters per family, largest first, empty
#'   when nothing was removed.
#'
#' @examples
#' doceds_family_chars(list(
#'   c(rgpd = 120L, letter_header = 80L),
#'   c(rgpd = 60L)
#' ))
#'
#' @export
doceds_family_chars <- function(per_document) {
  counts <- unlist(unname(per_document), use.names = TRUE)
  if (!length(counts)) {
    return(stats::setNames(integer(), character()))
  }
  totals <- tapply(as.integer(counts), names(counts), sum)
  totals[order(-totals)]
}

# Every constant that decides what gets removed, in a stable order.
#
# The set is derived from the namespace rather than listed, and that is the whole
# point: a list would be one more thing to remember, and it would go stale
# exactly when it mattered — somebody adds a pattern, forgets the list, and the
# identity keeps claiming the rules are what they were. Naming a constant
# `.DOCEDS_*` is enough to put it under this.
#
# Sorted, so the digest describes the rules and not the order they happen to be
# defined in.
.doceds_rule_objects <- function(env = asNamespace("redsan")) {
  names <- sort(grep("^\\.DOCEDS_", ls(env, all.names = TRUE), value = TRUE))
  mget(names, envir = env)
}

# The rules flattened to one canonical string: each constant's name, then the
# name and value of every element in it.
#
# The separators are C0 control bytes, because they are the one thing a pattern
# here cannot contain, and a separator's whole job is to make two different rule
# sets impossible to flatten to the same text. Built from their byte values
# rather than written as literals: a control character sitting in source is
# invisible to whoever reads this next and does not survive every editor.
#
# Deliberately not named `.DOCEDS_*`. It is punctuation for the digest, not a
# rule, and a constant with that prefix would end up inside the very thing it
# helps compute.
.doceds_digest_separators <- function() {
  stats::setNames(
    vapply(1:4, function(byte) rawToChar(as.raw(byte)), character(1)),
    c("constant", "element", "between_elements", "between_constants")
  )
}

.doceds_rule_text <- function(objects) {
  sep <- .doceds_digest_separators()
  paste0(
    vapply(
      seq_along(objects),
      function(i) {
        value <- objects[[i]]
        paste0(
          names(objects)[[i]],
          sep[["constant"]],
          paste0(
            names(value),
            sep[["element"]],
            as.character(unlist(value)),
            collapse = sep[["between_elements"]]
          )
        )
      },
      character(1)
    ),
    collapse = sep[["between_constants"]]
  )
}

# What the rules are, as one value that cannot be maintained by hand.
#
# It hashes UTF-8 **bytes**, not R objects, and that is not fussiness. Hashing
# the objects made the digest depend on how each string happened to be flagged
# rather than on what it said: the patterns carry accented characters, R marks
# them `unknown` in a UTF-8 locale and `UTF-8` elsewhere, and the two hash
# differently. The same rules on two machines would have reported themselves as
# different rules — the one failure a provenance field must not have.
#
# It covers the patterns and the thresholds — everything that decides what is
# removed. It does **not** cover the code that applies them: a change to
# `.remove_text_intervals()` leaves this untouched, and the package version is
# what records that. Two runs agreeing here agree about the recognition rules,
# which is the question the audit downstream asks.
.doceds_rules_digest <- function(env = asNamespace("redsan")) {
  digest::digest(
    charToRaw(enc2utf8(.doceds_rule_text(.doceds_rule_objects(env)))),
    algo = "sha256",
    serialize = FALSE
  )
}

.doceds_remove_lab_tables <- function(remove_lab_tables) {
  if (
    !is.logical(remove_lab_tables) ||
      length(remove_lab_tables) != 1L ||
      is.na(remove_lab_tables)
  ) {
    stop("`remove_lab_tables` must be TRUE or FALSE.", call. = FALSE)
  }
  remove_lab_tables
}

.doceds_boilerplate_patterns <- function(remove_lab_tables) {
  patterns <- .DOCEDS_BOILERPLATE_PATTERNS
  if (!remove_lab_tables) {
    patterns[["lab_table"]] <- NULL
  }
  patterns
}

#' Which trimming rules ran, and with which limits
#'
#' Reports the identity and the thresholds of the rules [trim_doceds_text()]
#' applies, so a caller can record what produced a trimmed text alongside the
#' text itself.
#'
#' @param remove_lab_tables Whether the optional pasted-laboratory-table family
#'   is active. Defaults to `TRUE`, matching [trim_doceds_text()].
#'
#' @details
#' A trimmed document is not self-describing: two runs a year apart can differ
#' because the families changed, because a bound moved, or because neither did.
#' A saved result that records this alongside its numbers can be told apart from
#' one that cannot, which matters when the numbers are being compared over time.
#'
#' Read it rather than copying the values. A consumer that keeps its own copy of
#' a rule name or a threshold reports what it believes ran, which is the same
#' thing as reporting nothing once the two drift apart.
#'
#' `digest` is the field to compare, because it is the one nobody maintains. It
#' is derived from every pattern and threshold the trimmer holds, so a rule that
#' changes changes it whether or not anyone remembered to say so. It is a
#' SHA-256 digest of the canonical rule text's UTF-8 bytes, computed without R
#' serialization, so two machines running the same rules agree on it whatever
#' their locale. `preamble_rule`
#' and `boilerplate_rule` are names and are deliberately not versioned — a
#' version written into a string is a fact somebody has to remember, and its only
#' possible failure is the one that matters: staying put while the rules move.
#'
#' What `digest` does **not** cover is the code that applies the rules. A change
#' to how spans are merged or removed leaves it untouched; `version` is what
#' records that. Two runs agreeing on both agree about the whole trimming.
#'
#' @return A list: `package` and `version` identify the installed rules;
#'   `digest`, `digest_algorithm`, and `digest_schema` identify the rules
#'   themselves, derived from them rather than declared; `remove_lab_tables`
#'   records whether the optional laboratory-table family was active;
#'   `preamble_rule` and `boilerplate_rule` name the rule sets;
#'   `preamble_limit_chars` is how early the letter date has to appear to be
#'   treated as a frame boundary; `boilerplate_families` names every family in
#'   the order they are applied; `inline_rules` gives the patterns of the two
#'   rules that act inside a line rather than on whole ones, which have patterns
#'   instead of family names; and `near_total_share` with `near_total_min_chars`
#'   are the diagnostic that abandons a trim which matched essentially the whole
#'   document.
#'
#' @examples
#' spec <- doceds_trim_spec()
#' spec$boilerplate_rule
#' spec$boilerplate_families
#'
#' @export
doceds_trim_spec <- function(remove_lab_tables = TRUE) {
  remove_lab_tables <- .doceds_remove_lab_tables(remove_lab_tables)
  list(
    package = "redsan",
    version = as.character(utils::packageVersion("redsan")),
    digest = .doceds_rules_digest(),
    digest_algorithm = "sha256",
    digest_schema = "doceds-rule-text-v1",
    preamble_rule = .DOCEDS_PREAMBLE_RULE,
    preamble_limit_chars = .DOCEDS_PREAMBLE_LIMIT,
    boilerplate_rule = .DOCEDS_BOILERPLATE_RULE,
    boilerplate_families = names(.doceds_boilerplate_patterns(remove_lab_tables)),
    inline_rules = c(
      field = .DOCEDS_FIELD_PATTERN,
      rule_run = .DOCEDS_RULE_RUN_PATTERN
    ),
    near_total_share = .DOCEDS_NEAR_TOTAL_SHARE,
    near_total_min_chars = .DOCEDS_NEAR_TOTAL_MIN_CHARS,
    remove_lab_tables = remove_lab_tables
  )
}
