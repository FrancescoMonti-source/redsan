# Audit-only exploration of repeated text across DOCEDS events.
#
# This file is deliberately outside R/: it is an exploratory workflow, not part
# of the redsan package API. It never modifies the input documents.

normalize_doceds_text <- function(x) {
  x <- enc2utf8(as.character(x))
  x <- stringi::stri_trans_nfkc_casefold(x)
  # De-identification placeholders are folded to one token. Without this the
  # same signature block coming from two doctors produces two fingerprints, its
  # support splits across variants, and a very frequent passage is reported as
  # a rare one.
  x <- stringi::stri_replace_all_regex(
    x,
    "\\[[a-z0-9_]{2,20}\\]",
    "<ph>",
    vectorize_all = FALSE
  )
  x <- stringi::stri_replace_all_regex(
    x,
    paste0(
      "\\b(?:0?[1-9]|[12][0-9]|3[01])[./-](?:0?[1-9]|1[0-2])",
      "(?:[./-](?:19|20)[0-9]{2})?"
    ),
    "<date>",
    vectorize_all = FALSE
  )
  x <- stringi::stri_replace_all_regex(
    x,
    "\\bpage\\s+[0-9]+\\s+(?:sur|/)\\s*[0-9]+\\b",
    "page <n> sur <n>",
    vectorize_all = FALSE
  )
  stringi::stri_trim_both(stringi::stri_replace_all_regex(
    x,
    "[\\p{Z}\\s]+",
    " ",
    vectorize_all = FALSE
  ))
}

# Both ends of the document are fingerprinted. Inspecting only the tail is the
# reason the letter header and the mail-merge identity line were never
# reported: they are not rare, they are at the top.
.doceds_window_shingles <- function(
  text,
  shingle_words,
  head_fraction,
  tail_fraction,
  max_head_words,
  max_tail_words
) {
  normalized <- normalize_doceds_text(text)
  if (is.na(normalized) || !nzchar(normalized)) {
    return(NULL)
  }

  words <- stringi::stri_split_regex(normalized, "\\s+")[[1L]]
  n_words <- length(words)
  if (n_words < shingle_words) {
    return(NULL)
  }
  last_start <- n_words - shingle_words + 1L

  head_end <- min(
    last_start,
    max_head_words,
    ceiling(n_words * head_fraction)
  )
  tail_start <- max(
    1L,
    n_words - max_tail_words + 1L,
    floor(n_words * (1 - tail_fraction)) + 1L
  )
  starts <- sort(unique(c(
    if (head_end >= 1L) seq_len(head_end) else integer(),
    if (tail_start <= last_start) seq.int(tail_start, last_start) else integer()
  )))
  if (!length(starts)) {
    return(NULL)
  }

  pieces <- lapply(seq_len(shingle_words) - 1L, function(offset) {
    words[starts + offset]
  })
  shingles <- do.call(paste, c(pieces, sep = "\u001f"))

  list(
    words = words,
    chars = nchar(normalized),
    postings = data.table::data.table(
      shingle = shingles,
      window = ifelse(starts <= head_end, "head", "tail"),
      word_start = starts,
      relative_start = starts / n_words
    )
  )
}

#' Explore repeated passages across distinct DOCEDS events
#'
#' The function represents both ends of each document as an ordered sequence of
#' word shingles. It counts each shingle at most once per EVTID, then merges
#' adjacent frequent shingles back into candidate passages. OPROOM and BT rows
#' are excluded before sampling and analysis.
#'
#' With `pre_trim = TRUE` each document is first passed through the rules the
#' package applies today, so every reported passage is by construction one that
#' still reaches the model. Coverage stops being a list of anchors this file has
#' to keep in step with `R/find_cim10_evidence.R` and becomes a property of the
#' input.
#'
#' @param documents A DOCEDS data frame with EVTID, ELTID, RECTXT, and
#'   optionally RECTYPE.
#' @param sample_evtids Maximum number of distinct EVTIDs to inspect. Use NULL
#'   for the full input.
#' @param seed Sampling seed.
#' @param pre_trim Whether to apply the package preamble, boilerplate and
#'   placeholder rules before fingerprinting.
#' @param shingle_words Number of consecutive words in one fingerprint.
#' @param min_evtids Minimum distinct-EVTID support for a fingerprint.
#' @param head_fraction Fraction of the start of each document to inspect.
#' @param tail_fraction Fraction of the end of each document to inspect.
#' @param max_head_words Maximum number of leading words to inspect.
#' @param max_tail_words Maximum number of trailing words to inspect.
#' @param max_gap Number of missing adjacent fingerprints tolerated inside one
#'   reconstructed passage.
#' @param min_span_words Minimum reconstructed passage length.
#' @param progress Whether to display document fingerprinting progress.
#'
#' @return An audit list. `shingle_stats` contains aggregate fingerprints.
#'   `spans` contains local provenance and normalized candidate text; do not
#'   commit or publish that object when it comes from clinical documents.
#'   `span_stats` is ordered by `yield_chars`, the characters a rule matching
#'   that passage would remove from the corpus.
explore_repeated_doceds <- function(
  documents,
  sample_evtids = 5000L,
  seed = 20260729L,
  pre_trim = TRUE,
  shingle_words = 8L,
  min_evtids = 10L,
  head_fraction = 0.15,
  tail_fraction = 0.30,
  max_head_words = 400L,
  max_tail_words = 1000L,
  max_gap = 1L,
  min_span_words = 12L,
  progress = interactive()
) {
  required <- c("EVTID", "ELTID", "RECTXT")
  missing <- setdiff(required, names(documents))
  if (length(missing)) {
    stop("Missing DOCEDS columns: ", paste(missing, collapse = ", "))
  }
  # Checked before the loop rather than on the first document, so a missing
  # package does not surface halfway through a long run.
  if (pre_trim && !requireNamespace("redsan", quietly = TRUE)) {
    stop("`pre_trim = TRUE` needs redsan loaded or installed.")
  }

  docs <- data.table::as.data.table(documents)[, intersect(
    c("EVTID", "ELTID", "RECTYPE", "RECTXT"),
    names(documents)
  ), with = FALSE]
  docs[, `:=`(
    EVTID = as.character(EVTID),
    ELTID = as.character(ELTID),
    RECTXT = as.character(RECTXT)
  )]
  if ("RECTYPE" %in% names(docs)) {
    docs[, RECTYPE := as.character(RECTYPE)]
    docs <- docs[is.na(RECTYPE) | !RECTYPE %in% c("OPROOM", "BT")]
  }
  docs <- docs[
    !is.na(EVTID) & nzchar(EVTID) &
      !is.na(ELTID) & nzchar(ELTID) &
      !is.na(RECTXT) & nzchar(trimws(RECTXT))
  ]
  docs[, doc_row := .I]

  available_evtids <- unique(docs$EVTID)
  if (!is.null(sample_evtids) && length(available_evtids) > sample_evtids) {
    set.seed(seed)
    selected <- sample(available_evtids, sample_evtids)
    docs <- docs[EVTID %in% selected]
    data.table::setorder(docs, doc_row)
  }

  postings <- vector("list", nrow(docs))
  token_cache <- vector("list", nrow(docs))
  bar <- if (progress) {
    utils::txtProgressBar(min = 0L, max = nrow(docs), style = 3L)
  } else {
    NULL
  }
  if (!is.null(bar)) {
    on.exit(close(bar), add = TRUE)
  }

  corpus_chars <- 0
  for (i in seq_len(nrow(docs))) {
    text <- docs$RECTXT[[i]]
    if (pre_trim) {
      text <- redsan::trim_doceds_text(text)$text
    }
    fingerprinted <- .doceds_window_shingles(
      text,
      shingle_words = shingle_words,
      head_fraction = head_fraction,
      tail_fraction = tail_fraction,
      max_head_words = max_head_words,
      max_tail_words = max_tail_words
    )
    if (!is.null(fingerprinted)) {
      token_cache[[i]] <- fingerprinted$words
      corpus_chars <- corpus_chars + fingerprinted$chars
      postings[[i]] <- fingerprinted$postings[, `:=`(
        local_doc_row = i,
        EVTID = docs$EVTID[[i]],
        ELTID = docs$ELTID[[i]]
      )]
    }
    if (!is.null(bar) && (i %% 100L == 0L || i == nrow(docs))) {
      utils::setTxtProgressBar(bar, i)
    }
  }

  postings <- data.table::rbindlist(postings, use.names = TRUE)
  if (!nrow(postings)) {
    return(list(
      summary = list(
        documents = nrow(docs),
        evtids = data.table::uniqueN(docs$EVTID),
        postings = 0L,
        corpus_chars = corpus_chars,
        frequent_shingles = 0L,
        candidate_spans = 0L
      ),
      shingle_stats = data.table::data.table(),
      spans = data.table::data.table(),
      span_stats = data.table::data.table()
    ))
  }

  support <- unique(postings[, .(shingle, EVTID)])[
    ,
    .(n_evtids = .N),
    by = shingle
  ][n_evtids >= min_evtids]
  hits <- support[postings, on = "shingle", nomatch = 0L]
  data.table::setorder(hits, local_doc_row, word_start)

  shingle_stats <- hits[, .(
    n_evtids = data.table::uniqueN(EVTID),
    n_documents = data.table::uniqueN(local_doc_row),
    head_share = mean(window == "head"),
    median_relative_start = stats::median(relative_start),
    end_80_share = mean(relative_start >= 0.80)
  ), by = shingle][order(-n_evtids, -end_80_share)]

  # Runs break on word position rather than on a running index, so a passage
  # found at the top of a document is never spliced onto one found at the
  # bottom across the gap between the two windows.
  hits[, run_id := cumsum(
    c(TRUE, diff(word_start) > max_gap + 1L)
  ), by = local_doc_row]
  spans <- hits[, .(
    EVTID = EVTID[[1L]],
    ELTID = ELTID[[1L]],
    window = window[[1L]],
    word_start = min(word_start),
    word_end = max(word_start) + shingle_words - 1L,
    n_shingles = .N,
    min_shingle_evtids = min(n_evtids),
    median_shingle_evtids = as.numeric(stats::median(n_evtids)),
    relative_start = min(relative_start)
  ), by = .(local_doc_row, run_id)]
  spans[, span_words := word_end - word_start + 1L]
  spans <- spans[span_words >= min_span_words]
  span_stats <- data.table::data.table()
  if (nrow(spans)) {
    spans[, normalized_text := mapply(
      function(row, start, end) {
        paste(token_cache[[row]][seq.int(start, end)], collapse = " ")
      },
      local_doc_row,
      word_start,
      word_end,
      USE.NAMES = FALSE
    )]
    spans[, span_chars := nchar(normalized_text)]
    data.table::setorder(
      spans,
      -min_shingle_evtids,
      -span_words,
      -relative_start
    )
    # Ranking by support alone says which passage is most common, never which
    # is worth writing a rule for. `yield_chars` is what a rule matching this
    # passage would remove from the corpus, and `corpus_share` says when to
    # stop looking.
    span_stats <- spans[, .(
      n_evtids = data.table::uniqueN(EVTID),
      n_documents = .N,
      window = .majority_window(window),
      span_words = as.numeric(stats::median(span_words)),
      span_chars = as.integer(stats::median(span_chars)),
      yield_chars = as.numeric(sum(span_chars)),
      median_relative_start = stats::median(relative_start)
    ), by = normalized_text][order(-yield_chars)]
    span_stats[, corpus_share := yield_chars / corpus_chars]
  }

  list(
    summary = list(
      documents = nrow(docs),
      evtids = data.table::uniqueN(docs$EVTID),
      postings = nrow(postings),
      corpus_chars = corpus_chars,
      frequent_shingles = nrow(shingle_stats),
      candidate_spans = nrow(spans),
      excluded_rectypes = c("OPROOM", "BT"),
      parameters = list(
        pre_trim = pre_trim,
        shingle_words = shingle_words,
        min_evtids = min_evtids,
        head_fraction = head_fraction,
        tail_fraction = tail_fraction,
        max_head_words = max_head_words,
        max_tail_words = max_tail_words,
        max_gap = max_gap,
        min_span_words = min_span_words
      )
    ),
    shingle_stats = shingle_stats,
    spans = spans,
    span_stats = span_stats
  )
}

# Where a passage mostly sits. Reading the first occurrence's window labelled
# a header block as a tail one whenever a single document carried it late.
.majority_window <- function(window) {
  names(sort(table(window), decreasing = TRUE))[[1L]]
}

.span_shingle_set <- function(text, shingle_words) {
  words <- stringi::stri_split_regex(text, "\\s+")[[1L]]
  if (length(words) < shingle_words) {
    return(character())
  }
  starts <- seq_len(length(words) - shingle_words + 1L)
  pieces <- lapply(seq_len(shingle_words) - 1L, function(offset) {
    words[starts + offset]
  })
  unique(do.call(paste, c(pieces, sep = "\u001f")))
}

# A passage that reads like care rather than administration. This is a brake on
# the ranking, not a classifier: a cluster it flags is not a candidate until a
# human has read it, however large its yield.
.CLINICAL_LEAD_ANCHORS <- c(
  "sonde vesicale", "sonde vésicale",
  "il a ete realise", "il a été réalisé",
  "le patient presente", "le patient présente",
  "la patiente presente", "la patiente présente",
  "traitement par", "posologie", "mg/j",
  "examen clinique", "a l'entree", "à l'entrée",
  "conclusion", "diagnostic"
)

.has_clinical_lead <- function(text) {
  Reduce(`|`, lapply(.CLINICAL_LEAD_ANCHORS, function(anchor) {
    grepl(anchor, text, fixed = TRUE)
  }))
}

#' Cluster repeated passage variants by local fingerprint overlap
#'
#' Candidate passages are represented as sets of word shingles. Sparse matrix
#' multiplication finds only pairs sharing at least one fingerprint, avoiding a
#' full document-pair comparison. Two variants are joined when their Jaccard
#' similarity or their smaller-to-larger containment exceeds the configured
#' threshold. Connected variants form one auditable family.
#'
#' @param spans The `spans` table returned by `explore_repeated_doceds()`.
#' @param corpus_chars Total normalized characters inspected, from
#'   `explore_repeated_doceds()$summary$corpus_chars`. Turns a cluster's yield
#'   into a share of the corpus.
#' @param shingle_words Words per clustering fingerprint.
#' @param min_shared Minimum shared fingerprints required for an edge.
#' @param min_jaccard Minimum intersection-over-union similarity.
#' @param min_containment Minimum intersection-over-smaller-set containment.
#'
#' @return A list containing occurrence-level `assignments`, aggregate
#'   structural `clusters` ordered by yield with the clinically risky ones last,
#'   and accepted similarity `edges`. Representative text is patient-derived and
#'   must remain local.
cluster_repeated_spans <- function(
  spans,
  corpus_chars = NA_real_,
  shingle_words = 5L,
  min_shared = 4L,
  min_jaccard = 0.55,
  min_containment = 0.75
) {
  if (!is.data.frame(spans) || !"normalized_text" %in% names(spans)) {
    stop("`spans` must contain `normalized_text`.")
  }
  occurrences <- data.table::as.data.table(spans)
  variants <- unique(as.character(occurrences$normalized_text))
  variants <- variants[!is.na(variants) & nzchar(variants)]
  if (!length(variants)) {
    return(list(
      assignments = data.table::data.table(),
      clusters = data.table::data.table(),
      edges = data.table::data.table()
    ))
  }

  sets <- lapply(variants, .span_shingle_set, shingle_words = shingle_words)
  vocabulary <- unique(unlist(sets, use.names = FALSE))
  row_index <- rep.int(seq_along(sets), lengths(sets))
  column_index <- match(unlist(sets, use.names = FALSE), vocabulary)
  incidence <- Matrix::sparseMatrix(
    i = row_index,
    j = column_index,
    x = 1,
    dims = c(length(variants), length(vocabulary))
  )
  intersections <- methods::as(
    Matrix::tcrossprod(incidence),
    "TsparseMatrix"
  )
  sizes <- Matrix::rowSums(incidence)
  edges <- data.table::data.table(
    i = intersections@i + 1L,
    j = intersections@j + 1L,
    x = intersections@x
  )[i < j]
  if (nrow(edges)) {
    edges[, `:=`(
      jaccard = x / (sizes[i] + sizes[j] - x),
      containment = x / pmin(sizes[i], sizes[j])
    )]
    edges <- edges[
      x >= min_shared &
        (jaccard >= min_jaccard | containment >= min_containment)
    ]
  }

  parent <- seq_along(variants)
  rank <- integer(length(variants))
  find_root <- function(node) {
    while (parent[[node]] != node) {
      node <- parent[[node]]
    }
    node
  }
  if (nrow(edges)) {
    for (edge in seq_len(nrow(edges))) {
      left <- find_root(edges$i[[edge]])
      right <- find_root(edges$j[[edge]])
      if (left == right) {
        next
      }
      if (rank[[left]] < rank[[right]]) {
        parent[[left]] <- right
      } else {
        parent[[right]] <- left
        if (rank[[left]] == rank[[right]]) {
          rank[[left]] <- rank[[left]] + 1L
        }
      }
    }
  }
  roots <- vapply(seq_along(variants), find_root, integer(1))
  cluster_id <- match(roots, unique(roots))
  variant_map <- data.table::data.table(
    normalized_text = variants,
    variant_id = seq_along(variants),
    cluster_id = cluster_id
  )
  assignments <- variant_map[
    occurrences,
    on = "normalized_text",
    nomatch = 0L
  ]

  clusters <- assignments[, {
    representative <- names(sort(
      table(normalized_text),
      decreasing = TRUE
    ))[[1L]]
    list(
      n_variants = data.table::uniqueN(variant_id),
      n_occurrences = .N,
      n_evtids = data.table::uniqueN(EVTID),
      window = .majority_window(window),
      yield_chars = as.numeric(sum(span_chars)),
      median_span_words = as.numeric(stats::median(span_words)),
      median_relative_start = as.numeric(stats::median(relative_start)),
      contains_clinical_lead = any(.has_clinical_lead(normalized_text)),
      representative_text = representative
    )
  }, by = cluster_id]
  # Everything that survives pre-trimming is by definition uncovered, so there
  # is nothing left to label. What remains to decide is what a rule would be
  # worth, and whether it is safe.
  clusters[, corpus_share := yield_chars / corpus_chars]
  data.table::setorder(clusters, contains_clinical_lead, -yield_chars)

  list(
    assignments = assignments,
    clusters = clusters,
    edges = edges
  )
}
