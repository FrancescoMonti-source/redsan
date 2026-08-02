args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1L) {
  stop(
    "Usage: Rscript tools/run_doceds_repetition_exploration.R ",
    "<docs-rds> [label] [sample-evtids]"
  )
}

source("tools/explore_doceds_repetition.R")

path <- args[[1L]]
label <- if (length(args) >= 2L) args[[2L]] else basename(path)
sample_evtids <- if (length(args) >= 3L) as.integer(args[[3L]]) else 5000L
documents <- readRDS(path)
started <- Sys.time()
result <- explore_repeated_doceds(
  documents,
  sample_evtids = sample_evtids,
  progress = interactive()
)
clustered <- cluster_repeated_spans(
  result$spans,
  corpus_chars = result$summary$corpus_chars
)
elapsed <- as.numeric(difftime(Sys.time(), started, units = "secs"))

excluded <- if ("RECTYPE" %in% names(documents)) {
  table(
    factor(
      as.character(documents$RECTYPE),
      levels = c("OPROOM", "BT")
    )
  )
} else {
  c(OPROOM = 0L, BT = 0L)
}
exact <- result$span_stats[n_evtids >= 2L]
patterns <- c(
  "informatique",
  "confidential",
  "secret médical",
  "droit d’accès",
  "cnil",
  "rgpd"
)
admin_flags <- if (nrow(result$spans)) {
  rowSums(vapply(
    patterns,
    function(pattern) {
      grepl(pattern, result$spans$normalized_text, fixed = TRUE)
    },
    logical(nrow(result$spans))
  )) > 0L
} else {
  logical()
}
admin <- result$spans[admin_flags]

cat(sprintf(
  paste0(
    "%s input_rows=%d excluded_OPROOM=%d excluded_BT=%d ",
    "sampled_docs=%d sampled_evtids=%d corpus_chars=%.0f postings=%d ",
    "frequent_shingles=%d candidate_spans=%d elapsed_s=%.1f\n"
  ),
  label,
  nrow(documents),
  excluded[["OPROOM"]],
  excluded[["BT"]],
  result$summary$documents,
  result$summary$evtids,
  result$summary$corpus_chars,
  result$summary$postings,
  result$summary$frequent_shingles,
  result$summary$candidate_spans,
  elapsed
))
cat(sprintf(
  paste0(
    "%s end80_spans=%d exact_repeated_sequences=%d ",
    "exact_sequence_evtids_max=%d administrative_lexicon_spans=%d ",
    "administrative_lexicon_evtids=%d\n"
  ),
  label,
  sum(result$spans$relative_start >= 0.80),
  nrow(exact),
  if (nrow(exact)) max(exact$n_evtids) else 0L,
  nrow(admin),
  data.table::uniqueN(admin$EVTID)
))
if (nrow(exact)) {
  cat(sprintf(
    paste0(
      "%s exact_words_median=%.0f exact_words_p95=%.0f ",
      "exact_start_median=%.3f\n"
    ),
    label,
    stats::median(exact$span_words),
    unname(stats::quantile(exact$span_words, 0.95)),
    stats::median(exact$median_relative_start)
  ))
}
if (nrow(clustered$clusters)) {
  cat(sprintf(
    "%s clusters=%d clinically_risky=%d addressable_share=%.3f\n",
    label,
    nrow(clustered$clusters),
    sum(clustered$clusters$contains_clinical_lead),
    sum(clustered$clusters[!(contains_clinical_lead)]$corpus_share)
  ))
  print(clustered$clusters[
    !(contains_clinical_lead),
    .(cluster_id, n_evtids, n_occurrences, window,
      yield_chars, corpus_share, median_span_words)
  ][seq_len(min(40L, .N))])
}

invisible(list(repetition = result, clustering = clustered))
