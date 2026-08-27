# Generic public ICCA retrieval ----------------------------------------------
#
# This definition intentionally supersedes the earlier narrow source dispatcher
# in R/icca.R while preserving its tested encounter path and convenience aliases.

get_icca <- function(evtids, source = "encounter", link = "auto",
                     connection = NULL, env = "edsan-ct", ks_path = NULL) {
  if (!is.character(source) || length(source) != 1L || is.na(source) ||
      !nzchar(trimws(source))) {
    stop("`source` must be one non-empty ICCA source.", call. = FALSE)
  }

  if (identical(trimws(source), "encounter")) {
    return(.icca_get_encounter(
      evtids,
      connection = connection,
      env = env,
      ks_path = ks_path
    ))
  }

  .icca_get_source(
    evtids,
    source = source,
    link = link,
    connection = connection,
    env = env,
    ks_path = ks_path
  )
}
