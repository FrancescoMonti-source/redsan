# Legacy ICCA retrieval alias ------------------------------------------------

#' Deprecated ICCA retrieval name
#'
#' @inheritParams icca_get
#' @return The value returned by [icca_get()].
#' @export
get_icca <- function(evtids, source = "encounter", link = "auto",
                     connection = NULL, instance = c("adult", "ped"),
                     env = "edsan-ct", ks_path = NULL) {
  .redsan_deprecate("get_icca", "icca_get")
  icca_get(
    evtids = evtids,
    source = source,
    link = link,
    connection = connection,
    instance = instance,
    env = env,
    ks_path = ks_path
  )
}
