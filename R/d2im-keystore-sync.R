# D2IM keystore synchronization ---------------------------------------------

#' Synchronize the d2imr keystore from Python
#'
#' Compatibility wrapper for the former redsan-owned implementation. The
#' synchronization implementation and active-keystore state belong to d2imr.
#'
#' @param python Optional Python interpreter path passed to d2imr.
#' @return Invisibly, the names of values added by d2imr.
#' @export
sync_d2im_keystore <- function(python = NULL) {
  if (!requireNamespace("d2imr", quietly = TRUE)) {
    stop("`sync_d2im_keystore()` requires the package `d2imr`.", call. = FALSE)
  }

  warning(
    "`sync_d2im_keystore()` is deprecated; use `d2imr::sync_keystore_from_python()`.",
    call. = FALSE
  )
  sync <- .d2imr_sync_fn()
  sync(python = python)
}

.d2imr_sync_fn <- function() {
  getExportedValue("d2imr", "sync_keystore_from_python")
}
