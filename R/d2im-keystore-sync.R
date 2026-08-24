# D2IM keystore synchronization ---------------------------------------------

.d2im_sync_default_python <- function() {
  env_python <- Sys.getenv("PYKERNEL", unset = "")
  if (nzchar(env_python) && file.exists(env_python)) return(env_python)

  "/opt/kernels/py3.14/bin/python"
}

.d2im_sync_missing_keys <- function(python_keys, r_keys) {
  setdiff(unique(as.character(python_keys)), as.character(r_keys))
}

.d2im_sync_r_keystore_functions <- function() {
  ns <- getNamespace("d2imr")

  load_keystore <- get0(
    "d2im_keystore.load_keystore",
    envir = ns,
    mode = "function",
    inherits = FALSE
  )
  save_keystore <- get0(
    "d2im_keystore.save_keystore",
    envir = ns,
    mode = "function",
    inherits = FALSE
  )

  if (!is.function(load_keystore) || !is.function(save_keystore)) {
    stop(
      "The installed `d2imr` version does not expose the internal keystore ",
      "load/save functions required for synchronization.",
      call. = FALSE
    )
  }

  list(load = load_keystore, save = save_keystore)
}

.d2im_sync_python_keys <- function(py_keystore) {
  reticulate::py_run_string("
from d2im.ksc import keystore as _redsan_d2im_keystore
import ast
import contextlib
import io
import re

_redsan_buffer = io.StringIO()
with contextlib.redirect_stdout(_redsan_buffer):
    _redsan_d2im_keystore.show()

_redsan_output = _redsan_buffer.getvalue()
_redsan_match = re.search(
    r'Keystore contains:\\s*(\\[.*\\])',
    _redsan_output,
    flags=re.S
)

if _redsan_match is None:
    raise RuntimeError('Unable to extract key names from Python keystore.show()')

_redsan_d2im_sync_keys = ast.literal_eval(_redsan_match.group(1))
")

  keys <- unlist(
    reticulate::py_eval("_redsan_d2im_sync_keys", convert = TRUE),
    use.names = FALSE
  )
  unique(as.character(keys))
}

#' Synchronize missing D2IM keystore entries from Python to R
#'
#' Copies entries that exist in the Python D2IM keystore but are absent from the
#' active `d2imr` keystore. Existing R entries are never overwritten. Before any
#' write, the active R keystore is backed up with file mode `0600`.
#'
#' This is an administrative compatibility helper for environments where the
#' Python D2IM keystore is maintained more actively than the R keystore. It is
#' independent of normal EDSAN or ICCA retrieval and is never called
#' automatically by `redsan`.
#'
#' @param python Python interpreter containing the `d2im` package. When `NULL`,
#'   `PYKERNEL` is used if it points to an existing file; otherwise the Podsan
#'   default `/opt/kernels/py3.14/bin/python` is used.
#'
#' @return Invisibly, a character vector containing only the names of keys added
#'   to the R keystore. Secret values are never returned or printed.
#'
#' @details
#' The function requires the optional packages `d2imr` and `reticulate`. It uses
#' the active path returned by `d2imr::get_activ_keystore_path()` and the
#' internal `d2imr` keystore load/save functions because the installed `d2imr`
#' API does not provide an exported whole-keystore synchronization operation.
#'
#' Secret values are transferred in process memory only. `keystore.show()` is
#' captured inside Python solely to recover key names; its output is not emitted
#' to the R console. If `reticulate` has already initialized a different Python
#' interpreter in the current R session, restart the R session before calling
#' this function.
#'
#' @examples
#' \dontrun{
#' sync_d2im_keystore()
#' added <- sync_d2im_keystore()
#' added # names only; no secret values
#' }
#'
#' @export
sync_d2im_keystore <- function(python = NULL) {
  if (!requireNamespace("d2imr", quietly = TRUE)) {
    stop("`sync_d2im_keystore()` requires the optional package `d2imr`.",
         call. = FALSE)
  }
  if (!requireNamespace("reticulate", quietly = TRUE)) {
    stop("`sync_d2im_keystore()` requires the optional package `reticulate`.",
         call. = FALSE)
  }

  if (is.null(python)) python <- .d2im_sync_default_python()
  if (!is.character(python) || length(python) != 1L || is.na(python) ||
      !nzchar(python) || !file.exists(python)) {
    stop("`python` must point to an existing Python interpreter.", call. = FALSE)
  }

  old_reticulate_python <- Sys.getenv("RETICULATE_PYTHON", unset = NA_character_)
  on.exit({
    if (is.na(old_reticulate_python)) {
      Sys.unsetenv("RETICULATE_PYTHON")
    } else {
      Sys.setenv(RETICULATE_PYTHON = old_reticulate_python)
    }
  }, add = TRUE)

  Sys.setenv(RETICULATE_PYTHON = python)
  tryCatch(
    reticulate::use_python(python, required = TRUE),
    error = function(e) {
      stop(
        "Could not select the D2IM Python interpreter. If reticulate already ",
        "initialized another Python in this R session, restart R and retry. ",
        "Underlying error: ", conditionMessage(e),
        call. = FALSE
      )
    }
  )

  py_keystore <- tryCatch(
    reticulate::import("d2im.ksc", convert = TRUE)$keystore,
    error = function(e) {
      stop(
        "Could not import the Python D2IM keystore from `", python, "`: ",
        conditionMessage(e),
        call. = FALSE
      )
    }
  )

  if (!("get" %in% reticulate::py_list_attributes(py_keystore))) {
    stop("Python D2IM keystore does not expose `get()`.", call. = FALSE)
  }

  python_keys <- .d2im_sync_python_keys(py_keystore)

  r_path <- d2imr::get_activ_keystore_path()
  if (!is.character(r_path) || length(r_path) != 1L || is.na(r_path) ||
      !nzchar(r_path) || !file.exists(r_path)) {
    stop("The active d2imr keystore path is missing or invalid.", call. = FALSE)
  }

  r_fns <- .d2im_sync_r_keystore_functions()
  r_store <- r_fns$load(r_path)
  if (is.null(r_store)) {
    stop("Unable to load the active R keystore: ", r_path, call. = FALSE)
  }

  missing_keys <- .d2im_sync_missing_keys(python_keys, names(r_store))
  if (!length(missing_keys)) {
    message("D2IM R keystore is already synchronized.")
    return(invisible(character()))
  }

  backup_path <- paste0(
    r_path,
    ".pre-python-sync-",
    format(Sys.time(), "%Y%m%d-%H%M%S")
  )
  backup_ok <- file.copy(r_path, backup_path, overwrite = FALSE)
  if (!backup_ok) {
    stop("Unable to create R keystore backup.", call. = FALSE)
  }
  Sys.chmod(backup_path, mode = "0600")

  missing_values <- setNames(
    lapply(missing_keys, function(key) {
      value <- py_keystore$get(key)
      if (is.null(value)) {
        stop("Python keystore returned NULL for key: ", key, call. = FALSE)
      }
      value
    }),
    missing_keys
  )

  for (key in missing_keys) {
    r_store[[key]] <- missing_values[[key]]
  }
  rm(missing_values)

  tryCatch(
    r_fns$save(r_store, r_path),
    error = function(e) {
      stop(
        "Unable to save the synchronized R keystore. The pre-write backup is ",
        "available at `", backup_path, "`. Underlying error: ",
        conditionMessage(e),
        call. = FALSE
      )
    }
  )
  rm(r_store)

  r_store_after <- r_fns$load(r_path)
  if (is.null(r_store_after)) {
    stop(
      "The R keystore was saved but could not be reloaded for verification. ",
      "Backup: ", backup_path,
      call. = FALSE
    )
  }

  still_missing <- setdiff(python_keys, names(r_store_after))
  rm(r_store_after)
  if (length(still_missing)) {
    stop(
      "Synchronization incomplete. Still missing: ",
      paste(still_missing, collapse = ", "),
      ". Backup: ", backup_path,
      call. = FALSE
    )
  }

  message(
    "D2IM keystore synchronized: ", length(missing_keys), " key(s) added."
  )
  message("Existing R values were preserved.")
  message("Backup: ", backup_path)

  invisible(missing_keys)
}
