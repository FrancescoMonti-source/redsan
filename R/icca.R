# ICCA SQL Server access ------------------------------------------------------

.icca_sql_for_validation <- function(sql) {
  sql <- gsub("(?s)/\\*.*?\\*/", " ", sql, perl = TRUE)
  sql <- gsub("--[^\\r\\n]*", " ", sql, perl = TRUE)
  # Remove single-quoted literals before looking for write verbs. Doubled single
  # quotes are SQL Server's escaped quote syntax.
  gsub("'(?:''|[^'])*'", "''", sql, perl = TRUE)
}

.icca_validate_read_query <- function(sql) {
  if (!is.character(sql) || length(sql) != 1L || is.na(sql) || !nzchar(trimws(sql))) {
    stop("`sql` must be one non-empty SQL string.", call. = FALSE)
  }

  sql <- trimws(sql)
  sql_without_trailing_semicolon <- sub(";\\s*$", "", sql, perl = TRUE)
  check <- trimws(.icca_sql_for_validation(sql_without_trailing_semicolon))
  if (grepl(";", check, fixed = TRUE)) {
    stop("`query_icca()` accepts exactly one SQL statement.", call. = FALSE)
  }

  if (!grepl("(?is)^(select\\b|with\\b)", check, perl = TRUE)) {
    stop("`query_icca()` accepts read-only SELECT queries (including CTEs) only.",
         call. = FALSE)
  }

  write_pattern <- paste0(
    "(?i)\\b(",
    paste(
      c("insert", "update", "delete", "drop", "alter", "create", "truncate",
        "merge", "exec", "execute", "grant", "revoke", "deny", "into"),
      collapse = "|"
    ),
    ")\\b"
  )
  if (grepl(write_pattern, check, perl = TRUE)) {
    stop("`query_icca()` rejects SQL that can modify database state.", call. = FALSE)
  }

  sql_without_trailing_semicolon
}

.icca_normalize_params <- function(params) {
  if (is.null(params)) return(NULL)
  if (!(is.atomic(params) || is.list(params))) {
    stop("`params` must be NULL, an atomic vector, or a list.", call. = FALSE)
  }
  if (length(params) == 0L) return(NULL)
  unname(as.list(params))
}

.icca_python_module <- local({
  module <- NULL

  function() {
    if (!is.null(module)) return(module)

    if (!requireNamespace("reticulate", quietly = TRUE)) {
      stop(
        "ICCA access requires the optional package `reticulate` and the CHU ",
        "Python environment containing `d2im`/`pymssql`.",
        call. = FALSE
      )
    }

    python_dir <- system.file("python", package = "redsan")
    if (!nzchar(python_dir)) {
      stop("The packaged ICCA Python backend could not be located.", call. = FALSE)
    }

    module <<- tryCatch(
      reticulate::import_from_path(
        "redsan_icca_backend",
        path = python_dir,
        convert = TRUE
      ),
      error = function(e) {
        stop(
          "Could not load the ICCA Python backend. Ensure reticulate is using ",
          "the CHU Python environment where `d2im` and `pymssql` are available. ",
          "Underlying error: ", conditionMessage(e),
          call. = FALSE
        )
      }
    )
    module
  }
})

.icca_python_execute <- function(sql, params = NULL, d2im_keystore_path = NULL) {
  module <- .icca_python_module()

  result <- tryCatch(
    module$execute_sql(
      sql = sql,
      params = params,
      keystore_path = d2im_keystore_path
    ),
    error = function(e) {
      stop("ICCA query failed: ", conditionMessage(e), call. = FALSE)
    }
  )

  if (is.null(result)) {
    stop(
      "ICCA query returned no result object. Check the d2im Python keystore ",
      "and ICCAJ database configuration.",
      call. = FALSE
    )
  }

  result
}

.icca_query <- function(sql, params = NULL, d2im_keystore_path = NULL,
                        backend = .icca_python_execute) {
  sql <- .icca_validate_read_query(sql)
  params <- .icca_normalize_params(params)

  if (!is.null(d2im_keystore_path) &&
      (!is.character(d2im_keystore_path) ||
       length(d2im_keystore_path) != 1L ||
       is.na(d2im_keystore_path) ||
       !nzchar(d2im_keystore_path))) {
    stop("`d2im_keystore_path` must be NULL or one non-empty path.", call. = FALSE)
  }

  out <- backend(
    sql = sql,
    params = params,
    d2im_keystore_path = d2im_keystore_path
  )

  if (!is.data.frame(out)) {
    stop("The ICCA backend must return a data frame.", call. = FALSE)
  }

  tibble::as_tibble(out)
}

#' Execute a read-only query against ICCA
#'
#' Executes one parameterized read-only SQL Server query against the CHU ICCAJ
#' database through the Python `d2im` client. The function is intentionally
#' low-level: it exposes source access without adding clinical concept
#' definitions or pseudonymization logic.
#'
#' @param sql One SQL Server `SELECT` statement. Common table expressions
#'   (`WITH ... SELECT ...`) are accepted. Multiple statements and write
#'   operations are rejected.
#' @param params Optional positional values for `%s` placeholders understood by
#'   the underlying `pymssql` driver.
#' @param d2im_keystore_path Optional path to a **Python d2im** keystore. This is
#'   not the R `d2imr` keystore format used by [edsan_pseudonymize()] and
#'   [edsan_reidentify()]. When `NULL`, d2impy uses its configured default.
#'
#' @return A tibble containing the SQL Server result.
#'
#' @details
#' ICCA access requires the optional R package `reticulate` and a Python
#' environment where the CHU `d2im` package and `pymssql` are importable.
#' `redsan` calls `d2im.dbc.edsan_dbc.sqlserver_execute_query()` with
#' `Database.ICCAJ` and requests a pandas result for conversion to R.
#'
#' The read-only validation is an accidental-write guard, not a SQL security
#' sandbox. Database permissions remain the authoritative access control.
#'
#' @examples
#' \dontrun{
#' # Connectivity/schema smoke test: returns zero rows and no patient data.
#' query_icca(
#'   "SELECT TOP 0 encounterid FROM CISReportingDB.dbo.D_Encounter"
#' )
#'
#' query_icca(
#'   paste(
#'     "SELECT TOP 10 encounterid, encounternumber",
#'     "FROM CISReportingDB.dbo.D_Encounter",
#'     "WHERE encounternumber = %s"
#'   ),
#'   params = "123456789"
#' )
#' }
#'
#' @export
query_icca <- function(sql, params = NULL, d2im_keystore_path = NULL) {
  .icca_query(
    sql = sql,
    params = params,
    d2im_keystore_path = d2im_keystore_path
  )
}
