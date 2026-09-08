# CORA Oracle access ---------------------------------------------------------

.cora_sql_for_validation <- function(sql) {
  sql <- gsub("(?s)/\\*.*?\\*/", " ", sql, perl = TRUE)
  sql <- gsub("--[^\\r\\n]*", " ", sql, perl = TRUE)
  gsub("'(?:''|[^'])*'", "''", sql, perl = TRUE)
}

.cora_validate_read_query <- function(sql) {
  if (!is.character(sql) || length(sql) != 1L || is.na(sql) ||
      !nzchar(trimws(sql))) {
    stop("`sql` must be one non-empty SQL string.", call. = FALSE)
  }

  sql <- trimws(sql)
  sql_without_trailing_semicolon <- sub(";\\s*$", "", sql, perl = TRUE)
  check <- trimws(.cora_sql_for_validation(sql_without_trailing_semicolon))

  if (grepl(";", check, fixed = TRUE)) {
    stop("`query_cora()` accepts exactly one SQL statement.", call. = FALSE)
  }

  if (!grepl("(?is)^(select\\b|with\\b)", check, perl = TRUE)) {
    stop(
      "`query_cora()` accepts read-only SELECT queries (including CTEs) only.",
      call. = FALSE
    )
  }

  write_pattern <- paste0(
    "(?i)\\b(",
    paste(
      c(
        "insert", "update", "delete", "drop", "alter", "create",
        "truncate", "merge", "begin", "declare", "call", "exec", "execute",
        "grant", "revoke", "deny", "commit", "rollback", "savepoint", "lock",
        "into"
      ),
      collapse = "|"
    ),
    ")\\b"
  )
  if (grepl(write_pattern, check, perl = TRUE) ||
      grepl("(?i)\\bfor\\s+update\\b", check, perl = TRUE)) {
    stop("`query_cora()` rejects SQL that can modify or lock database state.",
         call. = FALSE)
  }

  sql_without_trailing_semicolon
}

.cora_execute <- function(connection, sql) {
  .cora_require_namespace("DBI", "query execution")
  DBI::dbGetQuery(connection, sql)
}

.cora_disconnect <- function(connection) {
  .cora_require_namespace("DBI", "connection cleanup")
  DBI::dbDisconnect(connection)
}

.cora_query <- function(sql, connection = NULL, ojdbc_jar = NULL,
                        connect = .cora_connect,
                        execute = .cora_execute,
                        disconnect = .cora_disconnect) {
  sql <- .cora_validate_read_query(sql)

  owns_connection <- is.null(connection)
  if (owns_connection) {
    connection <- connect(ojdbc_jar = ojdbc_jar)
    if (is.null(connection)) {
      stop("The CORA connection factory returned `NULL`.", call. = FALSE)
    }
    on.exit(disconnect(connection), add = TRUE)
  }

  out <- tryCatch(
    execute(connection, sql),
    error = function(e) {
      stop("CORA query failed: ", conditionMessage(e), call. = FALSE)
    }
  )

  if (!is.data.frame(out)) {
    stop("The CORA query backend must return a data frame.", call. = FALSE)
  }

  tibble::as_tibble(out)
}

#' Execute a read-only query against CORA
#'
#' Executes one read-only Oracle query against CORA using the same JDBC
#' connection settings already used by [get_cora_diet()]. The connection is
#' configured through the active `d2imr` keystore (`db.cora.url`,
#' `db.cora.usr`, and `db.cora.pwd`).
#'
#' @param sql One Oracle `SELECT` statement. Common table expressions
#'   (`WITH ... SELECT ...`) are accepted. Multiple statements, write operations,
#'   and `SELECT ... FOR UPDATE` are rejected.
#' @param connection Optional existing CORA DBI connection. When `NULL`,
#'   `redsan` opens a transient JDBC connection and closes it before returning.
#'   A caller-supplied connection is never closed by `query_cora()`.
#' @param ojdbc_jar Optional path to an Oracle JDBC driver. When omitted,
#'   `redsan` resolves the driver automatically from `REDSAN_OJDBC_JAR`, the
#'   standard CORA workstation installation, or known Podsan Oracle paths.
#' @return A tibble containing the Oracle query result.
#' @details
#' `redsan` does not hard-code a CORA account or schema. The active environment
#' decides which Oracle credentials are stored in the keystore, so the same
#' function can use an EDSaN service account in Podsan and a dedicated DIM
#' account on a workstation. Schema qualification should remain explicit in SQL,
#' for example `CORA_REC.MY_TABLE`.
#'
#' The read-only guard validates SQL syntax conservatively before sending it to
#' Oracle. Database privileges remain the authoritative security boundary.
#'
#' Parameter binding is intentionally not exposed yet because CORA currently
#' uses `RJDBC`; placeholder support should be validated against the deployed
#' driver before adding it to the public API.
#'
#' @examples
#' \dontrun{
#' query_cora("SELECT USER AS session_user FROM dual")
#'
#' query_cora(
#'   "SELECT owner, table_name
#'    FROM all_tables
#'    WHERE owner = 'CORA_REC' AND ROWNUM <= 10"
#' )
#' }
#' @export
query_cora <- function(sql, connection = NULL, ojdbc_jar = NULL) {
  .cora_query(
    sql = sql,
    connection = connection,
    ojdbc_jar = ojdbc_jar
  )
}
