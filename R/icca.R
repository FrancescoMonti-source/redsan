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

.icca_require_namespace <- function(package, purpose) {
  if (!requireNamespace(package, quietly = TRUE)) {
    stop(
      "ICCA ", purpose, " requires the optional package `", package, "`.",
      call. = FALSE
    )
  }
}

.icca_keystore_value <- function(key) {
  .icca_require_namespace("d2imr", "connection setup")

  getter <- tryCatch(
    getExportedValue("d2imr", "d2im_keystore.get"),
    error = function(e) NULL
  )
  if (!is.function(getter)) {
    stop(
      "Package `d2imr` must export `d2im_keystore.get()` for ICCA connection setup.",
      call. = FALSE
    )
  }

  value <- tryCatch(
    getter(key),
    error = function(e) {
      stop(
        "Could not read ICCA connection key `", key, "` from the d2imr keystore: ",
        conditionMessage(e),
        call. = FALSE
      )
    }
  )
  value <- as.character(value)
  if (length(value) != 1L || is.na(value) || !nzchar(value)) {
    stop(
      "Required ICCA connection key `", key,
      "` is missing or empty in the active d2imr keystore.",
      call. = FALSE
    )
  }
  value
}

.icca_connect <- function(driver = "FreeTDS", database = "CISReportingDB",
                          tds_version = "7.4") {
  .icca_require_namespace("DBI", "connection setup")
  .icca_require_namespace("odbc", "connection setup")

  server <- .icca_keystore_value("db.iccaadu.srv")
  port_raw <- .icca_keystore_value("db.iccaadu.port")
  user <- .icca_keystore_value("db.iccaadu.usr")
  password <- .icca_keystore_value("db.iccaadu.pwd")

  port <- suppressWarnings(as.integer(port_raw))
  if (length(port) != 1L || is.na(port) || port < 1L || port > 65535L) {
    stop(
      "ICCA connection key `db.iccaadu.port` is not a valid TCP port.",
      call. = FALSE
    )
  }

  tryCatch(
    DBI::dbConnect(
      odbc::odbc(),
      Driver = driver,
      Server = server,
      Port = port,
      Database = database,
      UID = user,
      PWD = password,
      TDS_Version = tds_version
    ),
    error = function(e) {
      stop("Could not connect to ICCA: ", conditionMessage(e), call. = FALSE)
    }
  )
}

.icca_execute <- function(connection, sql, params = NULL) {
  .icca_require_namespace("DBI", "query execution")
  if (is.null(params)) {
    DBI::dbGetQuery(connection, sql)
  } else {
    DBI::dbGetQuery(connection, sql, params = params)
  }
}

.icca_disconnect <- function(connection) {
  .icca_require_namespace("DBI", "connection cleanup")
  DBI::dbDisconnect(connection)
}

.icca_query <- function(sql, params = NULL, connection = NULL,
                        connect = .icca_connect,
                        execute = .icca_execute,
                        disconnect = .icca_disconnect) {
  sql <- .icca_validate_read_query(sql)
  params <- .icca_normalize_params(params)

  owns_connection <- is.null(connection)
  if (owns_connection) {
    connection <- connect()
    if (is.null(connection)) {
      stop("The ICCA connection factory returned `NULL`.", call. = FALSE)
    }
    on.exit(disconnect(connection), add = TRUE)
  }

  out <- tryCatch(
    execute(connection, sql, params),
    error = function(e) {
      stop("ICCA query failed: ", conditionMessage(e), call. = FALSE)
    }
  )

  if (!is.data.frame(out)) {
    stop("The ICCA query backend must return a data frame.", call. = FALSE)
  }

  tibble::as_tibble(out)
}

#' Execute a read-only query against ICCA
#'
#' Executes one parameterized read-only SQL Server query against the CHU ICCA
#' database using native R DBI/ODBC access. The function is intentionally
#' low-level: it exposes source access without adding clinical concept
#' definitions or pseudonymization logic.
#'
#' @param sql One SQL Server `SELECT` statement. Common table expressions
#'   (`WITH ... SELECT ...`) are accepted. Multiple statements and write
#'   operations are rejected.
#' @param params Optional positional values for `?` placeholders. Values are
#'   passed to [DBI::dbGetQuery()] as bound parameters.
#' @param connection Optional existing DBI connection. When `NULL`, `redsan`
#'   opens an ICCA connection using FreeTDS and the active `d2imr` keystore,
#'   then closes it after the query. A caller-supplied connection is never
#'   closed by `query_icca()`.
#'
#' @return A tibble containing the SQL Server result.
#'
#' @details
#' Automatic connection setup requires the optional packages `DBI`, `odbc`, and
#' `d2imr`, a registered FreeTDS ODBC driver, and the following keys in the
#' active d2imr keystore: `db.iccaadu.srv`, `db.iccaadu.port`,
#' `db.iccaadu.usr`, and `db.iccaadu.pwd`. The target database is
#' `CISReportingDB` and the default TDS protocol version is `7.4`.
#'
#' The read-only validation is an accidental-write guard, not a SQL security
#' sandbox. Database permissions remain the authoritative access control.
#'
#' @examples
#' \dontrun{
#' # Connectivity/schema smoke test: returns zero rows and no patient data.
#' query_icca(
#'   "SELECT TOP 0 encounterId FROM CISReportingDB.dbo.D_Encounter"
#' )
#'
#' query_icca(
#'   paste(
#'     "SELECT TOP 10 encounterId, encounterNumber",
#'     "FROM CISReportingDB.dbo.D_Encounter",
#'     "WHERE encounterNumber = ?"
#'   ),
#'   params = "123456789"
#' )
#' }
#'
#' @export
query_icca <- function(sql, params = NULL, connection = NULL) {
  .icca_query(sql = sql, params = params, connection = connection)
}
