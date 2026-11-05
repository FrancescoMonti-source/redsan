#' Open a persistent connection to a database source
#'
#' Returns a DBI connection using the source's existing credential lookup and
#' driver setup. Supports CORA (Oracle) and ICCA (SQL Server) only; REST sources
#' such as EDSaN CT are not supported.
#'
#' @param source Database source: `"cora"` or `"icca"`.
#' @param ... Named options forwarded to the source's connection factory.
#'   CORA accepts `ojdbc_jar` and `ks_path`. ICCA accepts `instance`
#'   (`"adult"`, the default, or `"ped"`), `driver`, `database`, and `tds_version`.
#' @return A DBI connection owned by the caller. Close it with
#'   `DBI::dbDisconnect()` when finished, including when a query fails.
#' @details
#' Use this connection directly with DBI or dbplyr, or pass it as `connection`
#' to [cora_query()] or [icca_query()]. Those functions still open and close a
#' transient connection when called without `connection`.
#'
#' Connection requirements are unchanged: CORA uses the optional packages DBI,
#' RJDBC, rJava, and d2imr plus an Oracle JDBC driver; ICCA uses DBI, odbc, and
#' d2imr plus a configured ODBC driver (FreeTDS by default).
#' @seealso [cora_query()], [icca_query()]
#' @examples
#' \dontrun{
#' con <- db_connect("cora")
#' tryCatch(
#'   DBI::dbGetQuery(con, "SELECT 1 AS n FROM dual"),
#'   finally = DBI::dbDisconnect(con)
#' )
#'
#' con <- db_connect("icca", instance = "ped")
#' tryCatch(
#'   icca_query("SELECT TOP 10 * FROM dbo.D_Encounter", connection = con),
#'   finally = DBI::dbDisconnect(con)
#' )
#' }
#' @export
db_connect <- function(source, ...) {
  if (!is.character(source) || length(source) != 1L || is.na(source) ||
      !source %in% c("cora", "icca")) {
    stop(
      "`source` must be one of \"cora\" or \"icca\"; only database sources are supported.",
      call. = FALSE
    )
  }

  switch(source, cora = .cora_connect(...), icca = .icca_connect(...))
}
