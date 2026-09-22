# CORA schema exploration ----------------------------------------------------

.cora_assert_scalar_text <- function(x, arg) {
  if (!is.character(x) || length(x) != 1L || is.na(x) || !nzchar(trimws(x))) {
    stop(sprintf("`%s` must be one non-empty string.", arg), call. = FALSE)
  }
  trimws(x)
}

.cora_normalize_identifier <- function(x, arg) {
  x <- .cora_assert_scalar_text(x, arg)
  if (!grepl("^[A-Za-z][A-Za-z0-9_$#]*$", x)) {
    stop(
      sprintf(
        "`%s` must be a standard unquoted Oracle identifier; got `%s`.",
        arg,
        x
      ),
      call. = FALSE
    )
  }
  toupper(x)
}

.cora_sql_string <- function(x) {
  paste0("'", gsub("'", "''", x, fixed = TRUE), "'")
}

.cora_parse_table_ref <- function(table) {
  table <- .cora_assert_scalar_text(table, "table")
  parts <- strsplit(table, ".", fixed = TRUE)[[1L]]

  if (length(parts) == 1L) {
    return(list(
      owner = NULL,
      table = .cora_normalize_identifier(parts[[1L]], "table")
    ))
  }

  if (length(parts) == 2L) {
    return(list(
      owner = .cora_normalize_identifier(parts[[1L]], "owner"),
      table = .cora_normalize_identifier(parts[[2L]], "table")
    ))
  }

  stop("`table` must be `TABLE` or `OWNER.TABLE`.", call. = FALSE)
}

.cora_describe_table <- function(table, connection = NULL, ojdbc_jar = NULL,
                                 query = .cora_query) {
  ref <- .cora_parse_table_ref(table)

  predicates <- sprintf(
    "UPPER(table_name) = %s",
    .cora_sql_string(ref$table)
  )
  if (!is.null(ref$owner)) {
    predicates <- c(
      predicates,
      sprintf("UPPER(owner) = %s", .cora_sql_string(ref$owner))
    )
  }

  sql <- paste(
    "SELECT",
    "  owner, table_name, column_id, column_name, data_type, nullable,",
    "  data_length, data_precision, data_scale, char_length",
    "FROM all_tab_columns",
    paste("WHERE", paste(predicates, collapse = " AND ")),
    "ORDER BY owner, column_id",
    sep = "\n"
  )

  out <- query(
    sql = sql,
    connection = connection,
    ojdbc_jar = ojdbc_jar
  )

  if (nrow(out) == 0L) {
    stop(
      sprintf("No accessible table or view named `%s` was found.", table),
      call. = FALSE
    )
  }

  if (is.null(ref$owner)) {
    owners <- sort(unique(out$OWNER))
    if (length(owners) > 1L) {
      stop(
        sprintf(
          paste0(
            "`%s` exists in multiple accessible schemas: %s. ",
            "Use `OWNER.TABLE` to disambiguate."
          ),
          ref$table,
          paste(owners, collapse = ", ")
        ),
        call. = FALSE
      )
    }
  }

  keep <- c(
    "COLUMN_ID", "COLUMN_NAME", "DATA_TYPE", "NULLABLE",
    "DATA_LENGTH", "DATA_PRECISION", "DATA_SCALE", "CHAR_LENGTH"
  )
  tibble::as_tibble(out[, keep, drop = FALSE])
}

#' Describe the columns of a CORA table or view
#'
#' Returns a compact structural description of one accessible Oracle table or
#' view using `ALL_TAB_COLUMNS`. No CORA schema is assumed.
#'
#' @param table Table or view name. Prefer a schema-qualified name such as
#'   `"CORA_REC.TB_SEJOUR"`. An unqualified name is accepted only when it exists
#'   in exactly one accessible schema.
#' @param connection Optional existing CORA DBI connection. When `NULL`, a
#'   transient connection is opened through the same machinery as [cora_query()].
#' @param ojdbc_jar Optional path to an Oracle JDBC driver.
#' @return A tibble with column position, name, datatype, nullability, and Oracle
#'   length/precision metadata.
#' @details Quoted Oracle identifiers are intentionally not supported. The
#'   helper is an ergonomic view over Oracle metadata, not a replacement for
#'   [cora_query()].
#' @examples
#' \dontrun{
#' cora_describe_table("CORA_REC.TB_SEJOUR")
#' }
#' @export
cora_describe_table <- function(table, connection = NULL, ojdbc_jar = NULL) {
  .cora_describe_table(
    table = table,
    connection = connection,
    ojdbc_jar = ojdbc_jar
  )
}

.cora_dig <- function(term, owner = NULL, exact = FALSE,
                      connection = NULL, ojdbc_jar = NULL,
                      query = .cora_query) {
  term <- .cora_assert_scalar_text(term, "term")
  if (!is.logical(exact) || length(exact) != 1L || is.na(exact)) {
    stop("`exact` must be TRUE or FALSE.", call. = FALSE)
  }

  owner <- if (is.null(owner)) {
    NULL
  } else {
    .cora_normalize_identifier(owner, "owner")
  }

  term_sql <- .cora_sql_string(toupper(term))
  object_match <- if (isTRUE(exact)) {
    sprintf("UPPER(o.object_name) = %s", term_sql)
  } else {
    sprintf("INSTR(UPPER(o.object_name), %s) > 0", term_sql)
  }
  column_match <- if (isTRUE(exact)) {
    sprintf("UPPER(c.column_name) = %s", term_sql)
  } else {
    sprintf("INSTR(UPPER(c.column_name), %s) > 0", term_sql)
  }

  object_owner <- if (is.null(owner)) {
    ""
  } else {
    paste0(" AND UPPER(o.owner) = ", .cora_sql_string(owner))
  }
  column_owner <- if (is.null(owner)) {
    ""
  } else {
    paste0(" AND UPPER(c.owner) = ", .cora_sql_string(owner))
  }

  sql <- paste0(
    "WITH object_types AS (\n",
    "  SELECT owner, object_name, MAX(object_type) AS object_type\n",
    "  FROM all_objects\n",
    "  WHERE object_type IN ('TABLE', 'VIEW')\n",
    "  GROUP BY owner, object_name\n",
    "),\n",
    "pk_columns AS (\n",
    "  SELECT con.owner, con.table_name, col.column_name\n",
    "  FROM all_constraints con\n",
    "  JOIN all_cons_columns col\n",
    "    ON con.owner = col.owner\n",
    "   AND con.constraint_name = col.constraint_name\n",
    "  WHERE con.constraint_type = 'P'\n",
    ")\n",
    "SELECT owner, object_type, table_name, column_name, data_type, matched_on, key_type\n",
    "FROM (\n",
    "  SELECT\n",
    "    o.owner,\n",
    "    o.object_type,\n",
    "    o.object_name AS table_name,\n",
    "    CAST(NULL AS VARCHAR2(128)) AS column_name,\n",
    "    CAST(NULL AS VARCHAR2(128)) AS data_type,\n",
    "    'OBJECT' AS matched_on,\n",
    "    CAST(NULL AS VARCHAR2(2)) AS key_type\n",
    "  FROM all_objects o\n",
    "  WHERE o.object_type IN ('TABLE', 'VIEW')\n",
    "    AND ", object_match, object_owner, "\n",
    "\n",
    "  UNION ALL\n",
    "\n",
    "  SELECT\n",
    "    c.owner,\n",
    "    COALESCE(ot.object_type, 'TABLE/VIEW') AS object_type,\n",
    "    c.table_name,\n",
    "    c.column_name,\n",
    "    c.data_type,\n",
    "    'COLUMN' AS matched_on,\n",
    "    CASE WHEN pk.column_name IS NOT NULL THEN 'PK' END AS key_type\n",
    "  FROM all_tab_columns c\n",
    "  LEFT JOIN object_types ot\n",
    "    ON ot.owner = c.owner\n",
    "   AND ot.object_name = c.table_name\n",
    "  LEFT JOIN pk_columns pk\n",
    "    ON pk.owner = c.owner\n",
    "   AND pk.table_name = c.table_name\n",
    "   AND pk.column_name = c.column_name\n",
    "  WHERE ", column_match, column_owner, "\n",
    ") matches\n",
    "ORDER BY owner, table_name, matched_on, column_name"
  )

  query(
    sql = sql,
    connection = connection,
    ojdbc_jar = ojdbc_jar
  )
}

#' Search CORA's accessible Oracle metadata
#'
#' Searches accessible table/view names and column names without assuming a
#' particular CORA schema. Declared primary-key columns are annotated as `PK`.
#'
#' @param term Non-empty text to search for, case-insensitively.
#' @param owner Optional Oracle schema/owner to restrict the search to. When
#'   `NULL`, every schema visible to the current CORA account is searched.
#' @param exact If `FALSE` (default), `term` is searched as a literal substring.
#'   If `TRUE`, only exact table/view or column-name matches are returned.
#' @param connection Optional existing CORA DBI connection. When `NULL`, a
#'   transient connection is opened through the same machinery as [cora_query()].
#' @param ojdbc_jar Optional path to an Oracle JDBC driver.
#' @return A tibble identifying the owner, object type, table/view, matched
#'   column when applicable, datatype, match location, and declared PK status.
#' @details `cora_dig()` reports metadata facts only. Shared column names are not
#'   interpreted as foreign-key relationships, and the function does not search
#'   patient-level data values.
#' @examples
#' \dontrun{
#' cora_dig("ID_SEJOUR")
#' cora_dig("diag", owner = "CORA_REC")
#' cora_dig("TB_SEJOUR", exact = TRUE)
#' }
#' @export
cora_dig <- function(term, owner = NULL, exact = FALSE,
                     connection = NULL, ojdbc_jar = NULL) {
  .cora_dig(
    term = term,
    owner = owner,
    exact = exact,
    connection = connection,
    ojdbc_jar = ojdbc_jar
  )
}
