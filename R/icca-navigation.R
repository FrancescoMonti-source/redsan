# ICCA metadata navigation ----------------------------------------------------

.icca_source_aliases <- c(
  assessment = "DAR.PtAssessment",
  medication = "DAR.PtMedication"
)

.icca_anchor_columns <- c("encounterId", "patientId", "episodeId", "systemId")

.icca_normalize_source <- function(source) {
  if (!is.character(source) || length(source) != 1L || is.na(source) ||
      !nzchar(trimws(source))) {
    stop("`source` must be one non-empty ICCA object name.", call. = FALSE)
  }

  source <- trimws(source)
  if (source %in% names(.icca_source_aliases)) {
    source <- unname(.icca_source_aliases[[source]])
  }

  parts <- strsplit(source, ".", fixed = TRUE)[[1L]]
  if (length(parts) == 3L && identical(tolower(parts[[1L]]), "cisreportingdb")) {
    parts <- parts[-1L]
  }
  if (length(parts) != 2L || any(!nzchar(parts))) {
    stop(
      "`source` must be `schema.object` (for example `DAR.PatientAssessment`). ",
      "The convenience aliases `assessment` and `medication` are also accepted.",
      call. = FALSE
    )
  }

  list(
    schema = parts[[1L]],
    object = parts[[2L]],
    qualified = paste(parts, collapse = ".")
  )
}

.icca_quote_identifier <- function(x) {
  paste0("[", gsub("]", "]]", x, fixed = TRUE), "]")
}

.icca_qualified_source <- function(source) {
  src <- .icca_normalize_source(source)
  paste(
    .icca_quote_identifier("CISReportingDB"),
    .icca_quote_identifier(src$schema),
    .icca_quote_identifier(src$object),
    sep = "."
  )
}

.icca_metadata_select <- function() {
  paste(
    "s.name AS schema_name,",
    "o.name AS object_name,",
    "o.type_desc,",
    "COUNT(c.column_id) AS n_columns,",
    "MAX(CASE WHEN c.name = 'encounterId' THEN 1 ELSE 0 END) AS has_encounter_id,",
    "MAX(CASE WHEN c.name = 'patientId' THEN 1 ELSE 0 END) AS has_patient_id,",
    "MAX(CASE WHEN c.name = 'episodeId' THEN 1 ELSE 0 END) AS has_episode_id,",
    "MAX(CASE WHEN c.name = 'systemId' THEN 1 ELSE 0 END) AS has_system_id"
  )
}

.icca_logical_metadata <- function(out) {
  for (nm in c("has_encounter_id", "has_patient_id", "has_episode_id", "has_system_id")) {
    if (nm %in% names(out)) out[[nm]] <- as.logical(out[[nm]])
  }
  out
}

.icca_object_metadata <- function(source, connection = NULL, query = query_icca) {
  src <- .icca_normalize_source(source)
  sql <- paste(
    "SELECT", .icca_metadata_select(),
    "FROM CISReportingDB.sys.objects o",
    "INNER JOIN CISReportingDB.sys.schemas s ON s.schema_id = o.schema_id",
    "LEFT JOIN CISReportingDB.sys.columns c ON c.object_id = o.object_id",
    "WHERE s.name = ? AND o.name = ? AND o.type IN ('U', 'V')",
    "GROUP BY s.name, o.name, o.type_desc"
  )
  out <- query(sql = sql, params = c(src$schema, src$object), connection = connection)
  if (!nrow(out)) {
    stop("ICCA object `", src$qualified, "` was not found.", call. = FALSE)
  }
  tibble::as_tibble(.icca_logical_metadata(out))
}

#' List and search ICCA database objects
#'
#' Reads live SQL Server metadata to list ICCA tables and views. This is the
#' entry point for discovering sources without maintaining a hard-coded source
#' list in `redsan`.
#'
#' @param search Optional text matched case-insensitively against
#'   `schema.object`.
#' @param schema Optional schema name such as `"DAR"`, `"dbo"`, or `"CUS"`.
#' @param type Optional object type: `"table"` or `"view"`.
#' @param connection Optional existing ICCA DBI connection.
#' @return A tibble with one row per ICCA table/view and flags for common
#'   `D_Encounter` anchor columns.
#' @export
icca_catalog <- function(search = NULL, schema = NULL, type = NULL,
                         connection = NULL) {
  sql <- paste(
    "SELECT", .icca_metadata_select(),
    "FROM CISReportingDB.sys.objects o",
    "INNER JOIN CISReportingDB.sys.schemas s ON s.schema_id = o.schema_id",
    "LEFT JOIN CISReportingDB.sys.columns c ON c.object_id = o.object_id",
    "WHERE o.type IN ('U', 'V') AND o.is_ms_shipped = 0",
    "GROUP BY s.name, o.name, o.type_desc",
    "ORDER BY s.name, o.name"
  )
  out <- tibble::as_tibble(.icca_logical_metadata(query_icca(sql, connection = connection)))
  out$type <- ifelse(out$type_desc == "USER_TABLE", "table", "view")
  out <- out[, c("schema_name", "object_name", "type", "n_columns",
                 "has_encounter_id", "has_patient_id", "has_episode_id",
                 "has_system_id"), drop = FALSE]

  if (!is.null(search)) {
    if (!is.character(search) || length(search) != 1L || is.na(search)) {
      stop("`search` must be NULL or one character string.", call. = FALSE)
    }
    label <- paste(out$schema_name, out$object_name, sep = ".")
    out <- out[grepl(search, label, ignore.case = TRUE, fixed = TRUE), , drop = FALSE]
  }
  if (!is.null(schema)) {
    out <- out[tolower(out$schema_name) %in% tolower(schema), , drop = FALSE]
  }
  if (!is.null(type)) {
    type <- match.arg(tolower(type), c("table", "view"))
    out <- out[out$type == type, , drop = FALSE]
  }

  tibble::as_tibble(out)
}

.icca_relation_rows <- function(connection = NULL, query = query_icca) {
  fk_sql <- paste(
    "SELECT",
    "  'foreign_key' AS relation_type,",
    "  OBJECT_SCHEMA_NAME(fk.parent_object_id) AS from_schema,",
    "  OBJECT_NAME(fk.parent_object_id) AS from_object,",
    "  pc.name AS from_column,",
    "  OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS to_schema,",
    "  OBJECT_NAME(fk.referenced_object_id) AS to_object,",
    "  rc.name AS to_column,",
    "  fk.name AS relation_name",
    "FROM CISReportingDB.sys.foreign_keys fk",
    "INNER JOIN CISReportingDB.sys.foreign_key_columns fkc",
    "  ON fk.object_id = fkc.constraint_object_id",
    "INNER JOIN CISReportingDB.sys.columns pc",
    "  ON pc.object_id = fkc.parent_object_id",
    " AND pc.column_id = fkc.parent_column_id",
    "INNER JOIN CISReportingDB.sys.columns rc",
    "  ON rc.object_id = fkc.referenced_object_id",
    " AND rc.column_id = fkc.referenced_column_id"
  )

  dep_sql <- paste(
    "SELECT",
    "  'view_dependency' AS relation_type,",
    "  OBJECT_SCHEMA_NAME(d.referencing_id) AS from_schema,",
    "  OBJECT_NAME(d.referencing_id) AS from_object,",
    "  CAST(NULL AS nvarchar(128)) AS from_column,",
    "  COALESCE(d.referenced_schema_name, OBJECT_SCHEMA_NAME(d.referenced_id)) AS to_schema,",
    "  COALESCE(d.referenced_entity_name, OBJECT_NAME(d.referenced_id)) AS to_object,",
    "  CAST(NULL AS nvarchar(128)) AS to_column,",
    "  CAST(NULL AS nvarchar(128)) AS relation_name",
    "FROM CISReportingDB.sys.sql_expression_dependencies d",
    "WHERE OBJECTPROPERTY(d.referencing_id, 'IsView') = 1"
  )

  dplyr::bind_rows(
    query(sql = fk_sql, connection = connection),
    query(sql = dep_sql, connection = connection)
  )
}

#' Inspect declared ICCA object relations
#'
#' Combines SQL Server foreign keys (table-to-table column relations) with SQL
#' expression dependencies (objects used to build views). View dependencies
#' identify which objects are used, but do not by themselves encode join
#' columns.
#'
#' @param source Optional `schema.object`. When supplied, only relations touching
#'   that object are returned.
#' @param direction With `source`, return relations going `"out"`, coming
#'   `"in"`, or `"both"` directions.
#' @param connection Optional existing ICCA DBI connection.
#' @return A tibble of relation edges.
#' @export
icca_relations <- function(source = NULL, direction = c("both", "out", "in"),
                           connection = NULL) {
  direction <- match.arg(direction)
  out <- tibble::as_tibble(.icca_relation_rows(connection = connection))

  if (!is.null(source)) {
    src <- .icca_normalize_source(source)
    from_match <- tolower(out$from_schema) == tolower(src$schema) &
      tolower(out$from_object) == tolower(src$object)
    to_match <- !is.na(out$to_schema) & !is.na(out$to_object) &
      tolower(out$to_schema) == tolower(src$schema) &
      tolower(out$to_object) == tolower(src$object)
    keep <- switch(direction, out = from_match, in = to_match,
                   both = from_match | to_match)
    out <- out[keep, , drop = FALSE]
  }

  tibble::as_tibble(out)
}

#' Describe one ICCA table or view
#'
#' Returns live metadata for an ICCA object together with its columns and known
#' relations. The result describes database structure, not clinical semantics.
#'
#' @param source ICCA object as `schema.object`.
#' @param connection Optional existing ICCA DBI connection.
#' @return An `icca_description` list with `object`, `columns`, and `relations`
#'   tibbles.
#' @export
icca_describe <- function(source, connection = NULL) {
  src <- .icca_normalize_source(source)
  object <- .icca_object_metadata(source, connection = connection)

  columns_sql <- paste(
    "SELECT",
    "  c.column_id AS ordinal_position,",
    "  c.name AS column_name,",
    "  t.name AS data_type,",
    "  c.max_length,",
    "  c.precision,",
    "  c.scale,",
    "  c.is_nullable",
    "FROM CISReportingDB.sys.columns c",
    "INNER JOIN CISReportingDB.sys.objects o ON o.object_id = c.object_id",
    "INNER JOIN CISReportingDB.sys.schemas s ON s.schema_id = o.schema_id",
    "INNER JOIN CISReportingDB.sys.types t ON t.user_type_id = c.user_type_id",
    "WHERE s.name = ? AND o.name = ?",
    "ORDER BY c.column_id"
  )
  columns <- query_icca(
    columns_sql,
    params = c(src$schema, src$object),
    connection = connection
  )
  columns$is_nullable <- as.logical(columns$is_nullable)

  out <- list(
    object = tibble::as_tibble(object),
    columns = tibble::as_tibble(columns),
    relations = icca_relations(source, connection = connection)
  )
  class(out) <- c("icca_description", "list")
  out
}

#' @export
print.icca_description <- function(x, ...) {
  object <- x$object
  anchors <- .icca_anchor_columns[c(
    object$has_encounter_id[[1L]], object$has_patient_id[[1L]],
    object$has_episode_id[[1L]], object$has_system_id[[1L]]
  )]
  cat("<ICCA object> ", object$schema_name[[1L]], ".", object$object_name[[1L]],
      " (", tolower(sub("^USER_", "", object$type_desc[[1L]])), ")\n", sep = "")
  cat("Columns: ", object$n_columns[[1L]], " | D_Encounter anchors: ",
      if (length(anchors)) paste(anchors, collapse = ", ") else "none",
      "\n", sep = "")
  cat("\nColumns\n")
  print(x$columns, ...)
  cat("\nKnown relations\n")
  print(x$relations, ...)
  invisible(x)
}

.icca_source_anchors <- function(object) {
  flags <- c(
    encounterId = isTRUE(object$has_encounter_id[[1L]]),
    patientId = isTRUE(object$has_patient_id[[1L]]),
    episodeId = isTRUE(object$has_episode_id[[1L]]),
    systemId = isTRUE(object$has_system_id[[1L]])
  )
  names(flags)[flags]
}

.icca_choose_link <- function(object, link = c("auto", .icca_anchor_columns)) {
  link <- match.arg(link)
  available <- .icca_source_anchors(object)
  if (!length(available)) {
    stop(
      "This ICCA object has none of the direct D_Encounter anchor columns ",
      "(`encounterId`, `patientId`, `episodeId`, `systemId`). Use ",
      "`icca_relations()` to inspect indirect linkage and `query_icca()` for ",
      "unrestricted access.", call. = FALSE
    )
  }
  if (!identical(link, "auto")) {
    if (!link %in% available) {
      stop("Requested `link = ", link, "` is not present in this ICCA object.",
           call. = FALSE)
    }
    return(link)
  }
  if ("encounterId" %in% available) return("encounterId")
  if (length(available) == 1L) return(available)
  stop(
    "ICCA object has several possible D_Encounter anchors (",
    paste(available, collapse = ", "), "). Specify `link` explicitly.",
    call. = FALSE
  )
}

# Generic EVTID-linked retrieval ---------------------------------------------

.icca_get_source <- function(evtids, source, link = "auto", connection = NULL,
                             env = "edsan-ct", ks_path = NULL,
                             reidentify = edsan_reidentify,
                             query = query_icca,
                             metadata = .icca_object_metadata) {
  evtids <- .icca_validate_evtids(evtids)
  src <- .icca_normalize_source(source)
  if (!length(evtids)) return(tibble::tibble(EVTID = character()))

  object <- metadata(src$qualified, connection = connection, query = query)
  link <- .icca_choose_link(object, link = link)

  encounters <- .icca_get_encounter(
    evtids,
    connection = connection,
    env = env,
    ks_path = ks_path,
    reidentify = reidentify,
    query = query
  )
  if (!nrow(encounters)) return(tibble::tibble(EVTID = character()))

  if (!link %in% names(encounters)) {
    stop("D_Encounter retrieval does not expose anchor `", link, "`.", call. = FALSE)
  }
  source_map <- unique(encounters[, c("EVTID", link), drop = FALSE])
  names(source_map)[2L] <- ".icca_link_value"
  values <- unique(source_map$.icca_link_value)
  values <- values[!is.na(values)]
  if (!length(values)) return(tibble::tibble(EVTID = character()))

  placeholders <- paste(rep("?", length(values)), collapse = ", ")
  link_sql <- .icca_quote_identifier(link)
  sql <- paste0(
    "SELECT * FROM ", .icca_qualified_source(src$qualified),
    " WHERE ", link_sql, " IN (", placeholders, ")"
  )
  rows <- query(sql = sql, params = values, connection = connection)
  if (!nrow(rows)) return(tibble::tibble(EVTID = character()))

  rows <- tibble::as_tibble(rows)
  rows$.icca_link_value <- rows[[link]]
  out <- dplyr::inner_join(rows, source_map, by = ".icca_link_value")
  out$.icca_link_value <- NULL
  out <- out[, c("EVTID", setdiff(names(out), "EVTID")), drop = FALSE]
  tibble::as_tibble(out)
}
