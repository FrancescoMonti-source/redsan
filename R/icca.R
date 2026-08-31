# ICCA SQL Server access ------------------------------------------------------

.icca_sql_for_validation <- function(sql) {
  sql <- gsub("(?s)/\\*.*?\\*/", " ", sql, perl = TRUE)
  sql <- gsub("--[^\\r\\n]*", " ", sql, perl = TRUE)
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

.icca_instance_prefix <- function(instance = c("adult", "ped")) {
  instance <- match.arg(instance)
  switch(instance, adult = "iccaadu", ped = "iccaped")
}

.icca_connect <- function(instance = c("adult", "ped"),
                          driver = "FreeTDS", database = "CISReportingDB",
                          tds_version = "7.4") {
  .icca_require_namespace("DBI", "connection setup")
  .icca_require_namespace("odbc", "connection setup")

  prefix <- .icca_instance_prefix(instance)
  key <- function(suffix) paste0("db.", prefix, ".", suffix)

  server <- .icca_keystore_value(key("srv"))
  port_raw <- .icca_keystore_value(key("port"))
  user <- .icca_keystore_value(key("usr"))
  password <- .icca_keystore_value(key("pwd"))

  port <- suppressWarnings(as.integer(port_raw))
  if (length(port) != 1L || is.na(port) || port < 1L || port > 65535L) {
    stop(
      "ICCA connection key `", key("port"), "` is not a valid TCP port.",
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
#' database using native R DBI/ODBC access.
#'
#' @param sql One SQL Server `SELECT` statement. Common table expressions
#'   (`WITH ... SELECT ...`) are accepted. Multiple statements and write
#'   operations are rejected.
#' @param params Optional positional values for `?` placeholders.
#' @param connection Optional existing DBI connection. When `NULL`, `redsan`
#'   opens an ICCA connection and closes it after the query. A caller-supplied
#'   connection is never closed by `query_icca()`.
#' @param instance ICCA instance to query: `"adult"` (default) or `"ped"`.
#'   Ignored when `connection` is supplied explicitly.
#' @return A tibble containing the SQL Server result.
#' @export
query_icca <- function(sql, params = NULL, connection = NULL,
                       instance = c("adult", "ped")) {
  instance <- match.arg(instance)

  if (!is.null(connection)) {
    return(.icca_query(sql = sql, params = params, connection = connection))
  }

  connection <- .icca_connect(instance = instance)
  on.exit(.icca_disconnect(connection), add = TRUE)
  .icca_query(sql = sql, params = params, connection = connection)
}

# High-level pseudonymized ICCA retrieval ------------------------------------

.icca_validate_evtids <- function(evtids) {
  if (!is.character(evtids)) {
    stop("`evtids` must be character EDSaN EVTID values.", call. = FALSE)
  }
  if (length(evtids) == 0L) return(character())

  evtids <- trimws(evtids)
  if (anyNA(evtids) || any(!nzchar(evtids))) {
    stop("`evtids` must not contain missing or empty values.", call. = FALSE)
  }
  unique(evtids)
}

.icca_empty_encounter <- function() {
  tibble::tibble(
    EVTID = character(),
    encounterId = integer(),
    patientId = integer(),
    episodeId = integer(),
    gender = character(),
    primaryDiagnosis = character(),
    isArchived = logical(),
    systemId = integer()
  )
}

.icca_evtid_map <- function(evtids, env = "edsan-ct", ks_path = NULL,
                            reidentify = edsan_reidentify) {
  mapping <- reidentify(
    evtids,
    id_type = "EVTID",
    env = env,
    ks_path = ks_path
  )

  keep <- !is.na(mapping$HIS_ID) & nzchar(as.character(mapping$HIS_ID))
  mapping <- mapping[keep, , drop = FALSE]

  tibble::tibble(
    EVTID = as.character(mapping$EDSAN_ID),
    .IEP = as.character(mapping$HIS_ID)
  )
}

.icca_get_encounter <- function(evtids, connection = NULL,
                                env = "edsan-ct", ks_path = NULL,
                                reidentify = edsan_reidentify,
                                query = query_icca) {
  evtids <- .icca_validate_evtids(evtids)
  if (!length(evtids)) return(.icca_empty_encounter())

  mapping <- .icca_evtid_map(
    evtids,
    env = env,
    ks_path = ks_path,
    reidentify = reidentify
  )
  if (!nrow(mapping)) return(.icca_empty_encounter())

  ieps <- unique(mapping$.IEP)
  placeholders <- paste(rep("?", length(ieps)), collapse = ", ")
  sql <- paste(
    "SELECT",
    "  encounterId,",
    "  patientId,",
    "  episodeId,",
    "  encounterNumber,",
    "  gender,",
    "  primaryDiagnosis,",
    "  isArchived,",
    "  systemId",
    "FROM CISReportingDB.dbo.D_Encounter",
    paste0("WHERE encounterNumber IN (", placeholders, ")")
  )

  rows <- query(sql = sql, params = ieps, connection = connection)
  if (!nrow(rows)) return(.icca_empty_encounter())

  rows <- tibble::as_tibble(rows)
  rows$encounterNumber <- as.character(rows$encounterNumber)

  out <- dplyr::inner_join(
    rows,
    mapping,
    by = c("encounterNumber" = ".IEP")
  )
  out <- out[, c("EVTID", setdiff(names(out), c("EVTID", "encounterNumber"))),
             drop = FALSE]
  tibble::as_tibble(out)
}

.icca_detail_spec <- function(source) {
  common <- c(
    "encounterId",
    "cisPtInterventionId",
    "chartTime",
    "utcChartTime",
    "storeTime",
    "utcStoreTime",
    "interventionId",
    "interventionLongDisplayLabel",
    "interventionType",
    "interventionPropName",
    "attributeId",
    "attributeLongLabel",
    "attributePropName",
    "dataFocusLongLabel",
    "termLongLabel",
    "materialLongLabel",
    "siteLongLabel",
    "valueString",
    "valueDateTime",
    "utcValueDateTime",
    "valueNumber",
    "unitOfMeasure",
    "baseValueNumber",
    "baseUOM",
    "valueConcept",
    "remark",
    "mainState",
    "actionState",
    "isArchived",
    "dictionaryLabel",
    "dictionaryPropName"
  )

  switch(
    source,
    assessment = list(
      table = "CISReportingDB.DAR.PtAssessment",
      id_column = "ptAssessmentId",
      columns = c("ptAssessmentId", common)
    ),
    medication = list(
      table = "CISReportingDB.DAR.PtMedication",
      id_column = "ptMedicationId",
      columns = c("ptMedicationId", common, "isPrescribed")
    ),
    stop("Unsupported ICCA detail source `", source, "`.", call. = FALSE)
  )
}

.icca_empty_detail <- function(source) {
  source <- match.arg(source, c("assessment", "medication"))
  out <- tibble::tibble(EVTID = character())

  if (identical(source, "assessment")) {
    out$ptAssessmentId <- integer()
  } else {
    out$ptMedicationId <- integer()
  }

  out$encounterId <- integer()
  out$cisPtInterventionId <- character()
  out$chartTime <- as.POSIXct(character())
  out$utcChartTime <- as.POSIXct(character(), tz = "UTC")
  out$storeTime <- as.POSIXct(character())
  out$utcStoreTime <- as.POSIXct(character(), tz = "UTC")
  out$interventionId <- integer()
  out$interventionLongDisplayLabel <- character()
  out$interventionType <- character()
  out$interventionPropName <- character()
  out$attributeId <- integer()
  out$attributeLongLabel <- character()
  out$attributePropName <- character()
  out$dataFocusLongLabel <- character()
  out$termLongLabel <- character()
  out$materialLongLabel <- character()
  out$siteLongLabel <- character()
  out$valueString <- character()
  out$valueDateTime <- as.POSIXct(character())
  out$utcValueDateTime <- as.POSIXct(character(), tz = "UTC")
  out$valueNumber <- numeric()
  out$unitOfMeasure <- character()
  out$baseValueNumber <- numeric()
  out$baseUOM <- character()
  out$valueConcept <- character()
  out$remark <- character()
  out$mainState <- character()
  out$actionState <- character()
  out$isArchived <- logical()
  out$dictionaryLabel <- character()
  out$dictionaryPropName <- character()

  if (identical(source, "medication")) {
    out$isPrescribed <- logical()
  }

  out
}

.icca_get_detail <- function(evtids, source, connection = NULL,
                             env = "edsan-ct", ks_path = NULL,
                             reidentify = edsan_reidentify,
                             query = query_icca) {
  source <- match.arg(source, c("assessment", "medication"))
  evtids <- .icca_validate_evtids(evtids)
  if (!length(evtids)) return(.icca_empty_detail(source))

  encounters <- .icca_get_encounter(
    evtids,
    connection = connection,
    env = env,
    ks_path = ks_path,
    reidentify = reidentify,
    query = query
  )
  if (!nrow(encounters)) return(.icca_empty_detail(source))

  encounter_map <- unique(encounters[, c("EVTID", "encounterId"), drop = FALSE])
  encounter_ids <- unique(encounter_map$encounterId)
  placeholders <- paste(rep("?", length(encounter_ids)), collapse = ", ")
  spec <- .icca_detail_spec(source)
  selected <- paste(paste0("  ", spec$columns), collapse = ",\n")
  sql <- paste0(
    "SELECT\n",
    selected,
    "\nFROM ", spec$table,
    "\nWHERE encounterId IN (", placeholders, ")"
  )

  rows <- query(sql = sql, params = encounter_ids, connection = connection)
  if (!nrow(rows)) return(.icca_empty_detail(source))

  out <- dplyr::inner_join(
    tibble::as_tibble(rows),
    encounter_map,
    by = "encounterId"
  )
  out <- out[, c("EVTID", setdiff(names(out), "EVTID")), drop = FALSE]
  tibble::as_tibble(out)
}

#' Retrieve ICCA data by EDSaN EVTID
#'
#' Retrieves ICCA rows for pseudonymized EDSaN stay identifiers. `redsan`
#' reidentifies each EVTID to its IEP only long enough to locate the matching
#' ICCA encounter, then returns ICCA rows keyed by the original EVTID.
#'
#' @param evtids Character vector of EDSaN EVTID values.
#' @param source ICCA source to retrieve: `"encounter"`, `"assessment"`, or
#'   `"medication"`. Assessment and medication retrieval use the enriched
#'   `DAR.PtAssessment` and `DAR.PtMedication` reporting views.
#' @param connection Optional existing ICCA DBI connection.
#' @param instance ICCA instance to query: `"adult"` (default) or `"ped"`.
#'   Ignored when `connection` is supplied explicitly.
#' @param env EDSaN CT web-service environment name.
#' @param ks_path Optional d2imr keystore path for EDSaN CT correspondence.
#' @return A tibble keyed by `EVTID`.
#' @details `get_icca()` consumes the EVTID-to-IEP correspondences returned by
#'   EDSaN CT directly. If CT returns several IEPs for one EVTID, all are queried;
#'   EVTIDs without an IEP correspondence simply produce no ICCA rows.
#'   `character(0)` returns an empty result immediately and never produces an
#'   unfiltered query. `encounterNumber` (IEP) is used only as a transient lookup
#'   key and is never returned.
#'
#'   Assessment and medication outputs preserve ICCA's native long structure:
#'   one `cisPtInterventionId` identifies one intervention instance, which may
#'   span several rows carrying different `attributeId` / value combinations.
#'   `redsan` does not pivot, deduplicate, or clinically filter these rows.
#' @export
get_icca <- function(evtids, source = "encounter", connection = NULL,
                     instance = c("adult", "ped"),
                     env = "edsan-ct", ks_path = NULL) {
  source <- match.arg(source, c("encounter", "assessment", "medication"))
  instance <- match.arg(instance)

  if (!length(.icca_validate_evtids(evtids))) {
    if (identical(source, "encounter")) return(.icca_empty_encounter())
    return(.icca_empty_detail(source))
  }

  owns_connection <- is.null(connection)
  if (owns_connection) {
    connection <- .icca_connect(instance = instance)
    on.exit(.icca_disconnect(connection), add = TRUE)
  }

  if (identical(source, "encounter")) {
    return(.icca_get_encounter(
      evtids,
      connection = connection,
      env = env,
      ks_path = ks_path
    ))
  }

  .icca_get_detail(
    evtids,
    source = source,
    connection = connection,
    env = env,
    ks_path = ks_path
  )
}
