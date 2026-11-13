# CORA diet observation retrieval -------------------------------------------

.cora_require_namespace <- function(package, purpose) {
  if (!requireNamespace(package, quietly = TRUE)) {
    stop(
      "CORA ", purpose, " requires the optional package `", package, "`.",
      call. = FALSE
    )
  }
}

.cora_keystore_value <- function(key, ks_path = NULL) {
  .cora_require_namespace("d2imr", "connection setup")

  getter <- tryCatch(
    getExportedValue("d2imr", "d2im_keystore.get"),
    error = function(e) NULL
  )
  if (!is.function(getter)) {
    stop(
      "Package `d2imr` must export `d2im_keystore.get()` for CORA connection setup.",
      call. = FALSE
    )
  }

  value <- tryCatch(
    if (is.null(ks_path)) getter(key) else getter(key, ks_path = ks_path),
    error = function(e) {
      stop(
        "Could not read CORA connection key `", key, "` from the d2imr keystore: ",
        conditionMessage(e),
        call. = FALSE
      )
    }
  )

  value <- as.character(value)
  if (length(value) != 1L || is.na(value) || !nzchar(value)) {
    stop(
      "Required CORA connection key `", key,
      "` is missing or empty in the active d2imr keystore.",
      call. = FALSE
    )
  }

  value
}

.cora_default_ojdbc <- function() {
  candidates <- c(
    "/opt/oracle/instantclient_23_26/ojdbc17.jar",
    "/opt/oracle/instantclient_23_26/ojdbc11.jar",
    "/opt/oracle/instantclient_23_26/ojdbc8.jar",
    "/appli/shared/legacy_lib/lib/j/ojdbc6.jar",
    "/appli/shared/legacy_lib/j/ojdbc6.jar"
  )

  hit <- candidates[file.exists(candidates)]
  if (!length(hit)) {
    stop(
      "Could not find a usable Oracle JDBC driver for CORA. ",
      "Supply `ojdbc_jar` explicitly.",
      call. = FALSE
    )
  }

  hit[[1L]]
}

.cora_connect <- function(ojdbc_jar = NULL, ks_path = NULL) {
  .cora_require_namespace("DBI", "connection setup")
  .cora_require_namespace("RJDBC", "connection setup")
  .cora_require_namespace("rJava", "connection setup")

  if (is.null(ojdbc_jar)) ojdbc_jar <- .cora_default_ojdbc()
  if (!file.exists(ojdbc_jar)) {
    stop("Oracle JDBC driver not found at `", ojdbc_jar, "`.", call. = FALSE)
  }

  rJava::.jinit()
  rJava::.jcall(
    "java/lang/System", "S", "setProperty",
    "oracle.jdbc.timezoneAsRegion", "false"
  )
  rJava::.jcall(
    "java/lang/System", "S", "setProperty",
    "user.timezone", "UTC"
  )

  drv <- RJDBC::JDBC(
    "oracle.jdbc.OracleDriver",
    ojdbc_jar,
    identifier.quote = "`"
  )

  jdbc_url <- paste0(
    "jdbc:oracle:thin:@",
    .cora_keystore_value("db.cora.url", ks_path = ks_path)
  )

  tryCatch(
    DBI::dbConnect(
      drv,
      jdbc_url,
      .cora_keystore_value("db.cora.usr", ks_path = ks_path),
      .cora_keystore_value("db.cora.pwd", ks_path = ks_path)
    ),
    error = function(e) {
      stop("Could not connect to CORA: ", conditionMessage(e), call. = FALSE)
    }
  )
}

.cora_validate_ieps <- function(ieps) {
  if (!is.character(ieps)) {
    stop("`ieps` must be character IEP values.", call. = FALSE)
  }
  if (!length(ieps)) return(character())

  ieps <- trimws(ieps)
  if (anyNA(ieps) || any(!nzchar(ieps))) {
    stop("`ieps` must not contain missing or empty values.", call. = FALSE)
  }
  if (any(!grepl("^[0-9]+$", ieps))) {
    stop("`ieps` must contain digits only.", call. = FALSE)
  }

  unique(ieps)
}

.cora_validate_stay_ids <- function(ids, id_type = c("IEP", "EVTID")) {
  id_type <- match.arg(id_type)
  if (!is.character(ids)) {
    stop("`ids` must be character ", id_type, " values.", call. = FALSE)
  }
  if (!length(ids)) return(character())

  ids <- trimws(ids)
  if (anyNA(ids) || any(!nzchar(ids))) {
    stop("`ids` must not contain missing or empty values.", call. = FALSE)
  }
  if (any(!grepl("^[0-9]+$", ids))) {
    stop("`ids` must contain digits only.", call. = FALSE)
  }

  unique(ids)
}

.cora_diet_id_map <- function(ids, id_type = c("IEP", "EVTID"),
                              env = "edsan-ct", ks_path = NULL,
                              reidentify = .edsan_ct_legacy_reidentify,
                              pseudonymize = .edsan_ct_legacy_pseudonymize) {
  id_type <- match.arg(id_type)
  ids <- .cora_validate_stay_ids(ids, id_type)
  if (!length(ids)) {
    return(tibble::tibble(EVTID = character(), IEP = character()))
  }

  if (identical(id_type, "EVTID")) {
    mapped <- reidentify(
      ids,
      id_type = "EVTID",
      env = env,
      ks_path = ks_path
    )
    if (!all(c("EVTID", "IEP") %in% names(mapped))) {
      stop("EDSaN reidentification did not return EVTID/IEP columns.",
           call. = FALSE)
    }
    out <- tibble::tibble(
      EVTID = as.character(mapped$EVTID),
      IEP = as.character(mapped$IEP)
    )
  } else {
    mapped <- pseudonymize(
      ids,
      id_type = "IEP",
      env = env,
      ks_path = ks_path
    )
    if (!all(c("HIS_ID", "EDSAN_ID") %in% names(mapped))) {
      stop("EDSaN pseudonymization did not return HIS_ID/EDSAN_ID columns.",
           call. = FALSE)
    }
    out <- tibble::tibble(
      EVTID = as.character(mapped$EDSAN_ID),
      IEP = as.character(mapped$HIS_ID)
    )
  }

  out$EVTID <- trimws(out$EVTID)
  out$IEP <- trimws(out$IEP)
  unique(out)
}

.cora_validate_document_key <- function(nodocument, typedoc) {
  nodocument <- trimws(as.character(nodocument))
  typedoc <- trimws(as.character(typedoc))

  if (length(nodocument) != 1L || is.na(nodocument) ||
      !grepl("^[0-9]+$", nodocument)) {
    stop("Invalid CORA document identifier.", call. = FALSE)
  }
  if (length(typedoc) != 1L || is.na(typedoc) ||
      !grepl("^[A-Z0-9]$", typedoc)) {
    stop("Invalid CORA document type.", call. = FALSE)
  }

  list(nodocument = nodocument, typedoc = typedoc)
}

.cora_empty_diet <- function() {
  tibble::tibble(
    EVTID = character(),
    IEP = character(),
    NODOCUMENT = character(),
    TYPEDOC = character(),
    CORA_NOEVT = character(),
    DTDOC = as.POSIXct(character()),
    TIMECREATE = as.POSIXct(character()),
    REDACTEUR = character(),
    NOUSHEB = character(),
    NOUSRESP = character(),
    TEXT = character()
  )
}

.cora_blob_length <- function(connection, nodocument, typedoc) {
  key <- .cora_validate_document_key(nodocument, typedoc)
  sql <- paste0(
    "SELECT DBMS_LOB.GETLENGTH(DOCUMENT_BRUT) AS N\n",
    "FROM ICSF.T_DOCUMENT_MEMO\n",
    "WHERE NODOCUMENT = '", key$nodocument, "'\n",
    "  AND TYPEDOC = '", key$typedoc, "'"
  )

  out <- DBI::dbGetQuery(connection, sql)
  if (!nrow(out)) return(NA_integer_)
  as.integer(out$N[[1L]])
}

.cora_blob_chunk_hex <- function(connection, nodocument, typedoc,
                                 amount, offset) {
  key <- .cora_validate_document_key(nodocument, typedoc)
  amount <- as.integer(amount)
  offset <- as.integer(offset)
  if (is.na(amount) || amount < 1L || amount > 2000L ||
      is.na(offset) || offset < 1L) {
    stop("Invalid CORA BLOB chunk request.", call. = FALSE)
  }

  sql <- paste0(
    "SELECT RAWTOHEX(DBMS_LOB.SUBSTR(DOCUMENT_BRUT, ", amount, ", ", offset, ")) AS HEX\n",
    "FROM ICSF.T_DOCUMENT_MEMO\n",
    "WHERE NODOCUMENT = '", key$nodocument, "'\n",
    "  AND TYPEDOC = '", key$typedoc, "'"
  )

  out <- DBI::dbGetQuery(connection, sql)
  if (!nrow(out)) return(NA_character_)
  as.character(out$HEX[[1L]])
}

.cora_hex_to_raw <- function(hex) {
  if (is.na(hex) || !nzchar(hex)) return(raw())
  pos <- seq.int(1L, nchar(hex), by = 2L)
  as.raw(strtoi(substring(hex, pos, pos + 1L), base = 16L))
}

.cora_decode_gzip <- function(compressed, nodocument = NULL) {
  label <- if (is.null(nodocument)) "CORA document" else
    paste0("CORA Diet document `", nodocument, "`")

  if (length(compressed) < 3L ||
      !identical(as.integer(compressed[1:3]), c(31L, 139L, 8L))) {
    stop(label, " does not contain the expected GZIP payload.", call. = FALSE)
  }

  decoded <- tryCatch(
    memDecompress(compressed, type = "gzip"),
    error = function(e) {
      stop(label, " could not be decompressed: ", conditionMessage(e),
           call. = FALSE)
    }
  )

  text <- rawToChar(decoded)
  converted <- iconv(text, from = "latin1", to = "UTF-8")
  if (is.na(converted)) text else converted
}

.cora_read_diet_blob <- function(connection, nodocument, typedoc,
                                 chunk_size = 2000L) {
  len <- .cora_blob_length(connection, nodocument, typedoc)
  if (is.na(len) || len <= 0L) return(NA_character_)

  offsets <- seq.int(1L, len, by = chunk_size)
  chunks <- lapply(offsets, function(offset) {
    amount <- min(chunk_size, len - offset + 1L)
    .cora_hex_to_raw(
      .cora_blob_chunk_hex(
        connection,
        nodocument = nodocument,
        typedoc = typedoc,
        amount = amount,
        offset = offset
      )
    )
  })

  .cora_decode_gzip(do.call(c, chunks), nodocument = nodocument)
}

.cora_validate_diet_event_type <- function(event_type = c("H", "R")) {
  match.arg(event_type, choices = c("H", "R"), several.ok = TRUE)
}

.cora_diet_documents_sql <- function(ieps, event_type = c("H", "R")) {
  ieps <- .cora_validate_ieps(ieps)
  event_type <- .cora_validate_diet_event_type(event_type)
  quoted <- paste0("'", ieps, "'", collapse = ", ")

  parts <- character()

  if ("H" %in% event_type) {
    parts <- c(parts, paste0(
      "  SELECT DISTINCT\n",
      "    m.NOSEJ AS IEP,\n",
      "    d.NODOCUMENT,\n",
      "    d.TYPEDOC,\n",
      "    d.NOEVT AS CORA_NOEVT,\n",
      "    d.DTDOC,\n",
      "    d.TIMECREATE,\n",
      "    d.REDACTEUR,\n",
      "    m.NOUSHEB,\n",
      "    m.NOUSRESP\n",
      "  FROM ICSF.MVTUS m\n",
      "  JOIN ICSF.T_DOCUMENT d\n",
      "    ON d.NOEVT = m.NOMVTUS\n",
      "   AND d.TYPEEVT = 'H'\n",
      "  WHERE m.NOSEJ IN (", quoted, ")\n",
      "    AND d.NOSOUSVOLET = 443\n",
      "    AND d.ETATDOC = 1"
    ))
  }

  if ("R" %in% event_type) {
    parts <- c(parts, paste0(
      "  SELECT DISTINCT\n",
      "    r.NOSEJ AS IEP,\n",
      "    d.NODOCUMENT,\n",
      "    d.TYPEDOC,\n",
      "    d.NOEVT AS CORA_NOEVT,\n",
      "    d.DTDOC,\n",
      "    d.TIMECREATE,\n",
      "    d.REDACTEUR,\n",
      "    CAST(NULL AS VARCHAR2(10)) AS NOUSHEB,\n",
      "    CAST(NULL AS VARCHAR2(10)) AS NOUSRESP\n",
      "  FROM ICSF.T_EVT_RDV r\n",
      "  JOIN ICSF.T_DOCUMENT d\n",
      "    ON d.NOEVT = r.NOEVT\n",
      "   AND d.TYPEEVT = 'R'\n",
      "  WHERE r.NOSEJ IN (", quoted, ")\n",
      "    AND d.NOSOUSVOLET = 443\n",
      "    AND d.ETATDOC = 1"
    ))
  }

  paste0(
    "SELECT *\n",
    "FROM (\n",
    paste(parts, collapse = "\n\n  UNION ALL\n\n"),
    "\n)\n",
    "ORDER BY IEP, DTDOC"
  )
}


.cora_query_diet_documents <- function(connection, ieps,
                                       event_type = c("H", "R"),
                                       query_fn = DBI::dbGetQuery) {
  ieps <- .cora_validate_ieps(ieps)
  event_type <- .cora_validate_diet_event_type(event_type)
  if (!length(ieps)) return(tibble::tibble())

  # Oracle allows at most 1,000 expressions in an IN list. Keep each request
  # below that limit so large CORA lookups work without caller-side batching.
  batch_size <- 900L
  batch_index <- (seq_along(ieps) - 1L) %/% batch_size
  batches <- split(ieps, batch_index)

  results <- lapply(batches, function(batch_ieps) {
    query_fn(
      connection,
      .cora_diet_documents_sql(batch_ieps, event_type = event_type)
    )
  })
  documents <- dplyr::bind_rows(results)

  # Each SQL query sorts its own batch. Reapply the same keys after binding so
  # rows from different batches retain the original global query order.
  if (nrow(documents)) {
    documents <- documents[
      order(documents$IEP, documents$DTDOC, na.last = TRUE),
      ,
      drop = FALSE
    ]
  }

  documents
}


#' Retrieve CORA dietary observations (legacy interface)
#'
#' Deprecated wrapper for [cora_get_diet()] preserving the historical
#' 11-column result.
#' @inheritParams cora_get_diet
#' @return A tibble with `EVTID`, `IEP`, `NODOCUMENT`, `TYPEDOC`,
#'   `CORA_NOEVT`, `DTDOC`, `TIMECREATE`, `REDACTEUR`, `NOUSHEB`,
#'   `NOUSRESP`, and `TEXT`.
#' @export
get_cora_diet <- function(ids, id_type = c("IEP", "EVTID"),
                          event_type = c("H", "R"),
                          connection = NULL, ojdbc_jar = NULL,
                          chunk_size = 2000L,
                          env = "edsan-ct", ks_path = NULL) {
  .redsan_deprecate("get_cora_diet", "cora_get_diet")
  out <- cora_get_diet(
    ids, id_type = id_type, event_type = event_type,
    connection = connection, ojdbc_jar = ojdbc_jar,
    chunk_size = chunk_size, env = env, ks_path = ks_path
  )
  out[, names(.cora_empty_diet()), drop = FALSE]
}
