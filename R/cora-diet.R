# CORA diet observation retrieval -------------------------------------------

.cora_require_namespace <- function(package, purpose) {
  if (!requireNamespace(package, quietly = TRUE)) {
    stop(
      "CORA ", purpose, " requires the optional package `", package, "`.",
      call. = FALSE
    )
  }
}

.cora_keystore_value <- function(key) {
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
    getter(key),
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

.cora_connect <- function(ojdbc_jar = NULL) {
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
    .cora_keystore_value("db.cora.url")
  )

  tryCatch(
    DBI::dbConnect(
      drv,
      jdbc_url,
      .cora_keystore_value("db.cora.usr"),
      .cora_keystore_value("db.cora.pwd")
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

.cora_empty_diet <- function() {
  tibble::tibble(
    IEP = character(),
    NODOCUMENT = character(),
    TYPEDOC = character(),
    NOEVT = character(),
    DTDOC = as.POSIXct(character()),
    TIMECREATE = as.POSIXct(character()),
    REDACTEUR = character(),
    NOUSHEB = character(),
    NOUSRESP = character(),
    TEXT = character()
  )
}

.cora_query_diet_documents <- function(connection, ieps) {
  placeholders <- paste(rep("?", length(ieps)), collapse = ", ")

  sql <- paste0(
    "SELECT DISTINCT\n",
    "  m.NOSEJ AS IEP,\n",
    "  d.NODOCUMENT,\n",
    "  d.TYPEDOC,\n",
    "  d.NOEVT,\n",
    "  d.DTDOC,\n",
    "  d.TIMECREATE,\n",
    "  d.REDACTEUR,\n",
    "  m.NOUSHEB,\n",
    "  m.NOUSRESP\n",
    "FROM ICSF.MVTUS m\n",
    "JOIN ICSF.T_DOCUMENT d\n",
    "  ON d.NOEVT = m.NOMVTUS\n",
    " AND d.TYPEEVT = 'H'\n",
    "WHERE m.NOSEJ IN (", placeholders, ")\n",
    "  AND d.NOSOUSVOLET = 443\n",
    "  AND d.ETATDOC = 1\n",
    "ORDER BY m.NOSEJ, d.DTDOC"
  )

  DBI::dbGetQuery(connection, sql, params = unname(as.list(ieps)))
}

.cora_blob_length <- function(connection, nodocument, typedoc) {
  out <- DBI::dbGetQuery(
    connection,
    paste(
      "SELECT DBMS_LOB.GETLENGTH(DOCUMENT_BRUT) AS N",
      "FROM ICSF.T_DOCUMENT_MEMO",
      "WHERE NODOCUMENT = ? AND TYPEDOC = ?"
    ),
    params = list(nodocument, typedoc)
  )

  if (!nrow(out)) return(NA_integer_)
  as.integer(out$N[[1L]])
}

.cora_blob_chunk_hex <- function(connection, nodocument, typedoc,
                                 amount, offset) {
  out <- DBI::dbGetQuery(
    connection,
    paste0(
      "SELECT RAWTOHEX(DBMS_LOB.SUBSTR(DOCUMENT_BRUT, ", amount, ", ", offset, ")) AS HEX\n",
      "FROM ICSF.T_DOCUMENT_MEMO\n",
      "WHERE NODOCUMENT = ? AND TYPEDOC = ?"
    ),
    params = list(nodocument, typedoc)
  )

  if (!nrow(out)) return(NA_character_)
  as.character(out$HEX[[1L]])
}

.cora_hex_to_raw <- function(hex) {
  if (is.na(hex) || !nzchar(hex)) return(raw())
  pos <- seq.int(1L, nchar(hex), by = 2L)
  as.raw(strtoi(substring(hex, pos, pos + 1L), base = 16L))
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

  compressed <- do.call(c, chunks)
  if (length(compressed) < 3L ||
      !identical(as.integer(compressed[1:3]), c(31L, 139L, 8L))) {
    stop(
      "CORA Diet document `", nodocument,
      "` does not contain the expected GZIP payload.",
      call. = FALSE
    )
  }

  decoded <- tryCatch(
    memDecompress(compressed, type = "gzip"),
    error = function(e) {
      stop(
        "Could not decompress CORA Diet document `", nodocument, "`: ",
        conditionMessage(e),
        call. = FALSE
      )
    }
  )

  text <- rawToChar(decoded)
  converted <- iconv(text, from = "latin1", to = "UTF-8")
  if (is.na(converted)) text else converted
}

#' Retrieve CORA Diet observations by IEP
#'
#' Retrieves the Diet observation documents associated with one or more CORA
#' hospital stays. In CORA, the IEP is stored as `MVTSEJ.NOSEJ`; Diet documents
#' are linked through `MVTUS.NOMVTUS` to `T_DOCUMENT.NOEVT` and identified by
#' `NOSOUSVOLET = 443`. The document payload is read from
#' `T_DOCUMENT_MEMO.DOCUMENT_BRUT`, decompressed from GZIP, and returned as text.
#'
#' @param ieps Character vector of IEP values.
#' @param connection Optional existing CORA DBI connection. When `NULL`,
#'   `redsan` opens a CORA JDBC connection and closes it before returning.
#' @param ojdbc_jar Optional path to an Oracle JDBC driver. Used only when
#'   `connection` is `NULL`.
#' @param chunk_size Maximum number of BLOB bytes requested per Oracle
#'   `DBMS_LOB.SUBSTR()` call.
#' @return A tibble with one row per active Diet document and a `TEXT` column
#'   containing the decompressed observation.
#' @details This function performs indexed lookups by IEP and document key; it
#'   does not scan `T_DOCUMENT` or `T_DOCUMENT_MEMO` for text.
#' @export
get_cora_diet <- function(ieps, connection = NULL, ojdbc_jar = NULL,
                          chunk_size = 2000L) {
  ieps <- .cora_validate_ieps(ieps)
  if (!length(ieps)) return(.cora_empty_diet())

  if (length(chunk_size) != 1L || is.na(chunk_size) ||
      chunk_size < 1L || chunk_size > 2000L) {
    stop("`chunk_size` must be a single integer between 1 and 2000.",
         call. = FALSE)
  }
  chunk_size <- as.integer(chunk_size)

  .cora_require_namespace("DBI", "query execution")

  owns_connection <- is.null(connection)
  if (owns_connection) {
    connection <- .cora_connect(ojdbc_jar = ojdbc_jar)
    on.exit(DBI::dbDisconnect(connection), add = TRUE)
  }

  docs <- .cora_query_diet_documents(connection, ieps)
  if (!nrow(docs)) return(.cora_empty_diet())

  docs <- tibble::as_tibble(docs)
  docs$IEP <- trimws(as.character(docs$IEP))
  docs$NODOCUMENT <- trimws(as.character(docs$NODOCUMENT))
  docs$TYPEDOC <- trimws(as.character(docs$TYPEDOC))
  docs$NOEVT <- trimws(as.character(docs$NOEVT))
  docs$REDACTEUR <- trimws(as.character(docs$REDACTEUR))
  docs$NOUSHEB <- trimws(as.character(docs$NOUSHEB))
  docs$NOUSRESP <- trimws(as.character(docs$NOUSRESP))

  docs$TEXT <- mapply(
    function(nodocument, typedoc) {
      .cora_read_diet_blob(
        connection,
        nodocument = nodocument,
        typedoc = typedoc,
        chunk_size = chunk_size
      )
    },
    docs$NODOCUMENT,
    docs$TYPEDOC,
    USE.NAMES = FALSE
  )

  docs
}
