# Extend CORA Diet retrieval to RDV-linked documents -------------------------
#
# CORA Diet documents are stored in T_DOCUMENT/T_DOCUMENT_MEMO regardless of
# event type, but the route from an IEP to T_DOCUMENT differs:
# - H (hospitalisation): MVTUS.NOSEJ -> MVTUS.NOMVTUS = T_DOCUMENT.NOEVT
# - R (rendez-vous):     T_EVT_RDV.NOSEJ -> T_EVT_RDV.NOEVT = T_DOCUMENT.NOEVT
#
# This file is intentionally collated after cora-diet.R. It replaces the
# H-only query helper and extends get_cora_diet() with an event-type filter.

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
                                       event_type = c("H", "R")) {
  DBI::dbGetQuery(
    connection,
    .cora_diet_documents_sql(ieps, event_type = event_type)
  )
}

# Override the original H-only public implementation after all helpers above
# have been loaded. The default remains inclusive: both H and R are queried.
get_cora_diet <- function(ids, id_type = c("IEP", "EVTID"),
                          event_type = c("H", "R"),
                          connection = NULL, ojdbc_jar = NULL,
                          chunk_size = 2000L,
                          env = "edsan-ct", ks_path = NULL) {
  id_type <- match.arg(id_type)
  event_type <- .cora_validate_diet_event_type(event_type)
  ids <- .cora_validate_stay_ids(ids, id_type)
  if (!length(ids)) return(.cora_empty_diet())

  if (length(chunk_size) != 1L || is.na(chunk_size) ||
      chunk_size < 1L || chunk_size > 2000L) {
    stop("`chunk_size` must be a single integer between 1 and 2000.",
         call. = FALSE)
  }
  chunk_size <- as.integer(chunk_size)

  mapping <- .cora_diet_id_map(
    ids,
    id_type = id_type,
    env = env,
    ks_path = ks_path
  )
  query_ieps <- unique(mapping$IEP[!is.na(mapping$IEP) & nzchar(mapping$IEP)])
  if (!length(query_ieps)) return(.cora_empty_diet())

  .cora_require_namespace("DBI", "query execution")

  owns_connection <- is.null(connection)
  if (owns_connection) {
    connection <- .cora_connect(ojdbc_jar = ojdbc_jar)
    on.exit(DBI::dbDisconnect(connection), add = TRUE)
  }

  docs <- .cora_query_diet_documents(
    connection,
    query_ieps,
    event_type = event_type
  )
  if (!nrow(docs)) return(.cora_empty_diet())

  docs <- tibble::as_tibble(docs)
  docs$IEP <- trimws(as.character(docs$IEP))
  docs$NODOCUMENT <- trimws(as.character(docs$NODOCUMENT))
  docs$TYPEDOC <- trimws(as.character(docs$TYPEDOC))
  docs$CORA_NOEVT <- trimws(as.character(docs$CORA_NOEVT))
  docs$REDACTEUR <- trimws(as.character(docs$REDACTEUR))
  docs$NOUSHEB <- trimws(as.character(docs$NOUSHEB))
  docs$NOUSRESP <- trimws(as.character(docs$NOUSRESP))

  docs <- dplyr::left_join(docs, mapping, by = "IEP")
  docs <- docs[, c(
    "EVTID", "IEP", "NODOCUMENT", "TYPEDOC", "CORA_NOEVT",
    "DTDOC", "TIMECREATE", "REDACTEUR", "NOUSHEB", "NOUSRESP"
  ), drop = FALSE]

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

  tibble::as_tibble(docs)
}
