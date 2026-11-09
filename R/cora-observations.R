# CORA observation catalog and retrieval ------------------------------------

.cora_observation_event_types <- function(event_type = c("H", "R")) {
  match.arg(event_type, choices = c("H", "R"), several.ok = TRUE)
}

.cora_empty_observations <- function() {
  out <- .cora_empty_diet()
  out$NOSOUSVOLET <- character()
  out$OBS_CODE <- character()
  out$OBS_LABEL <- character()
  out$TYPEEVT <- character()
  out[, c("EVTID", "IEP", "NODOCUMENT", "TYPEDOC", "CORA_NOEVT",
          "NOSOUSVOLET", "OBS_CODE", "OBS_LABEL", "TYPEEVT",
          "DTDOC", "TIMECREATE", "REDACTEUR", "NOUSHEB", "NOUSRESP", "TEXT")]
}

#' List the CORA observations sub-volets
#'
#' Reads the live CORA reference table rather than maintaining a local list.
#' Observations belong to CORA volet 53 (MCK_OBSERV). Inactive categories
#' (ETAT != "A") are omitted unless requested explicitly.
#' @param connection Existing DBI connection, or NULL to connect temporarily.
#' @param ojdbc_jar Optional Oracle driver path.
#' @param active_only Whether to return only active categories.
#' @return A tibble with NOSOUSVOLET, CODE, LIBCOURT, LIBLONG, ETAT and volet fields.
#' @export
cora_observation_catalog <- function(connection = NULL, ojdbc_jar = NULL,
                                     active_only = TRUE) {
  if (!is.logical(active_only) || length(active_only) != 1L ||
      is.na(active_only)) {
    stop("`active_only` must be TRUE or FALSE.", call. = FALSE)
  }
  sql <- paste0(
    "SELECT s.NOSOUSVOLET, s.CODE, s.LIBCOURT, s.LIBLONG, s.ETAT, ",
    "v.CODE AS VOLET_CODE, v.LIBCOURT AS VOLET_LABEL ",
    "FROM ICSF.T_SOUS_VOLET s ",
    "JOIN ICSF.T_VOLET v ON s.NOVOLET = v.NOVOLET ",
    "WHERE s.NOVOLET = 53",
    if (active_only) " AND s.ETAT = 'A'" else "",
    " ORDER BY s.LIBCOURT"
  )
  .cora_query(sql, connection = connection, ojdbc_jar = ojdbc_jar)
}

.cora_resolve_observation_types <- function(types, catalog) {
  if (!is.character(types) || !length(types) || anyNA(types) ||
      any(!nzchar(types))) {
    stop("`types` must be 'all' or non-empty CORA observation codes.",
         call. = FALSE)
  }
  if (!is.data.frame(catalog) ||
      !all(c("CODE", "NOSOUSVOLET") %in% names(catalog))) {
    stop("Invalid CORA observation catalog response.", call. = FALSE)
  }
  if (identical(types, "all")) {
    chosen <- catalog
  } else {
    if ("all" %in% types) {
      stop("Use 'all' alone or provide explicit CORA codes.", call. = FALSE)
    }
    types <- unique(types)
    unknown <- setdiff(types, as.character(catalog$CODE))
    if (length(unknown)) {
      stop("Unknown or inactive CORA observation type(s): ",
           paste(unknown, collapse = ", "), call. = FALSE)
    }
    chosen <- catalog[as.character(catalog$CODE) %in% types, , drop = FALSE]
  }
  ids <- trimws(as.character(chosen$NOSOUSVOLET))
  if (anyNA(ids) || any(!grepl("^[0-9]+$", ids))) {
    stop("Invalid NOSOUSVOLET value in CORA catalog.", call. = FALSE)
  }
  chosen$NOSOUSVOLET <- ids
  chosen
}

.cora_observation_documents_sql <- function(ieps, subvolets,
                                            event_type = c("H", "R")) {
  ieps <- .cora_validate_ieps(ieps)
  event_type <- .cora_observation_event_types(event_type)
  subvolets <- trimws(as.character(subvolets))
  if (!length(ieps) || !length(subvolets) || anyNA(subvolets) ||
      any(!grepl("^[0-9]+$", subvolets))) {
    stop("IEPs and numeric CORA sub-volet IDs are required.", call. = FALSE)
  }
  quoted <- paste0("'", ieps, "'", collapse = ", ")
  in_subvolets <- paste(unique(subvolets), collapse = ", ")
  parts <- lapply(event_type, function(ev) {
    is_h <- identical(ev, "H")
    from <- if (is_h) "ICSF.MVTUS m" else "ICSF.T_EVT_RDV r"
    alias <- if (is_h) "m" else "r"
    key <- if (is_h) "m.NOMVTUS" else "r.NOEVT"
    paste0(
      "SELECT DISTINCT ", alias, ".NOSEJ AS IEP, ",
      "d.NODOCUMENT, d.TYPEDOC, d.NOEVT AS CORA_NOEVT, ",
      "d.NOSOUSVOLET, d.TYPEEVT, d.DTDOC, d.TIMECREATE, d.REDACTEUR, ",
      if (is_h) "m.NOUSHEB, m.NOUSRESP " else
        "CAST(NULL AS VARCHAR2(10)) AS NOUSHEB, CAST(NULL AS VARCHAR2(10)) AS NOUSRESP ",
      "FROM ", from, " JOIN ICSF.T_DOCUMENT d ",
      "ON d.NOEVT = ", key, " AND d.TYPEEVT = '", ev, "' ",
      "WHERE ", alias, ".NOSEJ IN (", quoted, ") ",
      "AND d.NOSOUSVOLET IN (", in_subvolets, ") AND d.ETATDOC = 1"
    )
  })
  paste0("SELECT * FROM (", paste(unlist(parts), collapse = " UNION ALL "),
         ") ORDER BY IEP, DTDOC")
}

.cora_query_observation_documents <- function(connection, ieps, subvolets,
                                              event_type = c("H", "R"),
                                              query_fn = DBI::dbGetQuery) {
  ieps <- .cora_validate_ieps(ieps)
  if (!length(ieps)) return(tibble::tibble())
  batches <- split(ieps, (seq_along(ieps) - 1L) %/% 900L)
  docs <- dplyr::bind_rows(lapply(batches, function(batch) {
    query_fn(connection, .cora_observation_documents_sql(
      batch, subvolets, event_type = event_type
    ))
  }))
  if (nrow(docs)) {
    docs <- docs[order(as.character(docs$IEP), docs$DTDOC,
                       na.last = TRUE), , drop = FALSE]
  }
  tibble::as_tibble(docs)
}

#' Retrieve CORA observations linked to hospital stays
#'
#' Uses the active CORA observation reference, EDSaN CT for the EVTID/IEP
#' correspondence, and indexed hospital (H) / rendez-vous (R) joins.
#' @param ids Character IEP or EVTID identifiers.
#' @param id_type "IEP" (default) or "EVTID".
#' @param types CORA T_SOUS_VOLET.CODE values, or "all" for all active codes.
#' @param event_type "H", "R" or both (default).
#' @param connection Optional existing CORA DBI connection.
#' @param ojdbc_jar Optional Oracle JDBC driver.
#' @param chunk_size Oracle BLOB bytes per chunk, from 1 to 2000.
#' @param env EDSaN CT service environment.
#' @param ks_path Optional EDSaN CT keystore path.
#' @return A tibble containing raw text in TEXT, a CORA document key
#'   (NODOCUMENT, TYPEDOC), provenance and both stay identifiers.
#'   For an IEP absent from EDSaN CT, EVTID is NA; such rows cannot
#'   be attached to a bundle. Service failures raise errors.
#' @export
cora_get_observations <- function(ids, id_type = c("IEP", "EVTID"),
                                  types = "all", event_type = c("H", "R"),
                                  connection = NULL, ojdbc_jar = NULL,
                                  chunk_size = 2000L,
                                  env = "edsan-ct", ks_path = NULL) {
  id_type <- match.arg(id_type)
  event_type <- .cora_observation_event_types(event_type)
  ids <- .cora_validate_stay_ids(ids, id_type)
  if (!length(ids)) return(.cora_empty_observations())
  if (length(chunk_size) != 1L || !is.numeric(chunk_size) ||
      is.na(chunk_size) || chunk_size != floor(chunk_size) ||
      chunk_size < 1L || chunk_size > 2000L) {
    stop("`chunk_size` must be an integer from 1 to 2000.", call. = FALSE)
  }
  mapping <- .cora_diet_id_map(ids, id_type = id_type, env = env,
                               ks_path = ks_path)
  query_ieps <- unique(mapping$IEP[!is.na(mapping$IEP) & nzchar(mapping$IEP)])
  if (!length(query_ieps)) return(.cora_empty_observations())

  # Multiple EDSaN IDs for an IEP are not safe to attach arbitrarily.
  matched <- mapping[!is.na(mapping$EVTID) & !is.na(mapping$IEP), ,
                     drop = FALSE]
  if (anyDuplicated(matched$IEP)) {
    groups <- split(matched$EVTID, matched$IEP)
    if (any(vapply(groups, function(x) length(unique(x)) > 1L, logical(1)))) {
      stop("Ambiguous EDSaN EVTID/IEP correspondence.", call. = FALSE)
    }
  }

  .cora_require_namespace("DBI", "query execution")
  owns_connection <- is.null(connection)
  if (owns_connection) {
    connection <- .cora_connect(ojdbc_jar = ojdbc_jar, ks_path = ks_path)
    on.exit(DBI::dbDisconnect(connection), add = TRUE)
  }
  catalog <- cora_observation_catalog(connection = connection)
  selected <- .cora_resolve_observation_types(types, catalog)
  if (!nrow(selected)) return(.cora_empty_observations())

  docs <- .cora_query_observation_documents(
    connection, query_ieps, selected$NOSOUSVOLET, event_type = event_type
  )
  if (!nrow(docs)) return(.cora_empty_observations())
  expected <- c("IEP", "NODOCUMENT", "TYPEDOC", "CORA_NOEVT",
                "NOSOUSVOLET", "TYPEEVT", "DTDOC", "TIMECREATE",
                "REDACTEUR", "NOUSHEB", "NOUSRESP")
  if (!all(expected %in% names(docs))) {
    stop("CORA observations query returned an incomplete schema.",
         call. = FALSE)
  }
  for (name in c("IEP", "NODOCUMENT", "TYPEDOC", "CORA_NOEVT",
                 "NOSOUSVOLET", "TYPEEVT", "REDACTEUR",
                 "NOUSHEB", "NOUSRESP")) {
    docs[[name]] <- as.character(docs[[name]])
  }
  selected$CODE <- as.character(selected$CODE)
  selected$LIBLONG <- as.character(selected$LIBLONG)
  docs <- dplyr::left_join(
    docs, selected[, c("NOSOUSVOLET", "CODE", "LIBLONG")],
    by = "NOSOUSVOLET"
  )
  names(docs)[names(docs) == "CODE"] <- "OBS_CODE"
  names(docs)[names(docs) == "LIBLONG"] <- "OBS_LABEL"
  docs <- dplyr::left_join(docs, unique(mapping), by = "IEP")
  if (identical(id_type, "EVTID")) {
    docs <- docs[!is.na(docs$EVTID) & docs$EVTID %in% ids, , drop = FALSE]
  }
  if (!nrow(docs)) return(.cora_empty_observations())

  # A CORA document is uniquely identified by (TYPEDOC, NODOCUMENT).
  keys <- paste(docs$TYPEDOC, docs$NODOCUMENT, sep = ":")
  unique_idx <- !duplicated(keys)
  decoded <- vapply(which(unique_idx), function(i) {
    .cora_read_diet_blob(connection, docs$NODOCUMENT[[i]],
                         docs$TYPEDOC[[i]], chunk_size = as.integer(chunk_size))
  }, character(1))
  docs$TEXT <- unname(decoded[match(keys, keys[unique_idx])])
  wanted <- names(.cora_empty_observations())
  tibble::as_tibble(docs[, wanted, drop = FALSE])
}

#' Retrieve CORA dietary observations
#'
#' Convenience wrapper for cora_get_observations(types = "DIET").
#' @inheritParams cora_get_observations
#' @return A CORA observations tibble with the enriched provenance columns.
#' @export
cora_get_diet <- function(ids, id_type = c("IEP", "EVTID"),
                          event_type = c("H", "R"),
                          connection = NULL, ojdbc_jar = NULL,
                          chunk_size = 2000L,
                          env = "edsan-ct", ks_path = NULL) {
  cora_get_observations(
    ids = ids, id_type = id_type, types = "DIET",
    event_type = event_type, connection = connection, ojdbc_jar = ojdbc_jar,
    chunk_size = chunk_size, env = env, ks_path = ks_path
  )
}
