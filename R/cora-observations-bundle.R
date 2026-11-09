# Attach CORA observations to EDSaN DOCEDS sources --------------------------

.cora_observations_doceds <- function(observations) {
  required <- c("EVTID", "NODOCUMENT", "TYPEDOC", "TEXT",
                "OBS_CODE", "NOSOUSVOLET", "TYPEEVT", "CORA_NOEVT")
  if (!is.data.frame(observations) ||
      !all(required %in% names(observations))) {
    stop("Observations must be a CORA observations data frame with provenance.",
         call. = FALSE)
  }
  if (!nrow(observations)) return(tibble::tibble(
    EVTID = character(), ELTID = character(), RECTXT = character(),
    RECTYPE = character(), RECTYPE_LABEL = character(), SOURCE = character(),
    NODOCUMENT = character(), TYPEDOC = character(),
    NOSOUSVOLET = character(), OBS_CODE = character(), TYPEEVT = character(),
    CORA_NOEVT = character()
  ))
  obs <- tibble::as_tibble(observations)
  if (anyNA(obs$EVTID) || any(!nzchar(as.character(obs$EVTID)))) {
    stop("CORA observations without EVTID cannot be attached to bundles.",
         call. = FALSE)
  }
  if (anyNA(obs$NODOCUMENT) || anyNA(obs$TYPEDOC)) {
    stop("CORA document keys cannot be missing.", call. = FALSE)
  }
  # Namespaced, composite identifier prevents CORA/EDSaN and type collisions.
  key <- paste0("CORA:", obs$TYPEDOC, ":", obs$NODOCUMENT)
  if (anyDuplicated(paste(obs$EVTID, key, sep = "|"))) {
    # Same document reachable through H/R or several movements: keep one
    # observation row per EVTID and original CORA document key.
    obs <- obs[!duplicated(paste(obs$EVTID, key, sep = "|")), , drop = FALSE]
    key <- paste0("CORA:", obs$TYPEDOC, ":", obs$NODOCUMENT)
  }
  out <- obs
  out$EVTID <- as.character(obs$EVTID)
  out$ELTID <- key
  out$RECTXT <- as.character(obs$TEXT)
  out$RECTYPE <- ifelse(obs$OBS_CODE == "DIET", "FICHE_DIET",
                        paste0("CORA_", obs$OBS_CODE))
  out$RECTYPE_LABEL <- if ("OBS_LABEL" %in% names(obs)) {
    as.character(obs$OBS_LABEL)
  } else rep(NA_character_, nrow(obs))
  out$SOURCE <- "CORA"
  if ("DTDOC" %in% names(obs)) out$RECDATE <- .pmsi_parse_datetime(obs$DTDOC)
  if ("IEP" %in% names(obs)) out$IEP <- as.character(obs$IEP)
  out$TEXT <- NULL
  out
}

#' Add CORA observations to bundles in memory
#'
#' Converts CORA observations into DOCEDS-compatible rows. Existing EDSaN
#' DOCEDS rows are unchanged. CORA rows retain their original identifiers,
#' category and event type; ELTID is namespaced by source and compound key.
#' Repeated calls with the same data are idempotent.
#' @param bundles A single edsan_event_bundle or named list of such bundles.
#' @param observations Result of cora_get_observations().
#' @return Bundles with merged sources$doceds tables.
#' @export
add_observations <- function(bundles, observations) {
  single <- inherits(bundles, "edsan_event_bundle")
  if (single) bundles <- list(bundles)
  if (!is.list(bundles) ||
      !all(vapply(bundles, inherits, logical(1), "edsan_event_bundle"))) {
    stop("`bundles` must contain edsan_event_bundle objects.", call. = FALSE)
  }
  if (!length(bundles)) return(if (single) bundles[[1L]] else bundles)
  extra <- .cora_observations_doceds(observations)
  for (i in seq_along(bundles)) {
    bundle <- bundles[[i]]
    id <- as.character(bundle$event_id)
    added <- extra[extra$EVTID == id, , drop = FALSE]
    existing <- bundle$sources$doceds
    if (is.null(existing)) {
      existing <- added[FALSE, , drop = FALSE]
    }
    if (!is.data.frame(existing)) {
      stop("Existing doceds source must be a data frame.", call. = FALSE)
    }
    if (nrow(existing) && !"ELTID" %in% names(existing)) {
      stop("Existing doceds table is missing ELTID.", call. = FALSE)
    }
    if (nrow(existing) && "ELTID" %in% names(existing)) {
      same <- as.character(existing$ELTID) %in% as.character(added$ELTID)
      if ("SOURCE" %in% names(existing)) {
        # Never quietly overwrite a pre-existing EDSaN row.
        collision <- same & (is.na(existing$SOURCE) | existing$SOURCE != "CORA")
      } else {
        collision <- same
      }
      if (any(collision)) {
        stop("CORA/EDSaN document ELTID collision.", call. = FALSE)
      }
      added <- added[!added$ELTID %in% as.character(existing$ELTID),
                     , drop = FALSE]
    }
    if (nrow(added)) {
      # Reuse patient context from an already-normalized EDSaN document when
      # available. CORA rows stay valid even when no EDSaN document exists.
      for (column in c("PATID", "PATAGE", "PATSEX", "PATBD")) {
        if (column %in% names(existing) && !column %in% names(added) &&
            nrow(existing)) {
          observed <- existing[[column]]
          idx <- which(!is.na(observed))
          # One EVTID can have multiple PATID values in EDSaN: do not
          # invent a patient identity when the native data are ambiguous.
          if (length(idx) && length(unique(as.character(observed[idx]))) == 1L) {
            added[[column]] <- rep(observed[idx[[1L]]], nrow(added))
          }
        }
      }
      # Existing EDSaN rows keep their data and provenance; the bind adds only
      # new columns required to carry CORA metadata.
      bundle$sources$doceds <- dplyr::bind_rows(existing, added)
    } else if (is.null(bundle$sources$doceds)) {
      bundle$sources$doceds <- existing
    }
    bundles[[i]] <- bundle
  }
  if (single) bundles[[1L]] else bundles
}
