# EDSaN CT identifier correspondence ----------------------------------------

.edsan_ct_specs <- list(
  his_to_edsan = list(
    endpoint = "getHISToEDSaNCorrespondences",
    types = list(
      IPP = list(api_type = "NIP", output_type = "PATID"),
      IEP = list(api_type = "CPAGE", output_type = "EVTID")
    )
  ),
  edsan_to_his = list(
    endpoint = "getEDSaNToHISCorrespondences",
    types = list(
      PATID = list(api_type = "NIP", output_type = "IPP"),
      EVTID = list(api_type = "CPAGE", output_type = "IEP")
    )
  )
)

.edsan_ct_validate_ids <- function(ids, require_character = FALSE,
                                   digits_only = FALSE) {
  if (require_character && !is.character(ids)) {
    stop(
      "`ids` must be character. Automatic IPP/IEP detection depends on the ",
      "leading zeroes of IPP values.",
      call. = FALSE
    )
  }
  if (!(is.character(ids) || is.numeric(ids)) || length(ids) == 0L) {
    stop("`ids` must contain one or more identifiers.", call. = FALSE)
  }

  ids <- trimws(.edsan_as_identifier(ids))
  if (anyNA(ids) || any(!nzchar(ids))) {
    stop("`ids` must not contain missing or empty identifiers.", call. = FALSE)
  }
  if (digits_only && any(!grepl("^[0-9]+$", ids))) {
    stop("`ids` must contain digit strings only.", call. = FALSE)
  }
  ids
}

.edsan_ct_detect_his_types <- function(ids) {
  ids <- .edsan_ct_validate_ids(ids, require_character = TRUE, digits_only = TRUE)
  ifelse(startsWith(ids, "00"), "IPP", "IEP")
}

.edsan_ct_validate_explicit_his_type <- function(ids, id_type) {
  id_type <- match.arg(id_type, c("IPP", "IEP"))
  detected <- .edsan_ct_detect_his_types(ids)
  if (any(detected != id_type)) {
    stop(
      "`id_type = \"", id_type,
      "\"` conflicts with the local format: IPP values start with `00`; ",
      "IEP values do not.",
      call. = FALSE
    )
  }
  rep(id_type, length(ids))
}

.edsan_ct_backend <- function() {
  call_fn <- tryCatch(getExportedValue("d2imr", "call_edsan_ws"),
                      error = function(e) NULL)
  path_fn <- tryCatch(getExportedValue("d2imr", "get_activ_keystore_path"),
                      error = function(e) NULL)
  if (!is.function(call_fn) || !is.function(path_fn)) {
    stop(
      "Package `d2imr` with exported `call_edsan_ws()` and ",
      "`get_activ_keystore_path()` is required for EDSaN CT correspondence.",
      call. = FALSE
    )
  }
  list(call = call_fn, active_keystore = path_fn)
}

.edsan_ct_call <- function(api_fct, api_type, api_query,
                           env = "edsan-ct", ks_path = NULL) {
  backend <- .edsan_ct_backend()
  if (is.null(ks_path)) ks_path <- backend$active_keystore()
  if (!is.character(ks_path) || length(ks_path) != 1L || is.na(ks_path) ||
      !nzchar(ks_path)) {
    stop(
      "No active d2imr keystore path is available. Supply `ks_path` explicitly ",
      "or activate a keystore with `d2imr::set_activ_keystore_path()`.",
      call. = FALSE
    )
  }
  backend$call(
    api_fct = api_fct,
    api_mod = api_type,
    api_query = api_query,
    env = env,
    ks_path = ks_path
  )
}

.edsan_ct_parse_values <- function(response, input_id, api_type) {
  if (is.null(response) || length(response) == 0L) return(character())
  if (!is.list(response)) {
    stop("EDSaN CT returned an unexpected non-list response.", call. = FALSE)
  }

  response_names <- names(response)
  if (!is.null(response_names) && input_id %in% response_names) {
    node <- response[[input_id]]
  } else if (length(response) == 1L &&
             (is.null(response_names) || is.na(response_names[[1L]]) ||
              !nzchar(response_names[[1L]]))) {
    node <- response[[1L]]
  } else {
    return(character())
  }

  if (is.list(node) && !is.null(names(node)) && api_type %in% names(node)) {
    value <- node[[api_type]]
  } else if (!is.list(node) || is.null(names(node))) {
    value <- node
  } else {
    stop(
      "EDSaN CT response for identifier `", input_id,
      "` lacks the expected `", api_type, "` field.",
      call. = FALSE
    )
  }

  values <- as.character(unlist(value, recursive = TRUE, use.names = FALSE))
  unique(values[!is.na(values) & nzchar(values)])
}

.edsan_ct_translate <- function(ids, input_types, direction,
                                env = "edsan-ct", ks_path = NULL,
                                call = .edsan_ct_call) {
  direction <- match.arg(direction, names(.edsan_ct_specs))
  spec <- .edsan_ct_specs[[direction]]
  if (length(ids) != length(input_types)) {
    stop("Internal error: `ids` and `input_types` must have equal length.",
         call. = FALSE)
  }

  rows <- vector("list", length(ids))
  for (i in seq_along(ids)) {
    type_spec <- spec$types[[input_types[[i]]]]
    if (is.null(type_spec)) {
      stop("Unsupported identifier type: ", input_types[[i]], call. = FALSE)
    }

    response <- call(
      api_fct = spec$endpoint,
      api_type = type_spec$api_type,
      api_query = ids[[i]],
      env = env,
      ks_path = ks_path
    )
    values <- .edsan_ct_parse_values(response, ids[[i]], type_spec$api_type)

    if (!length(values)) {
      rows[[i]] <- tibble::tibble(
        input_index = i, input_id = ids[[i]], input_type = input_types[[i]],
        output_id = NA_character_, output_type = type_spec$output_type,
        status = "not_found", n_matches = 0L
      )
    } else {
      status <- if (length(values) == 1L) "matched" else "multiple_matches"
      rows[[i]] <- tibble::tibble(
        input_index = rep.int(i, length(values)),
        input_id = rep.int(ids[[i]], length(values)),
        input_type = rep.int(input_types[[i]], length(values)),
        output_id = values,
        output_type = rep.int(type_spec$output_type, length(values)),
        status = rep.int(status, length(values)),
        n_matches = rep.int(length(values), length(values))
      )
    }
  }
  dplyr::bind_rows(rows)
}

#' Translate real hospital identifiers to EDSaN identifiers
#'
#' Uses EDSaN CT to translate patient identifiers (`IPP`) to `PATID` and stay
#' identifiers (`IEP`) to `EVTID`. When `id_type` is omitted, each identifier is
#' classified from the local invariant: IPP starts with `00`; every other digit
#' string is treated as IEP.
#'
#' @param ids Real hospital identifiers. Must be character so IPP leading zeroes
#'   cannot be lost.
#' @param id_type Optional explicit input type: `"IPP"` or `"IEP"`.
#' @param env EDSaN CT web-service environment name.
#' @param ks_path Optional d2imr keystore path. When `NULL`, the active path is
#'   resolved and passed explicitly, avoiding the invalid default in some d2imr
#'   versions.
#' @return A tibble retaining every input and reporting matched, not-found, or
#'   multiple correspondences.
#' @export
edsan_pseudonymize <- function(ids, id_type = NULL,
                               env = "edsan-ct", ks_path = NULL) {
  ids <- .edsan_ct_validate_ids(ids, require_character = TRUE, digits_only = TRUE)
  input_types <- if (is.null(id_type)) {
    .edsan_ct_detect_his_types(ids)
  } else {
    .edsan_ct_validate_explicit_his_type(ids, id_type)
  }

  out <- .edsan_ct_translate(ids, input_types, "his_to_edsan", env, ks_path)
  dplyr::transmute(
    out,
    HIS_ID = input_id, HIS_TYPE = input_type,
    EDSAN_ID = output_id, EDSAN_TYPE = output_type,
    status = status, n_matches = n_matches
  )
}

#' Reidentify EDSaN identifiers through EDSaN CT
#'
#' Translates `PATID` to `IPP` or `EVTID` to `IEP`. The pseudonymized domains
#' cannot be distinguished safely from the value alone, so `id_type` is required.
#'
#' @param ids EDSaN `PATID` or `EVTID` values.
#' @param id_type Input type: `"PATID"` or `"EVTID"`.
#' @inheritParams edsan_pseudonymize
#' @return A tibble retaining every input and reporting matched, not-found, or
#'   multiple correspondences.
#' @details This function deliberately exposes real hospital identifiers.
#' @export
edsan_reidentify <- function(ids, id_type,
                             env = "edsan-ct", ks_path = NULL) {
  ids <- .edsan_ct_validate_ids(ids)
  id_type <- match.arg(id_type, c("PATID", "EVTID"))
  out <- .edsan_ct_translate(
    ids, rep.int(id_type, length(ids)), "edsan_to_his", env, ks_path
  )
  dplyr::transmute(
    out,
    EDSAN_ID = input_id, EDSAN_TYPE = input_type,
    HIS_ID = output_id, HIS_TYPE = output_type,
    status = status, n_matches = n_matches
  )
}
