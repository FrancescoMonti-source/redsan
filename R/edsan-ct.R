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
      "`ids` must be character. Numeric input can lose IPP leading zeroes ",
      "or alter identifier values through double-precision conversion.",
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
  mismatched <- detected != id_type
  if (any(mismatched)) {
    warning(
      "`id_type = \"", id_type, "\"` was supplied for ", sum(mismatched),
      " identifier(s) that do not match the local `00 = IPP` format ",
      "heuristic. Proceeding with the explicit `id_type`.",
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

  invoke <- function() {
    backend$call(
      api_fct = api_fct,
      api_mod = api_type,
      api_query = api_query,
      env = env,
      ks_path = ks_path
    )
  }

  if (!identical(env, "edsan-ct")) return(invoke())

  if (!requireNamespace("httr", quietly = TRUE)) {
    stop(
      "EDSaN CT access in the Podsan environment requires the optional package `httr`.",
      call. = FALSE
    )
  }

  httr::with_config(
    httr::config(ssl_verifyhost = 0L, timeout = 30),
    invoke()
  )
}

.edsan_ct_is_error_payload <- function(response) {
  nm <- names(response)
  if (is.null(nm) || !("status" %in% nm)) return(FALSE)
  status <- response[["status"]]
  is.numeric(status) && length(status) == 1L && !is.na(status) &&
    (status < 200 || status >= 300)
}

.edsan_ct_error_message <- function(response) {
  msg <- response[["message"]]
  if (is.null(msg)) msg <- response[["error"]]
  if (is.null(msg)) msg <- paste("HTTP status", response[["status"]])
  paste(as.character(msg), collapse = " ")
}

.edsan_ct_parse_one <- function(response, input_id, api_type) {
  response_names <- names(response)
  if (!is.null(response_names) && input_id %in% response_names) {
    node <- response[[input_id]]
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

.edsan_ct_rows_for_chunk <- function(response, ids, input_type, type_spec,
                                     input_indices) {
  if (is.null(response)) {
    stop(
      "EDSaN CT call failed for a batch of ", length(ids),
      " identifier(s) (no response from the backend). Check keystore, network, ",
      "and authentication rather than treating this as missing correspondence.",
      call. = FALSE
    )
  }
  if (!is.list(response)) {
    stop("EDSaN CT returned an unexpected non-list response.", call. = FALSE)
  }
  if (.edsan_ct_is_error_payload(response)) {
    stop(
      "EDSaN CT returned an error for an identifier batch: ",
      .edsan_ct_error_message(response),
      call. = FALSE
    )
  }

  response_names <- names(response)
  if (length(response) > 0L &&
      (is.null(response_names) || !any(response_names %in% ids))) {
    stop("EDSaN CT returned an unrecognized response shape for the identifier batch.",
         call. = FALSE)
  }

  rows <- vector("list", length(ids))
  for (j in seq_along(ids)) {
    values <- .edsan_ct_parse_one(response, ids[[j]], type_spec$api_type)
    if (!length(values)) {
      rows[[j]] <- tibble::tibble(
        input_index = input_indices[[j]],
        input_id = ids[[j]],
        input_type = input_type,
        output_id = NA_character_,
        output_type = type_spec$output_type,
        status = "not_found",
        n_matches = 0L
      )
    } else {
      status <- if (length(values) == 1L) "matched" else "multiple_matches"
      rows[[j]] <- tibble::tibble(
        input_index = rep.int(input_indices[[j]], length(values)),
        input_id = rep.int(ids[[j]], length(values)),
        input_type = rep.int(input_type, length(values)),
        output_id = values,
        output_type = rep.int(type_spec$output_type, length(values)),
        status = rep.int(status, length(values)),
        n_matches = rep.int(length(values), length(values))
      )
    }
  }
  dplyr::bind_rows(rows)
}

.edsan_ct_translate <- function(ids, input_types, direction,
                                env = "edsan-ct", ks_path = NULL,
                                max_in_ids = 3500L,
                                call = .edsan_ct_call) {
  direction <- match.arg(direction, names(.edsan_ct_specs))
  spec <- .edsan_ct_specs[[direction]]
  if (length(ids) != length(input_types)) {
    stop("Internal error: `ids` and `input_types` must have equal length.",
         call. = FALSE)
  }
  max_in_ids <- as.integer(max_in_ids)
  if (length(max_in_ids) != 1L || is.na(max_in_ids) || max_in_ids < 1L) {
    stop("`max_in_ids` must be a positive integer.", call. = FALSE)
  }

  rows <- list()
  for (input_type in unique(input_types)) {
    type_spec <- spec$types[[input_type]]
    if (is.null(type_spec)) {
      stop("Unsupported identifier type: ", input_type, call. = FALSE)
    }

    idx <- which(input_types == input_type)
    chunks <- split(idx, ceiling(seq_along(idx) / max_in_ids))

    for (chunk_idx in chunks) {
      chunk_ids <- ids[chunk_idx]
      response <- call(
        api_fct = spec$endpoint,
        api_type = type_spec$api_type,
        api_query = paste(chunk_ids, collapse = " OR "),
        env = env,
        ks_path = ks_path
      )
      rows[[length(rows) + 1L]] <- .edsan_ct_rows_for_chunk(
        response = response,
        ids = chunk_ids,
        input_type = input_type,
        type_spec = type_spec,
        input_indices = chunk_idx
      )
    }
  }

  out <- dplyr::bind_rows(rows)
  out[order(out$input_index), , drop = FALSE]
}

#' Translate real hospital identifiers to EDSaN identifiers
#'
#' Uses EDSaN CT to translate patient identifiers (`IPP`) to `PATID` and stay
#' identifiers (`IEP`) to `EVTID`. When `id_type` is omitted, each identifier is
#' classified from the local invariant: IPP starts with `00`; every other digit
#' string is treated as IEP. An explicit `id_type` overrides this detection; it
#' only warns, and never errors, when a value does not match the heuristic.
#'
#' @param ids Real hospital identifiers. Must be character so IPP leading zeroes
#'   cannot be lost.
#' @param id_type Optional explicit input type: `"IPP"` or `"IEP"`. Takes
#'   precedence over automatic detection.
#' @param env EDSaN CT web-service environment name.
#' @param ks_path Optional d2imr keystore path. When `NULL`, the active path is
#'   resolved and passed explicitly, avoiding the invalid default in some d2imr
#'   versions.
#' @return A tibble retaining every input and reporting matched, not-found, or
#'   multiple correspondences. `not-found` is only reported for a valid EDSaN
#'   CT response with no correspondence; a backend failure or error response
#'   raises an error instead.
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
#' @param ids EDSaN `PATID` or `EVTID` values. Must be character; numeric input
#'   can silently alter identifier values through double-precision conversion.
#' @param id_type Input type: `"PATID"` or `"EVTID"`.
#' @inheritParams edsan_pseudonymize
#' @return A tibble retaining every input and reporting matched, not-found, or
#'   multiple correspondences. `not-found` is only reported for a valid EDSaN
#'   CT response with no correspondence; a backend failure or error response
#'   raises an error instead.
#' @details This function deliberately exposes real hospital identifiers.
#' @export
edsan_reidentify <- function(ids, id_type,
                             env = "edsan-ct", ks_path = NULL) {
  ids <- .edsan_ct_validate_ids(ids, require_character = TRUE)
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
