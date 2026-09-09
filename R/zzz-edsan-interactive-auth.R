# Interactive EDSaN CT authentication ----------------------------------------
#
# The standard Podsan path remains the d2imr keystore. On an interactive
# RStudio desktop session, when that keystore does not contain EDSaN CT
# credentials, redsan can fall back to the same REST API using credentials
# entered by the user. Those credentials live in memory only until R exits.

.edsan_ct_session_auth <- new.env(parent = emptyenv())

.edsan_ct_clear_session_auth <- function() {
  if (exists("credentials", envir = .edsan_ct_session_auth, inherits = FALSE)) {
    rm("credentials", envir = .edsan_ct_session_auth)
  }
  invisible(NULL)
}

.edsan_ct_valid_scalar <- function(x) {
  is.character(x) && length(x) == 1L && !is.na(x) && nzchar(trimws(x))
}

.edsan_ct_keystore_get <- function(key, ks_path) {
  if (!.edsan_ct_valid_scalar(ks_path) || !file.exists(ks_path) ||
      !requireNamespace("d2imr", quietly = TRUE)) {
    return(NULL)
  }

  getter <- tryCatch(
    getExportedValue("d2imr", "d2im_keystore.get"),
    error = function(e) NULL
  )
  if (!is.function(getter)) return(NULL)

  value <- suppressWarnings(suppressMessages(tryCatch(
    getter(key = key, path = ks_path),
    error = function(e) NULL
  )))
  value <- as.character(value)
  if (!.edsan_ct_valid_scalar(value)) return(NULL)
  value
}

.edsan_ct_keystore_auth <- function(env = "edsan-ct", ks_path = NULL) {
  ks_path <- .redsan_keystore_path(ks_path)
  if (!.edsan_ct_valid_scalar(ks_path) || !file.exists(ks_path)) return(NULL)

  prefix <- paste0("ws.", env, ".")
  values <- lapply(
    paste0(prefix, c("url", "usr", "pwd")),
    .edsan_ct_keystore_get,
    ks_path = ks_path
  )
  if (any(vapply(values, is.null, logical(1)))) return(NULL)

  list(
    url = sub("/+$", "", values[[1L]]),
    usr = values[[2L]],
    pwd = values[[3L]],
    ks_path = ks_path
  )
}

.edsan_ct_configured_url <- function(env = "edsan-ct", ks_path = NULL) {
  ks_path <- .redsan_keystore_path(ks_path)
  if (.edsan_ct_valid_scalar(ks_path) && file.exists(ks_path)) {
    key_url <- .edsan_ct_keystore_get(paste0("ws.", env, ".url"), ks_path)
    if (.edsan_ct_valid_scalar(key_url)) return(sub("/+$", "", key_url))
  }

  option_url <- getOption("redsan.edsan_ct_url", NULL)
  if (.edsan_ct_valid_scalar(option_url)) return(sub("/+$", "", option_url))

  env_url <- Sys.getenv("REDSAN_EDSAN_CT_URL", unset = "")
  if (.edsan_ct_valid_scalar(env_url)) return(sub("/+$", "", env_url))

  NULL
}

.edsan_ct_interactive_available <- function() {
  interactive() &&
    requireNamespace("rstudioapi", quietly = TRUE) &&
    isTRUE(tryCatch(rstudioapi::isAvailable(), error = function(e) FALSE))
}

.edsan_ct_interactive_credentials <- function(prompt_user = NULL,
                                               prompt_password = NULL) {
  if (exists("credentials", envir = .edsan_ct_session_auth, inherits = FALSE)) {
    return(get("credentials", envir = .edsan_ct_session_auth, inherits = FALSE))
  }

  injected_prompts <- !is.null(prompt_user) && !is.null(prompt_password)
  if (!injected_prompts && !.edsan_ct_interactive_available()) {
    stop(
      "EDSaN CT credentials are missing from the active keystore and an ",
      "interactive RStudio login is not available.",
      call. = FALSE
    )
  }

  if (is.null(prompt_user)) {
    prompt_user <- function() {
      rstudioapi::showPrompt(
        title = "EDSaN authentication",
        message = "Username:",
        default = ""
      )
    }
  }
  if (is.null(prompt_password)) {
    prompt_password <- function() rstudioapi::askForPassword("EDSaN password:")
  }

  usr <- prompt_user()
  pwd <- prompt_password()
  if (!.edsan_ct_valid_scalar(usr) || !.edsan_ct_valid_scalar(pwd)) {
    stop("EDSaN authentication was cancelled or left empty.", call. = FALSE)
  }

  credentials <- list(usr = usr, pwd = pwd)
  assign("credentials", credentials, envir = .edsan_ct_session_auth)
  credentials
}

.edsan_ct_proxy_config <- function() {
  if (!requireNamespace("httr", quietly = TRUE)) return(NULL)
  if (!requireNamespace("d2imr", quietly = TRUE)) return(httr::config())

  proxy_fn <- tryCatch(
    getFromNamespace("d2im_wsc.proxy_config", "d2imr"),
    error = function(e) NULL
  )
  if (is.function(proxy_fn)) {
    return(tryCatch(proxy_fn(), error = function(e) httr::config()))
  }
  httr::config()
}

.edsan_ct_build_url <- function(api_url, api_fct, api_type = NULL,
                                api_query = NULL) {
  pieces <- c(sub("/+$", "", api_url), api_fct)
  if (!is.null(api_type)) pieces <- c(pieces, api_type)
  if (!is.null(api_query)) pieces <- c(pieces, utils::URLencode(api_query))
  paste(pieces, collapse = "/")
}

.edsan_ct_http_get <- function(url, usr, pwd, accept = "application/json") {
  if (!requireNamespace("httr", quietly = TRUE)) {
    stop("Interactive EDSaN CT access requires the optional package `httr`.",
         call. = FALSE)
  }

  args <- list(
    url = url,
    httr::add_headers(
      Accept = accept,
      `Content-type` = "text/plain"
    ),
    httr::authenticate(usr, pwd),
    httr::config(
      ssl_verifypeer = FALSE,
      ssl_verifyhost = 0L,
      timeout = 30
    )
  )
  proxy <- .edsan_ct_proxy_config()
  if (!is.null(proxy)) args[[length(args) + 1L]] <- proxy
  do.call(httr::GET, args)
}

.edsan_ct_parse_http_response <- function(response, interactive_auth = FALSE) {
  status <- httr::status_code(response)
  if (identical(status, 204L)) return(list())

  if (identical(status, 401L) && isTRUE(interactive_auth)) {
    .edsan_ct_clear_session_auth()
    stop(
      "EDSaN CT rejected the interactive credentials (HTTP 401). ",
      "The cached credentials were cleared.",
      call. = FALSE
    )
  }

  if (status < 200L || status >= 300L) {
    payload <- tryCatch(httr::content(response, as = "parsed"),
                        error = function(e) NULL)
    msg <- NULL
    if (is.list(payload)) {
      msg <- payload$message
      if (is.null(msg)) msg <- payload$error
    }
    if (is.null(msg)) msg <- paste("HTTP status", status)
    return(list(status = status, message = paste(as.character(msg), collapse = " ")))
  }

  payload <- httr::content(response, as = "parsed")
  if (is.null(payload)) list() else payload
}

.edsan_ct_interactive_rest_call <- function(api_fct, api_type, api_query,
                                            env = "edsan-ct", ks_path = NULL) {
  api_url <- .edsan_ct_configured_url(env = env, ks_path = ks_path)
  if (!.edsan_ct_valid_scalar(api_url)) {
    stop(
      "EDSaN CT REST URL is unavailable. Set `REDSAN_EDSAN_CT_URL` in ",
      "`.Renviron` (or option `redsan.edsan_ct_url`) for interactive desktop access.",
      call. = FALSE
    )
  }

  credentials <- .edsan_ct_interactive_credentials()
  call_url <- .edsan_ct_build_url(api_url, api_fct, api_type, api_query)
  response <- .edsan_ct_http_get(call_url, credentials$usr, credentials$pwd)
  .edsan_ct_parse_http_response(response, interactive_auth = TRUE)
}

# Supersede the original backend wrapper while keeping the same internal API.
# A complete d2imr keystore always wins. The interactive path is considered only
# when the three EDSaN CT keystore entries are unavailable.
.edsan_ct_call <- function(api_fct, api_type, api_query,
                           env = "edsan-ct", ks_path = NULL) {
  resolved_path <- .redsan_keystore_path(ks_path)
  key_auth <- .edsan_ct_keystore_auth(env = env, ks_path = resolved_path)

  if (!is.null(key_auth)) {
    backend <- .edsan_ct_backend()
    invoke <- function() {
      backend$call(
        api_fct = api_fct,
        api_mod = api_type,
        api_query = api_query,
        env = env,
        ks_path = key_auth$ks_path
      )
    }

    if (!identical(env, "edsan-ct")) return(invoke())

    if (!requireNamespace("httr", quietly = TRUE)) {
      stop("EDSaN CT access requires the optional package `httr`.", call. = FALSE)
    }

    call_once <- function(quiet = FALSE) {
      expr <- quote(
        httr::with_config(
          httr::config(ssl_verifyhost = 0L, timeout = 30),
          invoke()
        )
      )
      if (quiet) {
        suppressWarnings(suppressMessages(eval(expr)))
      } else {
        eval(expr)
      }
    }

    response <- call_once(quiet = TRUE)
    if (is.null(response)) response <- call_once(quiet = FALSE)
    return(response)
  }

  if (!identical(env, "edsan-ct")) {
    stop(
      "EDSaN CT credentials are missing from the selected d2imr keystore for `",
      env, "`.",
      call. = FALSE
    )
  }

  .edsan_ct_interactive_rest_call(
    api_fct = api_fct,
    api_type = api_type,
    api_query = api_query,
    env = env,
    ks_path = resolved_path
  )
}

# Patient identity enrichment uses the same authentication policy as identifier
# translation. This supersedes the earlier keystore-only implementation.
.edsan_ct_patient_call <- function(patid, ks_path = NULL) {
  resolved_path <- .redsan_keystore_path(ks_path)
  key_auth <- .edsan_ct_keystore_auth(env = "edsan-ct", ks_path = resolved_path)

  interactive_auth <- is.null(key_auth)
  if (interactive_auth) {
    api_url <- .edsan_ct_configured_url(env = "edsan-ct", ks_path = resolved_path)
    if (!.edsan_ct_valid_scalar(api_url)) {
      stop(
        "EDSaN CT REST URL is unavailable. Set `REDSAN_EDSAN_CT_URL` in ",
        "`.Renviron` for interactive desktop access.",
        call. = FALSE
      )
    }
    credentials <- .edsan_ct_interactive_credentials()
  } else {
    api_url <- key_auth$url
    credentials <- list(usr = key_auth$usr, pwd = key_auth$pwd)
  }

  call_url <- .edsan_ct_build_url(
    api_url,
    "getPatientReidentificationInformations",
    api_query = patid
  )
  response <- .edsan_ct_http_get(
    call_url,
    credentials$usr,
    credentials$pwd,
    accept = "application/json"
  )

  status <- httr::status_code(response)
  if (identical(status, 204L)) return(NULL)
  if (identical(status, 401L) && interactive_auth) {
    .edsan_ct_clear_session_auth()
    stop(
      "EDSaN CT rejected the interactive credentials (HTTP 401). ",
      "The cached credentials were cleared.",
      call. = FALSE
    )
  }
  if (status < 200L || status >= 300L) {
    stop(
      "EDSaN CT patient reidentification failed with HTTP status ", status, ".",
      call. = FALSE
    )
  }

  payload <- httr::content(response, as = "parsed")
  if (!is.list(payload) || !is.list(payload$patient)) {
    stop("EDSaN CT returned an unrecognized patient reidentification response.",
         call. = FALSE)
  }

  payload$patient
}
