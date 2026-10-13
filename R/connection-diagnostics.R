# Read-only EDSaN CT / CORA connectivity checks -----------------------------

.check_connections_safe_error <- function(e) {
  msg <- gsub("https?://[^[:space:]]+", "[request URL redacted]",
              conditionMessage(e), perl = TRUE)
  gsub("[0-9]{7,}", "[identifier redacted]", msg, perl = TRUE)
}

.check_connections_step <- function(component, check, action) {
  started <- proc.time()[["elapsed"]]
  result <- tryCatch(
    list(status = "ok", detail = as.character(action())),
    error = function(e) {
      list(status = "failed", detail = .check_connections_safe_error(e))
    }
  )
  tibble::tibble(
    component = component, check = check, status = result$status,
    seconds = round(proc.time()[["elapsed"]] - started, 3L),
    detail = result$detail
  )
}

.check_connections_skipped <- function(component, check, reason) {
  tibble::tibble(
    component = component, check = check, status = "skipped",
    seconds = NA_real_, detail = reason
  )
}

.check_connections_ct <- function(env, ks_path) {
  # Use a synthetic value, not an identifier from a patient cohort.
  response <- edsan_ct("0", from = "EVTID", env = env, ks_path = ks_path)
  if (!is.data.frame(response) || !("status" %in% names(response)) ||
      nrow(response) != 1L ||
      !response$status[[1L]] %in% c("matched", "not_found")) {
    stop("EDSaN CT returned an unexpected response to the synthetic probe.",
         call. = FALSE)
  }
  "Valid correspondence response (synthetic identifier)"
}

.check_connections_endpoints <- function(descriptor) {
  hits <- regmatches(
    descriptor,
    gregexpr(
      "(?i)\\(HOST\\s*=\\s*[^)]+\\)\\s*\\(PORT\\s*=\\s*[0-9]+\\)",
      descriptor, perl = TRUE
    )
  )[[1L]]
  if (!length(hits)) {
    stop("No HOST/PORT pairs found in the CORA descriptor.", call. = FALSE)
  }
  host <- vapply(hits, function(x) {
    trimws(sub("(?is).*\\(HOST\\s*=\\s*([^)]+)\\).*", "\\1",
               x, perl = TRUE))
  }, character(1L))
  port <- vapply(hits, function(x) {
    as.integer(sub("(?is).*\\(PORT\\s*=\\s*([0-9]+)\\).*", "\\1",
                   x, perl = TRUE))
  }, integer(1L))
  unique(data.frame(host = host, port = port, stringsAsFactors = FALSE))
}

.check_connections_dns <- function(host) {
  address <- utils::nsl(host)
  if (!length(address) || anyNA(address) || !nzchar(address[[1L]])) {
    stop("DNS resolution returned no address.", call. = FALSE)
  }
  paste("Resolved to", address[[1L]])
}

.check_connections_tcp <- function(host, port, timeout) {
  connection <- socketConnection(
    host = host, port = port, open = "r+", blocking = TRUE,
    timeout = timeout
  )
  on.exit(close(connection), add = TRUE)
  "TCP connection established"
}

.check_connections_oracle <- function(ks_path) {
  query <- .cora_query(
    sql = "SELECT 1 AS CONNECTIVITY_TEST FROM DUAL", ks_path = ks_path
  )
  if (nrow(query) != 1L || query$CONNECTIVITY_TEST[[1L]] != 1) {
    stop("Oracle did not return the expected SELECT 1 result.",
         call. = FALSE)
  }
  "Oracle JDBC connection and SELECT 1 succeeded"
}

#' Diagnose EDSaN CT and CORA connectivity
#'
#' Calls the actual EDSaN correspondence endpoint using a synthetic identifier.
#' Checks DNS and TCP to the Oracle endpoints configured in the active keystore.
#' Does not query or return patient data.
#'
#' A full JDBC login and read-only SQL check is optional because Oracle failover
#' can take much longer than the individual socket timeout. A successful TCP
#' probe does not imply that JDBC login or authentication will work.
#'
#' @param env EDSaN CT environment (default: edsan-ct).
#' @param ks_path Optional path to an alternate d2imr keystore.
#' @param timeout Positive timeout in seconds per TCP probe.
#' @param oracle_query If TRUE, also connect with JDBC and execute SELECT 1.
#' @return A tibble with component, check, status, seconds and detail columns.
#'   Status is ok, failed or skipped; no patient IDs, URLs or credentials
#'   are included in the diagnostic output.
#' @examples
#' \dontrun{
#' edsan_check_connections()
#' edsan_check_connections(oracle_query = TRUE)
#' }
#' @export
edsan_check_connections <- function(env = "edsan-ct", ks_path = NULL,
                              timeout = 3, oracle_query = FALSE) {
  if (!is.numeric(timeout) || length(timeout) != 1L ||
      is.na(timeout) || !is.finite(timeout) || timeout <= 0) {
    stop("timeout must be one positive number of seconds.", call. = FALSE)
  }
  if (!is.logical(oracle_query) || length(oracle_query) != 1L ||
      is.na(oracle_query)) {
    stop("oracle_query must be TRUE or FALSE.", call. = FALSE)
  }

  rows <- list(.check_connections_step(
    "EDSaN CT", "correspondence",
    function() .check_connections_ct(env = env, ks_path = ks_path)
  ))

  endpoints <- NULL
  rows[[length(rows) + 1L]] <- .check_connections_step(
    "CORA", "configuration", function() {
      descriptor <- .cora_keystore_value("db.cora.url", ks_path = ks_path)
      endpoints <<- .check_connections_endpoints(descriptor)
      paste(nrow(endpoints), "Oracle endpoint(s) configured")
    }
  )

  if (is.null(endpoints)) {
    rows[[length(rows) + 1L]] <- .check_connections_skipped(
      "CORA", "DNS/TCP", "Oracle connection descriptor unavailable"
    )
    rows[[length(rows) + 1L]] <- .check_connections_skipped(
      "CORA", "JDBC", "Oracle connection descriptor unavailable"
    )
  } else {
    for (host in unique(endpoints$host)) {
      rows[[length(rows) + 1L]] <- .check_connections_step(
        "CORA", paste("DNS", host),
        function() .check_connections_dns(host)
      )
    }
    for (i in seq_len(nrow(endpoints))) {
      host <- endpoints$host[[i]]
      port <- endpoints$port[[i]]
      rows[[length(rows) + 1L]] <- .check_connections_step(
        "CORA", paste0("TCP ", host, ":", port),
        function() .check_connections_tcp(host, port, timeout)
      )
    }
    rows[[length(rows) + 1L]] <- if (oracle_query) {
      .check_connections_step(
        "CORA", "JDBC", function() .check_connections_oracle(ks_path)
      )
    } else {
      .check_connections_skipped(
        "CORA", "JDBC", "Optional: oracle_query = TRUE for SELECT 1"
      )
    }
  }
  dplyr::bind_rows(rows)
}
