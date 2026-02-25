#' Null coalescing operator
#'
#' Returns `x` if it is not `NULL`, otherwise returns `y`.
#' This is useful for providing defaults when optional values may be missing.
#'
#' @param x Any object. If not `NULL`, it is returned as-is.
#' @param y Any object. Returned only when `x` is `NULL`.
#'
#' @return `x` if `x` is not `NULL`, else `y`.
#'
#' @examples
#' NULL %||% 1
#' 0 %||% 1
#' list(a = 1) %||% list(a = 2)
#'
#' @keywords internal
`%||%` <- function(x, y) if (!is.null(x)) x else y



#' Detect "limit exceeded" style errors returned by EDSaN calls
#'
#' Heuristically classifies an error message as a "limit/quota/max rows" error.
#' Intended to support batching logic (e.g., retry with smaller windows when a
#' request returns too many results).
#'
#' The detection is intentionally broad and based on a case-insensitive regex
#' against the concatenated error text.
#'
#' @param err An error object, character vector, or list-like error message.
#'   If `NULL`, returns `FALSE`.
#'
#' @return A single logical value: `TRUE` if `err` looks like a limit/quota error,
#'   otherwise `FALSE`.
#'
#' @details
#' The current pattern matches terms such as: "too many", "limit", "quota",
#' "max results", "max rows", "allowed size limit exceeded".
#'
#' @examples
#' .edsan_is_limit_error("Too many results, please narrow your query")
#' .edsan_is_limit_error(c("Error:", "allowed size limit exceeded"))
#' .edsan_is_limit_error("Connection refused")  # FALSE
#'
#' @importFrom stringr str_detect str_to_lower
#' @keywords internal
.edsan_is_limit_error <- function(err) {
  if (is.null(err)) return(FALSE)
  err <- paste(err, collapse = " ")
  stringr::str_detect(
    stringr::str_to_lower(err),
    "(too many|limit|quota|max results|max rows|allowed size limit exceeded)"
  )
}


#' Check whether a PMSI raw datetime string contains an explicit time component
#'
#' Determines if the input contains a time pattern of the form `"HH:MM"`.
#' This is used to differentiate date-only strings from datetime strings.
#'
#' @param x A vector-like object representing dates/datetimes (character, Date,
#'   etc.). `NULL` values are treated as empty strings.
#'
#' @return A logical vector of the same length as `x`, `TRUE` when the element
#'   contains a time pattern `\\d{2}:\\d{2}`, otherwise `FALSE`.
#'
#' @examples
#' .pmsi_has_time(c("2024-01-01", "2024-01-01 13:45", "01/02/2024 08:00"))
#' .pmsi_has_time(NA_character_)
#'
#' @importFrom stringr str_detect
#' @keywords internal
.pmsi_has_time <- function(x) {
  x <- as.character(x %||% "")
  stringr::str_detect(x, "\\d{2}:\\d{2}")
}


#' Parse PMSI date/datetime strings into POSIXct
#'
#' Robustly parses common PMSI date formats, supporting both date-only and
#' datetime strings. Returns `POSIXct`, with `NA` where parsing fails.
#'
#' @param x A vector of date/datetime representations. Can be `NULL`, character,
#'   Date, or POSIXt. If `x` is `POSIXt`, it is returned unchanged.
#' @param tz Time zone used for parsing and for the resulting `POSIXct`.
#'   Defaults to `"Europe/Paris"`.
#'
#' @return A `POSIXct` vector (or `NULL` if `x` is `NULL`).
#'
#' @details
#' Parsing is performed using `lubridate::parse_date_time()` with multiple
#' possible orders, including:
#' - ISO: `"Y-m-d"`, `"Y-m-d H:M"`, `"Y-m-d H:M:S"`
#' - French-style: `"d/m/Y"`, `"d/m/Y H:M"`, `"d/m/Y H:M:S"`
#' - Compact: `"Ymd"`, `"Ymd HM"`, `"Ymd HMS"`
#'
#' The function runs quietly (`quiet = TRUE`) to avoid warnings during batch
#' processing; failed parses become `NA`.
#'
#' @examples
#' .pmsi_parse_datetime(c("2024-01-01", "2024-01-01 13:45", "01/02/2024 08:00"))
#' .pmsi_parse_datetime("20240101 134500")
#'
#' @importFrom lubridate parse_date_time
#' @keywords internal
.pmsi_parse_datetime <- function(x, tz = "Europe/Paris") {
  # Robust parsing for both date-only and datetime strings.
  # Returns POSIXct (NA where parsing fails).
  if (is.null(x)) return(x)
  if (inherits(x, "POSIXt")) return(x)
  x <- as.character(x)
  lubridate::parse_date_time(
    x,
    orders = c(
      "Y-m-d H:M:S", "Y-m-d H:M", "Y-m-d",
      "d/m/Y H:M:S", "d/m/Y H:M", "d/m/Y",
      "Ymd HMS", "Ymd HM", "Ymd"
    ),
    quiet = TRUE,
    tz = tz
  )
}



#' Extract an hms time only when the raw input contained a time component
#'
#' Given a parsed datetime vector and the original raw values, returns an
#' `hms` vector containing `"HH:MM:SS"` for entries where (a) parsing succeeded
#' and (b) the raw string appeared to include a time component. Otherwise,
#' returns `NA` for those entries.
#'
#' @param dt A `POSIXct`/`POSIXt` vector, typically produced by
#'   `.pmsi_parse_datetime()`.
#' @param raw The original raw vector used to generate `dt`. Used to decide
#'   whether an explicit time was present (via `.pmsi_has_time()`).
#'
#' @return An `hms` vector of the same length as `dt`.
#'
#' @examples
#' raw <- c("2024-01-01", "2024-01-01 13:45", NA)
#' dt  <- .pmsi_parse_datetime(raw)
#' .pmsi_time_hms(dt, raw)
#'
#' @importFrom hms as_hms
#' @keywords internal
.pmsi_time_hms <- function(dt, raw) {
  # Return hms time if raw had an explicit time component, else NA.
  out <- rep(NA_character_, length(dt))
  ok <- !is.na(dt) & .pmsi_has_time(raw)
  out[ok] <- format(dt[ok], "%H:%M:%S")
  hms::as_hms(out)
}
