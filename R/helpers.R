`%||%` <- function(x, y) if (!is.null(x)) x else y

.edsan_is_limit_error <- function(err) {
  if (is.null(err)) return(FALSE)
  err <- paste(err, collapse = " ")
  stringr::str_detect(
    stringr::str_to_lower(err),
    "(4000|40000|40 000|too many|limit|quota|max results|max rows)"
  )
}

.pmsi_has_time <- function(x) {
  x <- as.character(x %||% "")
  stringr::str_detect(x, "\\d{2}:\\d{2}")
}

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

.pmsi_time_hms <- function(dt, raw) {
  # Return hms time if raw had an explicit time component, else NA.
  out <- rep(NA_character_, length(dt))
  ok <- !is.na(dt) & .pmsi_has_time(raw)
  out[ok] <- format(dt[ok], "%H:%M:%S")
  hms::as_hms(out)
}
