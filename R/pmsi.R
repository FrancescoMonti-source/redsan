# PMSI processing API -----------------------------

#' Prepare raw PMSI stay payload for downstream tabulation
#'
#' Flattens a list of PMSI API stay entries into a single tibble and standardizes
#' column values. Nested structures are recursively unlisted; empty or missing
#' values become `NA_character_`, and multi-valued fields are collapsed with `;`.
#'
#' If present, `DATENT` and `DATSORT` are parsed to `POSIXct` using
#' `.pmsi_parse_datetime()`. For each, a corresponding `HEURE_*` column is added
#' using `.pmsi_time_hms()`, containing an `hms` time only when the raw string
#' included an explicit time component; otherwise it is `NA`.
#'
#' @param data List of PMSI API entries (one list per stay). Each entry may be a
#'   nested list.
#'
#' @return A tibble with one row per stay and flattened columns. When `DATENT`
#'   and/or `DATSORT` are present, they are converted to `POSIXct` and
#'   `HEURE_DATENT` / `HEURE_DATSORT` are added as `hms`. If a date column is
#'   absent, the corresponding `HEURE_*` column is still created and filled with
#'   `NA`.
#'
#' @details
#' Value normalization rules applied per field after flattening:
#' - `NULL` or length 0  --> `NA_character_`
#' - length > 1          --> `paste(..., collapse = ";")`
#' - otherwise           --> `as.character(v)`
#'
#' @examples
#' x <- list(
#'   list(PATID = "P1", EVTID = "E1", DATENT = "2020-01-01 08:30",
#'        DATSORT = "2020-01-02", SOME = c("a","b")),
#'   list(PATID = "P2", EVTID = "E2", DATENT = NULL)
#' )
#' out <- .pmsi_prepare(x)
#' out$SOME  # "a;b"
#' out$HEURE_DATSORT  # NA for date-only values
#'
#' @importFrom tibble as_tibble
#' @importFrom dplyr bind_rows
#' @keywords internal
.pmsi_prepare <- function(data) {
  clean_data <- lapply(data, function(x) {
    flat <- unlist(x, recursive = TRUE)
    flat <- lapply(flat, function(v) {
      if (is.null(v) || length(v) == 0) {
        NA_character_
      } else if (length(v) > 1) {
        paste(as.character(v), collapse = ";")
      } else {
        as.character(v)
      }
    })
    tibble::as_tibble(flat)
  }) %>% dplyr::bind_rows()

  if ("DATENT" %in% names(clean_data)) {
    raw <- clean_data$DATENT
    clean_data$DATENT <- .pmsi_parse_datetime(raw)
    clean_data$HEURE_DATENT <- .pmsi_time_hms(clean_data$DATENT, raw)
  } else {
    clean_data$HEURE_DATENT <- hms::as_hms(rep(NA_character_, nrow(clean_data)))
  }

  if ("DATSORT" %in% names(clean_data)) {
    raw <- clean_data$DATSORT
    clean_data$DATSORT <- .pmsi_parse_datetime(raw)
    clean_data$HEURE_DATSORT <- .pmsi_time_hms(clean_data$DATSORT, raw)
  } else {
    clean_data$HEURE_DATSORT <- hms::as_hms(rep(NA_character_, nrow(clean_data)))
  }

  clean_data
}



#' Extract the main (stay-level) PMSI table
#'
#' Selects a standard set of stay-level variables from prepared PMSI data and
#' returns one row per distinct stay.
#'
#' Intended to be applied after `.pmsi_prepare()`, which parses `DATENT/DATSORT`
#' and populates `HEURE_*` columns.
#'
#' @param data A tibble produced by `.pmsi_prepare()`.
#'
#' @return A tibble containing the intersection of `data` with the standard
#'   stay-level columns, deduplicated with `distinct()`.
#'
#' @details
#' Columns are selected with `dplyr::any_of()`: missing columns are silently
#' ignored (useful when PMSI payloads vary by endpoint/version).
#'
#' @examples
#' cleaned <- .pmsi_prepare(list(list(PATID="P1", EVTID="E1", DATENT="2020-01-01")))
#' main <- .pmsi_main(cleaned)
#'
#' @importFrom dplyr select any_of distinct
#' @keywords internal
.pmsi_main <- function(data) {
  cols <- c(
    "PATID", "EVTID", "ELTID",
    "DATENT", "HEURE_DATENT",
    "DATSORT", "HEURE_DATSORT",
    "PATBD", "PATAGE", "PATSEX",
    "SEJDUR", "MODEENT", "MODESORT",
    "PMSISTATUT", "SEJUM", "SEJUF",
    "GHM", "SEVERITE", "SRC"
  )
  data %>% dplyr::select(dplyr::any_of(cols)) %>% dplyr::distinct()
}





#' Extract PMSI actes in long format
#'
#' Builds an actes table (one row per acte occurrence) from prepared PMSI stay
#' data. The function keeps stay identifiers and key context variables, then
#' pivots acte-related columns (e.g., `CODEACTE*`, `DATEACTE*`, `UFPRO*`, `UFDEM*`,
#' `NOMENCLATURE*`) from wide to long using a `.value` pivot.
#'
#' This function assumes `DATENT` and `DATSORT` are already parsed as `POSIXct`
#' and `HEURE_*` columns are already created by `.pmsi_prepare()`.
#'
#' @param data A tibble produced by `.pmsi_prepare()`.
#'
#' @return A tibble in long format, with at least the columns:
#'   `PATID`, `EVTID`, `ELTID`, `PATBD`, `PATAGE`, `PATSEX`, `DATENT`,
#'   `HEURE_DATENT`, `DATSORT`, `HEURE_DATSORT`, `SEJDUR`, `SEJUM`, `SEJUF`,
#'   plus acte fields such as `CODEACTE`, `DATEACTE`, `UFPRO`, `UFDEM`,
#'   `NOMENCLATURE` when present. Rows with all acte values missing are dropped
#'   (`values_drop_na = TRUE`). Duplicates are removed with `distinct()`.
#'
#' @details
#' The pivot is driven by:
#' - `names_pattern = "(CODEACTE|DATEACTE|UFPRO|UFDEM|NOMENCLATURE).*"`
#' - `names_to = c(".value")`
#'
#' This collapses columns that share the same acte index/suffix into a single
#' row, producing canonical columns named exactly as the captured groups
#' (`CODEACTE`, `DATEACTE`, `UFPRO`, `UFDEM`, `NOMENCLATURE`).
#'
#' @examples
#' x <- list(list(
#'   PATID="P1", EVTID="E1", ELTID="L1",
#'   DATENT="2020-01-01 08:30",
#'   CODEACTE1="A123", DATEACTE1="2020-01-01 09:00", UFPRO1="01"
#' ))
#' cleaned <- .pmsi_prepare(x)
#' actes <- .pmsi_actes(cleaned)
#'
#' @importFrom dplyr select any_of contains distinct
#' @importFrom tidyr pivot_longer
#' @keywords internal
.pmsi_actes <- function(data) {
  # assume DATENT/DATSORT already POSIXct and HEURE_* already set by pmsi_prepare
  data %>%
    dplyr::select(dplyr::any_of(c(
      "PATID", "EVTID", "ELTID", "PATBD", "PATAGE", "PATSEX",
      "DATENT", "HEURE_DATENT", "DATSORT", "HEURE_DATSORT",
      "SEJDUR", "SEJUM", "SEJUF"
    )),
    dplyr::contains("ACTE"), dplyr::contains("UFPRO"), dplyr::contains("UFDEM")) %>%
    tidyr::pivot_longer(
      cols = c(
        dplyr::contains("CODEACTE"),
        dplyr::contains("DATEACTE"),
        dplyr::contains("UFPRO"),
        dplyr::contains("UFDEM"),
        dplyr::contains("NOMENCLATURE")
      ),
      names_to = c(".value"),
      names_pattern = "(CODEACTE|DATEACTE|UFPRO|UFDEM|NOMENCLATURE).*",
      values_drop_na = TRUE
    ) %>%
    dplyr::distinct()
}



#' Extract PMSI diagnoses from the DALL field
#'
#' Creates a diagnoses table by splitting the `DALL` field (space-separated
#' tokens) into one row per token and parsing diagnosis code and type.
#'
#' The function expects `DALL` entries to follow a pattern like `"TT:CODE"`
#' where `TT` is a 2-character type prefix (e.g., "01", "02") and `CODE` is the
#' diagnosis identifier. Tokens without digits are dropped.
#'
#' @param data A tibble produced by `.pmsi_prepare()`.
#'
#' @return A tibble with stay identifiers (`*_ID` columns present in `data`),
#'   `DATENT`, `HEURE_DATENT`, `DATSORT`, `HEURE_DATSORT`, plus:
#'   - `diag`: the substring after `:` (may be `NA` if no `:` is present)
#'   - `type_diag`: the first two characters of the token
#'
#' @details
#' Processing steps:
#' 1. Keep ID columns (`ends_with("ID")`), dates/times, and `DALL`
#' 2. `separate_rows(DALL, sep = " ")`
#' 3. Keep only tokens containing at least one digit (`str_detect(DALL, "\\d")`)
#' 4. Derive `diag` and `type_diag`, then drop the raw `DALL` column
#'
#' @examples
#' x <- list(list(PATID="P1", EVTID="E1", ELTID="L1",
#'                DATENT="2020-01-01 08:30", DALL="01:A41 02:I10"))
#' cleaned <- .pmsi_prepare(x)
#' diag <- .pmsi_diag(cleaned)
#'
#' @importFrom dplyr select ends_with filter mutate
#' @importFrom tidyr separate_rows
#' @importFrom stringr str_detect str_extract str_sub
#' @keywords internal
.pmsi_diag <- function(data) {
  data %>%
    dplyr::select(dplyr::ends_with("ID"), DATENT, HEURE_DATENT, DATSORT, HEURE_DATSORT, DALL) %>%
    tidyr::separate_rows(DALL, sep = " ") %>%
    dplyr::filter(stringr::str_detect(DALL, "\\d")) %>%
    dplyr::mutate(
      diag      = stringr::str_extract(DALL, ":(.+)", group = 1),
      type_diag = stringr::str_sub(DALL, 1, 2)
    ) %>%
    dplyr::select(-DALL)
}


#' Process PMSI stays into analysis-ready tables
#'
#' Flattens PMSI stay payloads and returns three tables:
#' \describe{
#'   \item{`main`}{one row per stay (stay-level variables)}
#'   \item{`actes`}{actes in long format (one row per acte)}
#'   \item{`diag`}{diagnoses derived from `DALL` (one row per token)}
#' }
#'
#' `DATENT` and `DATSORT` are parsed to `POSIXct` and corresponding `HEURE_*`
#' columns are derived as `hms` times when the raw input included an explicit
#' time component.
#'
#' @param data List of PMSI API entries (one list per stay).
#'
#' @return A list with tibbles `main`, `actes`, and `diag`.
#'
#' @examples
#' example_data <- list(
#'   list(
#'     PATID = "P1",
#'     EVTID = "E1",
#'     ELTID = "L1",
#'     DATENT = "2020-01-01T08:30:00",
#'     DATSORT = "2020-01-02T10:15:00",
#'     PATBD = "1980-01-01",
#'     PATAGE = "40",
#'     PATSEX = "M",
#'     SEJDUR = "2",
#'     SEJUM = "2020-01-01",
#'     SEJUF = "2020-01-02",
#'     DALL = "01:AA 02:BB",
#'     CODEACTE1 = "A123",
#'     DATEACTE1 = "2020-01-01T09:00:00",
#'     UFPRO1 = "01"
#'   )
#' )
#' pmsi <- process_pmsi(example_data)
#' pmsi$main
#' pmsi$actes
#' pmsi$diag
#'
#' @export
process_pmsi <- function(data) {
  cleaned <- .pmsi_prepare(data)
  list(
    main  = .pmsi_main(cleaned),
    actes = .pmsi_actes(cleaned),
    diag  = .pmsi_diag(cleaned)
  )
}
