# -----------------------------
# PMSI processing API
# -----------------------------

# Pure core: flatten + parse DATENT/DATSORT + compute HEURE_*
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

# Main (stay-level) table
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

# Actes (long format)
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

# Diagnoses (derived from DALL)
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

#' Process PMSI stays
#'
#' Flatten PMSI stay data into three tables: main stays, actes, and diagnoses.
#' DATENT and DATSORT are parsed and corresponding HEURE_* columns are derived.
#'
#' @param data List of PMSI API entries.
#' @return A list with tibbles `main`, `actes`, and `diag`.
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
#' main <- pmsi$main
#' actes <- pmsi$actes
#' diag <- pmsi$diag
#' @export
process_pmsi <- function(data) {
  cleaned <- .pmsi_prepare(data)
  list(
    main  = .pmsi_main(cleaned),
    actes = .pmsi_actes(cleaned),
    diag  = .pmsi_diag(cleaned)
  )
}
