# PMSI processing API -----------------------------

#' Prepare raw PMSI stay payload for downstream tabulation
#'
#' Flattens a list of PMSI API stay entries into a single tibble and standardizes
#' column values. Nested structures are recursively unlisted; empty or missing
#' values become `NA_character_`, multi-valued fields are collapsed with `;`,
#' and `PATAGE` is converted to numeric when present.
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
#' @noRd
.pmsi_prepare <- function(data) {
  if (is.null(data) || length(data) == 0) {
    return(tibble::tibble(
      HEURE_DATENT = hms::as_hms(character()),
      HEURE_DATSORT = hms::as_hms(character())
    ))
  }

  identifiers <- .edsan_identifier_names("pmsi", "main")
  clean_data <- lapply(data, function(x) {
    x <- .edsan_normalize_identifier_fields(x, identifiers)
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

  if ("PATAGE" %in% names(clean_data)) {
    clean_data$PATAGE <- suppressWarnings(as.numeric(clean_data$PATAGE))
  }

  clean_data
}



#' Extract the complete main PMSI table
#'
#' Selects a standard set of PMSI variables from prepared data and retains every
#' distinct source record. No source precedence is applied here.
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
#' @noRd
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
  if (nrow(data) == 0) {
    return(tibble::tibble(
      PATID = character(),
      EVTID = character(),
      ELTID = character(),
      DATENT = as.POSIXct(character()),
      HEURE_DATENT = hms::as_hms(character()),
      DATSORT = as.POSIXct(character()),
      HEURE_DATSORT = hms::as_hms(character()),
      PATBD = character(),
      PATAGE = numeric(),
      PATSEX = character(),
      SEJDUR = character(),
      MODEENT = character(),
      MODESORT = character(),
      PMSISTATUT = character(),
      SEJUM = character(),
      SEJUF = character(),
      GHM = character(),
      SEVERITE = character(),
      SRC = character()
    ))
  }

  data %>% dplyr::select(dplyr::any_of(cols)) %>% dplyr::distinct()
}

#' Prefer PMSI source C over DW within each unit
#'
#' Applies the PMSI source rule used by [process_pmsi()] by default.
#' Within each `PATID + EVTID + SEJUM + SEJUF` group, rows whose `SRC` is `"DW"`
#' are removed only when at least one `"C"` row is present. `"DW"` remains the
#' fallback for units without `"C"`; unknown and missing sources are always
#' retained. Source labels are matched case-insensitively after trimming, but
#' their stored values are not modified.
#'
#' Rows with a missing `PATID` or `EVTID` are not subject to source precedence.
#' If `SRC` is absent, `main` is returned unchanged.
#' The function preserves the input columns, column types, and retained row
#' order, including `ELTID`.
#'
#' @param main A PMSI main data frame. When `SRC` is present, `PATID`, `EVTID`,
#'   `SEJUM`, and `SEJUF` must also be present so the precedence group is
#'   defined.
#'
#' @return `main` after applying the `C`-over-`DW` rule, or unchanged when
#'   `SRC` is absent.
#'
#' @examples
#' main <- tibble::tibble(
#'   PATID = c("P1", "P1", "P1"),
#'   EVTID = c("E1", "E1", "E1"),
#'   SEJUM = c("U1", "U1", "U2"),
#'   SEJUF = c("UF1", "UF1", "UF2"),
#'   SRC = c("DW", "C", "DW")
#' )
#' prefer_pmsi_src_c_over_dw(main)
#'
#' @importFrom rlang .data
#' @export
prefer_pmsi_src_c_over_dw <- function(main) {
  if (!is.data.frame(main)) {
    stop("`main` must be a data frame.", call. = FALSE)
  }

  if (!"SRC" %in% names(main)) return(main)

  required <- c("PATID", "EVTID", "SEJUM", "SEJUF", "SRC")
  missing <- setdiff(required, names(main))
  if (length(missing) > 0) {
    stop(
      "`main` is missing required column(s): ",
      paste(missing, collapse = ", "),
      ".",
      call. = FALSE
    )
  }

  if (nrow(main) == 0) return(main)

  main %>%
    dplyr::mutate(
      .pmsi_source = toupper(trimws(as.character(.data$SRC))),
      .pmsi_key_complete = !is.na(.data$PATID) & !is.na(.data$EVTID)
    ) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(required[1:4]))) %>%
    dplyr::mutate(
      .pmsi_has_c = any(.data$.pmsi_source %in% "C")
    ) %>%
    dplyr::filter(
      !(
        .data$.pmsi_key_complete &
          .data$.pmsi_has_c &
          .data$.pmsi_source %in% "DW"
      )
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(
      -dplyr::all_of(c(".pmsi_source", ".pmsi_key_complete", ".pmsi_has_c"))
    )
}

.pmsi_missing_datetime <- function(x) {
  tz <- attr(x, "tzone") %||% "Europe/Paris"
  as.POSIXct(NA_real_, origin = "1970-01-01", tz = tz)
}

.pmsi_as_datetime <- function(x) {
  if (inherits(x, "POSIXt")) return(as.POSIXct(x))
  .pmsi_parse_datetime(x)
}

.pmsi_min_datetime <- function(x) {
  x <- .pmsi_as_datetime(x)
  if (length(x) == 0 || all(is.na(x))) return(.pmsi_missing_datetime(x))
  min(x, na.rm = TRUE)
}

.pmsi_max_datetime <- function(x) {
  x <- .pmsi_as_datetime(x)
  if (length(x) == 0 || all(is.na(x))) return(.pmsi_missing_datetime(x))
  max(x, na.rm = TRUE)
}

.pmsi_event_dates <- function(main) {
  if (!"EVTID" %in% names(main)) {
    return(tibble::tibble(
      EVTID = character(),
      DATENT = as.POSIXct(character()),
      DATSORT = as.POSIXct(character())
    ))
  }

  if (!"DATENT" %in% names(main)) {
    main$DATENT <- .pmsi_missing_datetime(main$EVTID)
  }
  if (!"DATSORT" %in% names(main)) {
    main$DATSORT <- .pmsi_missing_datetime(main$EVTID)
  }

  event_keys <- if ("PATID" %in% names(main)) c("PATID", "EVTID") else "EVTID"

  main %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(event_keys))) %>%
    dplyr::summarise(
      DATENT = .pmsi_min_datetime(DATENT),
      DATSORT = .pmsi_max_datetime(DATSORT),
      .groups = "drop"
    )
}

.pmsi_attach_event_dates <- function(data, event_dates) {
  if (!"EVTID" %in% names(data)) return(data)

  event_keys <- if (
    "PATID" %in% names(data) && "PATID" %in% names(event_dates)
  ) c("PATID", "EVTID") else "EVTID"

  data %>%
    dplyr::select(-dplyr::any_of(c("DATENT", "DATSORT"))) %>%
    dplyr::left_join(event_dates, by = event_keys)
}





#' Extract PMSI actes in long format
#'
#' Builds an actes table (one row per acte occurrence) from prepared PMSI stay
#' data. The function keeps stay identifiers and key context variables, then
#' pivots acte-related columns (e.g., `CODEACTE*`, `DATEACTE*`, `UFPRO*`, `UFDEM*`,
#' `NOMENCLATURE*`) from wide to long using a `.value` pivot.
#'
#' This function assumes `DATENT` and `DATSORT` are already parsed as `POSIXct`
#' and `HEURE_*` columns are already created by `.pmsi_prepare()`. After the
#' pivot, `DATEACTE` is parsed to `POSIXct` and `HEURE_DATEACTE` records an
#' explicitly supplied time.
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
#' @noRd
.pmsi_actes <- function(data) {
  # assume DATENT/DATSORT already POSIXct and HEURE_* already set by pmsi_prepare
  acte_cols <- names(data)[grepl("ACTE|UFPRO|UFDEM|NOMENCLATURE", names(data))]
  if (length(acte_cols) == 0) {
    return(tibble::tibble(
      PATID = character(),
      EVTID = character(),
      ELTID = character(),
      DATENT = as.POSIXct(character()),
      HEURE_DATENT = hms::as_hms(character()),
      DATSORT = as.POSIXct(character()),
      HEURE_DATSORT = hms::as_hms(character()),
      CODEACTE = character(),
      DATEACTE = as.POSIXct(character()),
      HEURE_DATEACTE = hms::as_hms(character()),
      UFPRO = character(),
      UFDEM = character(),
      NOMENCLATURE = character()
    ))
  }

  out <- data %>%
    dplyr::select(dplyr::any_of(c(
      "PATID", "EVTID", "ELTID", "PATBD", "PATAGE", "PATSEX",
      "DATENT", "HEURE_DATENT", "DATSORT", "HEURE_DATSORT",
      "SEJDUR", "SEJUM", "SEJUF"
    )),
    dplyr::contains("ACTE"), dplyr::contains("UFPRO"),
    dplyr::contains("UFDEM"), dplyr::contains("NOMENCLATURE")) %>%
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

  if ("DATEACTE" %in% names(out)) {
    raw <- out$DATEACTE
    out$DATEACTE <- .pmsi_parse_datetime(raw)
    out$HEURE_DATEACTE <- .pmsi_time_hms(out$DATEACTE, raw)
  }

  out
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
#' @noRd
.pmsi_diag <- function(data) {
  if (!"DALL" %in% names(data)) {
    return(tibble::tibble(
      PATID = character(),
      EVTID = character(),
      ELTID = character(),
      PATBD = character(),
      PATAGE = numeric(),
      PATSEX = character(),
      DATENT = as.POSIXct(character()),
      HEURE_DATENT = hms::as_hms(character()),
      DATSORT = as.POSIXct(character()),
      HEURE_DATSORT = hms::as_hms(character()),
      diag = character(),
      type_diag = character()
    ))
  }

  data %>%
    dplyr::select(
      dplyr::ends_with("ID"),
      dplyr::any_of(c("PATBD", "PATAGE", "PATSEX")),
      DATENT, HEURE_DATENT, DATSORT, HEURE_DATSORT, DALL
    ) %>%
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
#'   \item{`main`}{the normalized PMSI main table after the selected source policy}
#'   \item{`actes`}{actes in long format (one row per acte), with the matching
#'   CCAM/CDAM `CODEACTE_LABEL` when available}
#'   \item{`diag`}{diagnoses derived from `DALL` (one row per token), with the
#'   matching CIM-10 `CODE_LABEL` when available}
#' }
#'
#' `DATENT`, `DATSORT`, and acte-level `DATEACTE` are parsed to `POSIXct` and
#' corresponding `HEURE_*` columns are derived as `hms` times when the raw input
#' included an explicit time component. `actes` and `diag` remain complete and
#' receive event-level stay dates computed from the complete `main`: minimum
#' `DATENT` and maximum `DATSORT` per `PATID + EVTID`. For legacy payloads that
#' do not contain `PATID`, the event bounds fall back to `EVTID`. `PATAGE` is
#' numeric in every returned table that carries it. By default, the returned
#' `main` applies [prefer_pmsi_src_c_over_dw()] after those bounds have been
#' derived. Use `source_policy = "all"` to retain every normalized `main` row.
#' `actes` and `diag` are not source-filtered.
#'
#' @param data List of PMSI API entries (one list per stay).
#' @param source_policy PMSI `main` source policy. `"c_over_dw"` (default)
#'   removes `DW` rows only where `C` exists for the same
#'   `PATID + EVTID + SEJUM + SEJUF`; `"all"` retains every normalized row.
#'   If `SRC` is absent, both policies return the same `main`.
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
process_pmsi <- function(data, source_policy = c("c_over_dw", "all")) {
  source_policy <- match.arg(source_policy)
  cleaned <- .pmsi_prepare(data)
  complete_main <- .pmsi_main(cleaned)
  event_dates <- .pmsi_event_dates(complete_main)
  actes <- .pmsi_attach_event_dates(.pmsi_actes(cleaned), event_dates)
  diag <- .pmsi_attach_event_dates(.pmsi_diag(cleaned), event_dates)
  main <- if (identical(source_policy, "c_over_dw")) {
    prefer_pmsi_src_c_over_dw(complete_main)
  } else {
    complete_main
  }

  label_pmsi(
    list(
      main = .edsan_normalize_identifier_columns(main, "pmsi", "main"),
      actes = .edsan_normalize_identifier_columns(actes, "pmsi", "actes"),
      diag = .edsan_normalize_identifier_columns(diag, "pmsi", "diag")
    )
  )
}

#' Add reference labels to normalized PMSI tables
#'
#' Enriches normalized PMSI `actes` and `diag` tables with the reference labels
#' distributed with `redsan`. It can be applied to older artifacts;
#' [process_pmsi()] uses the same function for new outputs. The `main` table is
#' returned unchanged.
#'
#' Acts are matched to the combined CCAM/CDAM reference by
#' `NOMENCLATURE + CODEACTE`, producing `CODEACTE_LABEL`. Diagnoses are matched
#' to the CIM-10 reference by `diag = CODE`, producing `CODE_LABEL`. Joins are
#' exact, preserve every PMSI row, and leave unmatched labels as `NA`. Existing
#' `CODEACTE_LABEL` and `CODE_LABEL` columns are refreshed from the packaged
#' references.
#' Older artifacts without `NOMENCLATURE` retain their acts with missing
#' nomenclature and procedure labels.
#'
#' @param pmsi A list containing the `main`, `actes`, and `diag` data frames,
#'   normally returned by [process_pmsi()].
#'
#' @return The input PMSI list with `CODEACTE_LABEL` added to `actes` and
#'   `CODE_LABEL` added to `diag`. All other tables, columns, and rows are
#'   preserved.
#'
#' @examples
#' raw <- list(list(
#'   PATID = "P1",
#'   EVTID = "E1",
#'   ELTID = "L1",
#'   DATENT = "2024-01-01",
#'   DATSORT = "2024-01-02",
#'   CODEACTE1 = "JKFA023",
#'   NOMENCLATURE1 = "CCAM",
#'   DALL = "01:E46"
#' ))
#' normalized <- process_pmsi(raw)
#' labelled <- label_pmsi(normalized)
#' labelled$actes[c("NOMENCLATURE", "CODEACTE", "CODEACTE_LABEL")]
#' labelled$diag[c("diag", "CODE_LABEL")]
#'
#' @export
label_pmsi <- function(pmsi) {
  required_tables <- c("main", "actes", "diag")

  if (!is.list(pmsi) || !all(required_tables %in% names(pmsi))) {
    stop(
      "`pmsi` must be a list containing main, actes, and diag tables.",
      call. = FALSE
    )
  }

  invalid_tables <- required_tables[
    !vapply(pmsi[required_tables], is.data.frame, logical(1))
  ]
  if (length(invalid_tables) > 0L) {
    stop(
      "PMSI table(s) must be data frames: ",
      paste(invalid_tables, collapse = ", "), ".",
      call. = FALSE
    )
  }

  required_columns <- list(
    actes = "CODEACTE",
    diag = "diag"
  )
  for (table_name in names(required_columns)) {
    missing_columns <- setdiff(
      required_columns[[table_name]],
      names(pmsi[[table_name]])
    )
    if (length(missing_columns) > 0L) {
      stop(
        "PMSI table `", table_name, "` is missing required column(s): ",
        paste(missing_columns, collapse = ", "), ".",
        call. = FALSE
      )
    }
  }

  if (!"NOMENCLATURE" %in% names(pmsi$actes)) {
    pmsi$actes$NOMENCLATURE <- NA_character_
  }

  actes_reference <- edsan_reference("actes")
  cim10_reference <- edsan_reference("cim10")

  pmsi$actes <- pmsi$actes %>%
    dplyr::select(-dplyr::any_of("CODEACTE_LABEL")) %>%
    dplyr::left_join(
      actes_reference,
      by = c("NOMENCLATURE", "CODEACTE")
    )

  pmsi$diag <- pmsi$diag %>%
    dplyr::select(-dplyr::any_of("CODE_LABEL")) %>%
    dplyr::left_join(cim10_reference, by = c("diag" = "CODE"))

  pmsi
}
