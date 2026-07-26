#' Map header-like structure in document text
#'
#' Splits document text into lines and identifies short uppercase or
#' colon-terminated lines as header candidates. The function returns both
#' normalized candidate labels and their document-level sequences. It does not
#' assign clinical meaning to any label.
#'
#' @param data A data frame containing document text.
#' @param text_col Name of the column containing document text.
#' @param id_col Name of the source-element identifier column. Defaults to
#'   `ELTID` for DOCEDS data.
#' @param metadata_cols Columns retained in line-level outputs. By default,
#'   available DOCEDS provenance and context columns are retained:
#'   `PATID`, `EVTID`, `id_col`, `RECDATE`, `RECTYPE`, `SEJUM`, and `SEJUF`.
#' @param max_line_chars Maximum length of a line to consider as a candidate.
#' @param min_header_chars Minimum length of a normalized candidate label.
#' @param n_examples Maximum number of distinct raw variants retained in the
#'   `examples` column.
#' @param noise_pattern Optional regular expression applied after the minimum
#'   length filter. When `NULL`, no additional candidate labels are removed.
#'
#' @return A list with:
#'   * `lines`: one row per source-text line, including the requested metadata;
#'   * `candidate_hits`: candidate labels observed in each document;
#'   * `candidate_lines_raw`: candidate frequencies and raw `examples`;
#'   * `candidate_lines`: candidates after `min_header_chars` and optional
#'     `noise_pattern` filtering;
#'   * `header_hits`: line-level occurrences of filtered candidates;
#'   * `doc_signatures`: candidate sequence for each document;
#'   * `signature_counts`: frequency of each document-level sequence.
#'
#' @examples
#' documents <- tibble::tibble(
#'   PATID = c("P1", "P2"),
#'   EVTID = c("E1", "E2"),
#'   ELTID = c("L1", "L2"),
#'   RECTXT = c(
#'     "ANTÉCÉDENTS\nEXAMEN CLINIQUE:\nTexte 1",
#'     "ANTECEDENTS\nEXAMEN CLINIQUE:\nTexte 2"
#'   )
#' )
#'
#' mapped <- map_header_structure(documents)
#' mapped$candidate_lines_raw
#' mapped$candidate_lines
#'
#' @importFrom rlang .data
#' @export
map_header_structure <- function(
  data,
  text_col = "RECTXT",
  id_col = "ELTID",
  metadata_cols = intersect(
    c("PATID", "EVTID", id_col, "RECDATE", "RECTYPE", "SEJUM", "SEJUF"),
    names(data)
  ),
  max_line_chars = 100L,
  min_header_chars = 5L,
  n_examples = 5L,
  noise_pattern = NULL
) {
  if (!is.data.frame(data)) {
    stop("map_header_structure() requires a data frame.", call. = FALSE)
  }

  if (length(text_col) != 1L || !is.character(text_col) || is.na(text_col)) {
    stop("`text_col` must be one non-missing column name.", call. = FALSE)
  }

  if (length(id_col) != 1L || !is.character(id_col) || is.na(id_col)) {
    stop("`id_col` must be one non-missing column name.", call. = FALSE)
  }

  if (!is.character(metadata_cols)) {
    stop("`metadata_cols` must be a character vector.", call. = FALSE)
  }

  metadata_cols <- unique(metadata_cols)
  missing_cols <- setdiff(c(text_col, id_col, metadata_cols), names(data))
  if (length(missing_cols) > 0L) {
    stop(
      "Missing columns: ",
      paste(missing_cols, collapse = ", "),
      call. = FALSE
    )
  }

  if (text_col %in% metadata_cols) {
    stop("`text_col` cannot also be in `metadata_cols`.", call. = FALSE)
  }

  if ("doc_id" %in% metadata_cols) {
    stop("`doc_id` is reserved for the standardized source ID.", call. = FALSE)
  }

  if (
    length(max_line_chars) != 1L ||
      !is.numeric(max_line_chars) ||
      is.na(max_line_chars) ||
      max_line_chars < 1 ||
      max_line_chars != floor(max_line_chars)
  ) {
    stop("`max_line_chars` must be one positive integer.", call. = FALSE)
  }

  if (
    length(min_header_chars) != 1L ||
      !is.numeric(min_header_chars) ||
      is.na(min_header_chars) ||
      min_header_chars < 1 ||
      min_header_chars != floor(min_header_chars)
  ) {
    stop("`min_header_chars` must be one positive integer.", call. = FALSE)
  }

  if (
    length(n_examples) != 1L ||
      !is.numeric(n_examples) ||
      is.na(n_examples) ||
      n_examples < 0 ||
      n_examples != floor(n_examples)
  ) {
    stop("`n_examples` must be one non-negative integer.", call. = FALSE)
  }

  if (
    !is.null(noise_pattern) &&
      (length(noise_pattern) != 1L ||
        !is.character(noise_pattern) ||
        is.na(noise_pattern))
  ) {
    stop("`noise_pattern` must be NULL or one non-missing regex.", call. = FALSE)
  }

  max_line_chars <- as.integer(max_line_chars)
  min_header_chars <- as.integer(min_header_chars)
  n_examples <- as.integer(n_examples)
  source_cols <- unique(c(metadata_cols, id_col, text_col))

  line_tbl <- data %>%
    tibble::as_tibble() %>%
    dplyr::select(dplyr::all_of(source_cols)) %>%
    dplyr::mutate(
      doc_id = .data[[id_col]],
      text = stringr::str_replace_all(
        as.character(.data[[text_col]]),
        "\r\n?",
        "\n"
      )
    ) %>%
    dplyr::select(dplyr::all_of(c(metadata_cols, "doc_id", "text"))) %>%
    dplyr::mutate(lines = stringr::str_split(.data$text, "\n")) %>%
    dplyr::select(-dplyr::all_of("text")) %>%
    tidyr::unnest_longer(
      "lines",
      indices_to = "line_no",
      values_to = "line_raw"
    ) %>%
    dplyr::mutate(
      line_clean = stringr::str_squish(.data$line_raw),
      is_blank = is.na(.data$line_clean) | .data$line_clean == "",
      is_short = nchar(.data$line_clean) <= max_line_chars,
      is_upper = !.data$is_blank &
        stringr::str_detect(.data$line_clean, "[[:alpha:]]") &
        .data$line_clean == stringr::str_to_upper(.data$line_clean),
      ends_colon = stringr::str_detect(.data$line_clean, ":$"),
      line_key = stringr::str_to_lower(
        stringi::stri_trans_general(
          stringr::str_squish(.data$line_clean),
          "Latin-ASCII"
        )
      )
    )

  candidate_hits <- line_tbl %>%
    dplyr::filter(
      !.data$is_blank,
      .data$is_short,
      .data$is_upper | .data$ends_colon
    ) %>%
    dplyr::distinct(.data$doc_id, .data$line_key, .data$line_clean)

  candidate_lines_raw <- candidate_hits %>%
    dplyr::distinct(.data$doc_id, .data$line_key) %>%
    dplyr::count(.data$line_key, name = "n_docs") %>%
    dplyr::left_join(
      candidate_hits %>%
        dplyr::group_by(.data$line_key) %>%
        dplyr::summarise(
          n_variants = dplyr::n_distinct(.data$line_clean),
          examples = stringr::str_c(
            utils::head(sort(unique(.data$line_clean)), n_examples),
            collapse = " | "
          ),
          .groups = "drop"
        ),
      by = "line_key"
    ) %>%
    dplyr::arrange(dplyr::desc(.data$n_docs), .data$line_key)

  candidate_lines <- candidate_lines_raw %>%
    dplyr::filter(nchar(.data$line_key) >= min_header_chars)
  if (!is.null(noise_pattern)) {
    candidate_lines <- candidate_lines %>%
      dplyr::filter(!stringr::str_detect(.data$line_key, noise_pattern))
  }

  header_hits <- line_tbl %>%
    dplyr::semi_join(candidate_lines, by = "line_key") %>%
    dplyr::arrange(.data$doc_id, .data$line_no)

  doc_signatures <- header_hits %>%
    dplyr::group_by(.data$doc_id) %>%
    dplyr::summarise(
      dplyr::across(dplyr::all_of(metadata_cols), dplyr::first),
      header_sequence = stringr::str_c(.data$line_key, collapse = " > "),
      n_header_hits = dplyr::n(),
      .groups = "drop"
    )

  signature_counts <- doc_signatures %>%
    dplyr::count(.data$header_sequence, sort = TRUE)

  list(
    lines = line_tbl,
    candidate_hits = candidate_hits,
    candidate_lines_raw = candidate_lines_raw,
    candidate_lines = candidate_lines,
    header_hits = header_hits,
    doc_signatures = doc_signatures,
    signature_counts = signature_counts
  )
}
