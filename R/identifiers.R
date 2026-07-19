#' Count distinct EDSAN identifiers
#'
#' Reports the number of distinct values in identifier columns. Column matching
#' is case-insensitive, while the printed labels preserve the names found in
#' `data`.
#'
#' @param data A data frame containing EDSAN identifiers.
#' @param cols Character vector of identifier columns to count. When omitted,
#'   available columns among `PATID`, `EVTID`, `ELTID`, `BIOL_ID`, `DOC_ID`,
#'   and `PMSI_ID` are detected case-insensitively.
#'
#' @return `NULL`, invisibly. The counts are printed to the console.
#'
#' @examples
#' data <- data.frame(
#'   PATID = c(1, 1, 2, 3),
#'   EVTID = c(101, 102, 101, 103),
#'   ELTID = c(201, 202, 201, 203)
#' )
#' count_idtriplets(data)
#'
#' @export
count_idtriplets <- function(
  data,
  cols = c("patid", "evtid", "eltid", "biol_id", "doc_id", "pmsi_id")
) {
  data_colnames <- tolower(names(data))

  if (missing(cols)) {
    cols <- intersect(cols, data_colnames)
  } else {
    cols <- tolower(cols)
  }

  for (col in cols) {
    col_index <- match(col, data_colnames)

    if (is.na(col_index)) {
      cat("Column", col, "not found in the data, skipping.\n")
      next
    }

    col_name <- names(data)[col_index]
    cat(col_name, ": n =", dplyr::n_distinct(data[[col_name]]), "\n")
  }

  invisible(NULL)
}
