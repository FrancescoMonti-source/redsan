#' List reference mappings distributed with redsan
#'
#' Returns the catalogue of code-to-label mappings available through
#' [edsan_reference()]. These mappings describe EDSAN source codes, national
#' nomenclatures, and local organizational codes. They contain no patient data.
#'
#' @return A tibble with the reference `name`, its `scope`, its logical `key`,
#'   and a short `description`.
#'
#' @examples
#' edsan_references()
#'
#' @export
edsan_references <- function() {
  tibble::tibble(
    name = c("actes", "bio", "ccam", "cdam", "cim10", "uf", "uf2um", "um"),
    scope = c(
      "derived", "edsan", "national", "national",
      "national", "local", "local", "local"
    ),
    key = c(
      "NOMENCLATURE + CODEACTE", "TYPEANA", "CODEACTE", "CODEACTE",
      "CODE", "SEJUF", "SEJUF", "SEJUM"
    ),
    description = c(
      "Combined CCAM and CDAM acts with explicit nomenclature",
      "Biology analyte codes",
      "CCAM procedure codes",
      "CDAM procedure codes",
      "CIM-10 diagnosis codes",
      "Functional unit codes",
      "Functional-unit to medical-unit mapping",
      "Medical unit codes"
    )
  )
}

.edsan_reference_columns <- function(name) {
  switch(
    name,
    bio = c("TYPEANA", "TYPEANA_LABEL"),
    ccam = c("CODEACTE", "CODEACTE_LABEL"),
    cdam = c("CODEACTE", "CODEACTE_LABEL"),
    cim10 = c("CODE", "CODE_LABEL"),
    uf = c("SEJUF", "SEJUF_LABEL"),
    uf2um = c("SEJUF", "SEJUM"),
    um = c("SEJUM", "SEJUM_LABEL")
  )
}

.edsan_read_reference <- function(name) {
  path <- system.file(
    "extdata", "reference", paste0("ref_", name, ".txt"),
    package = "redsan",
    mustWork = TRUE
  )

  out <- utils::read.delim(
    path,
    sep = ";",
    header = TRUE,
    quote = "",
    comment.char = "",
    colClasses = "character",
    fileEncoding = "UTF-8",
    na.strings = "",
    fill = TRUE,
    check.names = FALSE,
    stringsAsFactors = FALSE
  )

  if (ncol(out) != 2L) {
    stop(
      "Reference `", name, "` must contain exactly two columns.",
      call. = FALSE
    )
  }

  expected <- .edsan_reference_columns(name)
  if (!identical(names(out), expected)) {
    stop(
      "Reference `", name, "` must have columns ",
      paste(expected, collapse = ", "),
      "; found ", paste(names(out), collapse = ", "), ".",
      call. = FALSE
    )
  }

  out <- tibble::as_tibble(out)
  key <- expected[[1L]]
  if (anyDuplicated(out[[key]])) {
    stop(
      "Reference `", name, "` must contain one row per `", key, "`.",
      call. = FALSE
    )
  }

  out
}

#' Read a reference mapping distributed with redsan
#'
#' Returns one normalized reference table. Reference keys must be unique.
#' Native codes are kept as character, including leading zeroes.
#'
#' `actes` is derived from the separate `ccam` and `cdam` references and adds a
#' `nomenclature` column. It is not stored as an independent source of truth.
#'
#' @param name One reference name listed by [edsan_references()].
#'
#' @return A tibble preserving native reference headers: `TYPEANA` for biology,
#'   `CODEACTE` for acts, `CODE` for CIM-10, and `SEJUF` / `SEJUM` for
#'   organizational mappings. Label columns use the corresponding `_LABEL`
#'   suffix. The derived `actes` reference adds `NOMENCLATURE`.
#'
#' @examples
#' cim10 <- edsan_reference("cim10")
#' utils::head(cim10)
#'
#' actes <- edsan_reference("actes")
#' utils::head(actes)
#'
#' @export
edsan_reference <- function(name) {
  available <- edsan_references()$name

  if (!is.character(name) || length(name) != 1L || is.na(name) ||
      !nzchar(name)) {
    stop("`name` must be one non-missing reference name.", call. = FALSE)
  }
  if (!name %in% available) {
    stop(
      "Unknown EDSAN reference: ", name,
      ". Available references: ", paste(available, collapse = ", "), ".",
      call. = FALSE
    )
  }

  if (identical(name, "actes")) {
    ccam <- .edsan_read_reference("ccam")
    ccam$NOMENCLATURE <- "CCAM"
    cdam <- .edsan_read_reference("cdam")
    cdam$NOMENCLATURE <- "CDAM"

    return(dplyr::select(
      dplyr::bind_rows(ccam, cdam),
      dplyr::all_of(c("NOMENCLATURE", "CODEACTE", "CODEACTE_LABEL"))
    ))
  }

  .edsan_read_reference(name)
}
