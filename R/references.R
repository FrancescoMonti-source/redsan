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
    name = c(
      "actes", "bact", "bio", "ccam", "cdam", "cim10", "csarr", "ghm",
      "modeent", "modesort", "ngap", "rectypes", "uf", "uf2um",
      "uf2umpmsi", "um"
    ),
    scope = c(
      "derived", "edsan", "edsan", "national", "national",
      "national", "national", "national", "national", "national",
      "national", "local", "local", "local", "local", "local"
    ),
    key = c(
      "NOMENCLATURE + CODEACTE", "TYPEANA", "TYPEANA", "CODEACTE", "CODEACTE",
      "CODE", "CODEACTE", "GHM", "MODEENT", "MODESORT",
      "CODEACTE", "RECTYPE", "SEJUF", "SEJUF", "SEJUF", "SEJUM"
    ),
    description = c(
      "Combined CCAM, CDAM, CSARR, and NGAP acts with explicit nomenclature",
      "Bacteriology analyte codes",
      "Biology analyte codes",
      "CCAM procedure codes",
      "CDAM procedure codes",
      "CIM-10 diagnosis codes",
      "CSARR rehabilitation procedure codes",
      "GHM diagnosis-related groups",
      "PMSI admission modes",
      "PMSI discharge modes",
      "NGAP procedure codes",
      "Document type codes",
      "Functional unit codes",
      "Functional-unit to EDSAN medical-unit mapping",
      "Current functional-unit to EDSAN and PMSI medical-unit mapping",
      "Medical unit codes"
    )
  )
}

.edsan_reference_columns <- function(name) {
  switch(
    name,
    bact = c("TYPEANA", "TYPEANA_LABEL"),
    bio = c("TYPEANA", "TYPEANA_LABEL"),
    ccam = c("CODEACTE", "CODEACTE_LABEL"),
    cdam = c("CODEACTE", "CODEACTE_LABEL"),
    cim10 = c("CODE", "CODE_LABEL"),
    csarr = c("CODEACTE", "CODEACTE_LABEL"),
    ghm = c("GHM", "GHM_LABEL"),
    modeent = c("MODEENT", "MODEENT_LABEL"),
    modesort = c("MODESORT", "MODESORT_LABEL"),
    ngap = c("CODEACTE", "CODEACTE_LABEL"),
    rectypes = c("RECTYPE", "RECTYPE_LABEL"),
    uf = c("SEJUF", "SEJUF_LABEL"),
    uf2um = c("SEJUF", "SEJUM"),
    uf2umpmsi = c("SEJUF", "SEJUM", "UM_PMSI"),
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

  expected <- .edsan_reference_columns(name)
  if (ncol(out) != length(expected)) {
    stop(
      "Reference `", name, "` must contain exactly ", length(expected),
      " columns.",
      call. = FALSE
    )
  }

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
#' `actes` is derived from the separate `ccam`, `cdam`, `csarr`, and `ngap`
#' references and adds a `NOMENCLATURE` column. It is not stored as an
#' independent source of truth. The same `CODEACTE` may exist in more than one
#' nomenclature, which is why the combined key includes `NOMENCLATURE`.
#'
#' `bact` covers the bacteriology analytes of the EDSAN `bact` module and is kept
#' separate from `bio`: some `TYPEANA` codes appear in both with a different
#' label, and only the module the rows come from settles which one applies.
#'
#' `uf2umpmsi` is the current local bridge from EDSAN `SEJUF` to both the EDSAN
#' organizational unit (`SEJUM`) and the CORA/PMSI medical unit (`UM_PMSI`). It
#' is a dated local snapshot, not a national PMSI reference and not a substitute
#' for a historized UF-to-UM join when analysing older stays.
#'
#' A reference may carry a missing label for a code that the source system leaves
#' undocumented, as several `rectypes` entries do. Codes are never dropped for
#' that reason.
#'
#' @param name One reference name listed by [edsan_references()].
#'
#' @return A tibble preserving native reference headers: `TYPEANA` for biology
#'   and bacteriology, `CODEACTE` for acts, `CODE` for CIM-10, `GHM` for
#'   diagnosis-related groups, `MODEENT` / `MODESORT` for PMSI stay modes,
#'   `RECTYPE` for document types, and `SEJUF` / `SEJUM` / `UM_PMSI` for local
#'   organizational mappings. Label columns use the corresponding `_LABEL`
#'   suffix. The derived `actes` reference adds `NOMENCLATURE`.
#'
#' @examples
#' cim10 <- edsan_reference("cim10")
#' utils::head(cim10)
#'
#' uf_pmsi <- edsan_reference("uf2umpmsi")
#' utils::head(uf_pmsi)
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
    # One act nomenclature per stored reference. A code may exist in several of
    # them, so `NOMENCLATURE` is what keeps the combined key unique and is why
    # `label_pmsi()` joins on both columns.
    nomenclatures <- c(ccam = "CCAM", cdam = "CDAM", csarr = "CSARR",
                       ngap = "NGAP")
    combined <- lapply(names(nomenclatures), function(reference) {
      out <- .edsan_read_reference(reference)
      out$NOMENCLATURE <- nomenclatures[[reference]]
      out
    })

    return(dplyr::select(
      dplyr::bind_rows(combined),
      dplyr::all_of(c("NOMENCLATURE", "CODEACTE", "CODEACTE_LABEL"))
    ))
  }

  .edsan_read_reference(name)
}
