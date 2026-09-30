args <- commandArgs(trailingOnly = TRUE)
if (!length(args) %in% c(2L, 3L) || any(!nzchar(args))) {
  stop(
    paste(
      "Usage: Rscript tools/check_doceds_trimmer_artifact.R",
      "/path/to/python /path/to/versioned/artifact [expected_version]"
    ),
    call. = FALSE
  )
}

python_exe <- args[[1L]]
model_dir <- args[[2L]]
# The version floor and worker contract live in the package validator, so this
# script accepts every release redsan accepts. Pass a version to pin one.
artifact_version <- redsan:::.doceds_onnx_validate_artifact(model_dir)
if (length(args) == 3L && !identical(artifact_version, args[[3L]])) {
  stop(
    sprintf(
      "Artifact version is '%s', but '%s' was expected.",
      artifact_version,
      args[[3L]]
    ),
    call. = FALSE
  )
}
manifest <- jsonlite::fromJSON(file.path(model_dir, "artifact.json"))

input <- data.frame(
  ELTID = c("bt", "ordon", "unrelated", "blank"),
  RECTYPE = c("BT", "ORDON7", "CRH2AB", ""),
  RECTXT = c(
    "FORMCHECKBOX\nBon de transport",
    "FORMCHECKBOX\nBon de transport",
    paste(
      "FORMCHECKBOX\nDiagnostic: pneumopathie. Température 39 C.",
      "Saturation 91 %. Amoxicilline 1 g trois fois par jour."
    ),
    paste(
      "FORMCHECKBOX\nDiagnostic: insuffisance cardiaque.",
      "Poids 72 kg. Furosémide 40 mg."
    )
  ),
  acceptance_order = c(4L, 3L, 2L, 1L),
  stringsAsFactors = FALSE
)
missing_rectype <- data.frame(
  ELTID = "missing",
  RECTXT = paste(
    "FORMCHECKBOX\nAntécédent de diabète traité par metformine",
    "850 mg matin et soir."
  ),
  acceptance_order = 5L,
  stringsAsFactors = FALSE
)

result <- redsan::trim_doceds_onnx(
  input,
  python_exe = python_exe,
  model_dir = model_dir
)
missing_result <- redsan::trim_doceds_onnx(
  missing_rectype,
  python_exe = python_exe,
  model_dir = model_dir
)

output_columns <- c(
  "RECTXT_TRIMMED",
  "TRIM_REDUCTION_PCT",
  "TRIM_PRESERVED_INTERVALS"
)
voucher_rows <- match(c("bt", "ordon"), result$ELTID)
contains_all <- function(text, phrases) {
  all(vapply(phrases, grepl, logical(1), x = text, fixed = TRUE))
}
unrelated_text <- result$RECTXT_TRIMMED[[match("unrelated", result$ELTID)]]
blank_rectype_text <- result$RECTXT_TRIMMED[[match("blank", result$ELTID)]]
missing_rectype_text <- missing_result$RECTXT_TRIMMED[[1L]]
stopifnot(
  identical(result$ELTID, input$ELTID),
  identical(result$RECTYPE, input$RECTYPE),
  identical(result$acceptance_order, input$acceptance_order),
  identical(missing_result$ELTID, missing_rectype$ELTID),
  identical(missing_result$acceptance_order, missing_rectype$acceptance_order),
  all(output_columns %in% names(result)),
  all(output_columns %in% names(missing_result)),
  !"TRIM_IS_BT" %in% names(result),
  !"TRIM_IS_BT" %in% names(missing_result),
  all(result$RECTXT_TRIMMED[voucher_rows] == ""),
  all(result$TRIM_REDUCTION_PCT[voucher_rows] == 100),
  all(result$TRIM_PRESERVED_INTERVALS[voucher_rows] == "[]"),
  contains_all(
    unrelated_text,
    c("Diagnostic: pneumopathie", "Saturation 91 %", "Amoxicilline 1 g")
  ),
  contains_all(
    blank_rectype_text,
    c("Diagnostic: insuffisance cardiaque", "Poids 72 kg", "Furosémide 40 mg")
  ),
  contains_all(
    missing_rectype_text,
    c("diabète", "metformine", "850 mg")
  ),
  all(vapply(result$TRIM_PRESERVED_INTERVALS, jsonlite::validate, logical(1))),
  all(vapply(
    missing_result$TRIM_PRESERVED_INTERVALS,
    jsonlite::validate,
    logical(1)
  ))
)

message(
  "Accepted edsan-doc-trimmer artifact ", manifest$artifact_version,
  " with worker contract ", manifest$worker_contract,
  " through redsan::trim_doceds_onnx()."
)
