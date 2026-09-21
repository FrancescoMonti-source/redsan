args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 2L || any(!nzchar(args))) {
  stop(
    paste(
      "Usage: Rscript tools/check_doceds_trimmer_artifact.R",
      "/path/to/python /path/to/versioned/artifact"
    ),
    call. = FALSE
  )
}

python_exe <- args[[1L]]
model_dir <- args[[2L]]
expected_version <- "1.1.0"
expected_contract <- "rectype-aware-v1"
manifest <- jsonlite::fromJSON(file.path(model_dir, "artifact.json"))
stopifnot(
  identical(manifest$artifact_version, expected_version),
  identical(manifest$worker_contract, expected_contract)
)

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
      "FORMCHECKBOX\nHospitalisation pour insuffisance cardiaque.",
      "Furosémide 40 mg. Surveillance du poids et de la créatinine."
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
  "TRIM_IS_BT",
  "TRIM_PRESERVED_INTERVALS"
)
transport_rows <- match(c("bt", "ordon"), result$ELTID)
preserved_rows <- match(c("unrelated", "blank"), result$ELTID)
stopifnot(
  identical(result$ELTID, input$ELTID),
  identical(result$acceptance_order, input$acceptance_order),
  identical(missing_result$ELTID, missing_rectype$ELTID),
  identical(missing_result$acceptance_order, missing_rectype$acceptance_order),
  all(output_columns %in% names(result)),
  all(output_columns %in% names(missing_result)),
  identical(result$TRIM_IS_BT, c(TRUE, TRUE, FALSE, FALSE)),
  identical(missing_result$TRIM_IS_BT, FALSE),
  all(result$RECTXT_TRIMMED[transport_rows] == ""),
  all(result$TRIM_REDUCTION_PCT[transport_rows] == 100),
  all(result$TRIM_PRESERVED_INTERVALS[transport_rows] == "[]"),
  all(nzchar(result$RECTXT_TRIMMED[preserved_rows])),
  nzchar(missing_result$RECTXT_TRIMMED),
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
