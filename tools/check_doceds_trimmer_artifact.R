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

input <- data.frame(
  ELTID = c("acceptance-1", "acceptance-2"),
  RECTYPE = c("CRH", "AUTRE"),
  RECTXT = c(
    "Compte rendu clinique. Le patient reste stable.",
    "Observation clinique sans complication."
  ),
  acceptance_order = c(2L, 1L),
  stringsAsFactors = FALSE
)

result <- redsan::trim_doceds_onnx(
  input,
  python_exe = python_exe,
  model_dir = model_dir
)

output_columns <- c(
  "RECTXT_TRIMMED",
  "TRIM_REDUCTION_PCT",
  "TRIM_IS_BT",
  "TRIM_PRESERVED_INTERVALS"
)
stopifnot(
  identical(result$ELTID, input$ELTID),
  identical(result$acceptance_order, input$acceptance_order),
  all(output_columns %in% names(result)),
  all(vapply(result$TRIM_PRESERVED_INTERVALS, jsonlite::validate, logical(1)))
)

manifest <- jsonlite::fromJSON(file.path(model_dir, "artifact.json"))
message(
  "Accepted edsan-doc-trimmer artifact ", manifest$artifact_version,
  " through redsan::trim_doceds_onnx()."
)
