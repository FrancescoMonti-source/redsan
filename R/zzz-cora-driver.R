# CORA JDBC driver resolution -------------------------------------------------
#
# Keep the historical Podsan paths while allowing workstation use without
# passing `ojdbc_jar` on every query. An explicit environment override wins.

.cora_default_ojdbc <- function() {
  override <- trimws(Sys.getenv("REDSAN_OJDBC_JAR", unset = ""))

  program_files_x86 <- Sys.getenv("ProgramFiles(x86)", unset = "")
  cora_windows_lib <- if (nzchar(program_files_x86)) {
    file.path(
      program_files_x86,
      "CORA",
      "ENV_PRODUCTION",
      "CORA Admin Recueil",
      "CORA_LI_CCAM",
      "CORA-LI-CCAM_lib"
    )
  } else {
    ""
  }

  windows_candidates <- character()
  if (nzchar(cora_windows_lib) && dir.exists(cora_windows_lib)) {
    windows_candidates <- list.files(
      cora_windows_lib,
      pattern = "^ojdbc.*\\.jar$",
      full.names = TRUE,
      ignore.case = TRUE
    )
  }

  candidates <- unique(c(
    override,
    windows_candidates,
    "/opt/oracle/instantclient_23_26/ojdbc17.jar",
    "/opt/oracle/instantclient_23_26/ojdbc11.jar",
    "/opt/oracle/instantclient_23_26/ojdbc8.jar",
    "/appli/shared/legacy_lib/lib/j/ojdbc6.jar",
    "/appli/shared/legacy_lib/j/ojdbc6.jar"
  ))
  candidates <- candidates[nzchar(candidates)]

  hit <- candidates[file.exists(candidates)]
  if (!length(hit)) {
    stop(
      "Could not find a usable Oracle JDBC driver for CORA. ",
      "Set `REDSAN_OJDBC_JAR` once for this workstation or supply ",
      "`ojdbc_jar` explicitly.",
      call. = FALSE
    )
  }

  normalizePath(hit[[1L]], winslash = "/", mustWork = TRUE)
}
