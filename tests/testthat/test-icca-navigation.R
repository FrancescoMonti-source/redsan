test_that("ICCA source names are schema-qualified and aliases stay convenient", {
  expect_identical(
    redsan:::.icca_normalize_source("assessment")$qualified,
    "DAR.PtAssessment"
  )
  expect_identical(
    redsan:::.icca_normalize_source("CISReportingDB.DAR.PatientAssessment")$qualified,
    "DAR.PatientAssessment"
  )
  expect_error(redsan:::.icca_normalize_source("PatientAssessment"), "schema.object")
})

test_that("ICCA link choice prefers encounterId and refuses ambiguous guesses", {
  object <- tibble::tibble(
    has_encounter_id = TRUE,
    has_patient_id = TRUE,
    has_episode_id = FALSE,
    has_system_id = TRUE
  )
  expect_identical(redsan:::.icca_choose_link(object, "auto"), "encounterId")
  expect_identical(redsan:::.icca_choose_link(object, "patientId"), "patientId")

  ambiguous <- tibble::tibble(
    has_encounter_id = FALSE,
    has_patient_id = TRUE,
    has_episode_id = TRUE,
    has_system_id = FALSE
  )
  expect_error(redsan:::.icca_choose_link(ambiguous, "auto"), "several possible")
})

test_that("generic ICCA retrieval queries any encounter-linked object", {
  seen <- NULL
  fake_reidentify <- function(ids, id_type, env, ks_path) {
    tibble::tibble(
      EDSAN_ID = ids,
      EDSAN_TYPE = "EVTID",
      HIS_ID = "IEP-1",
      HIS_TYPE = "IEP",
      status = "matched",
      n_matches = 1L
    )
  }
  fake_metadata <- function(source, connection, query) {
    tibble::tibble(
      schema_name = "DAR",
      object_name = "PatientVentilation",
      type_desc = "VIEW",
      n_columns = 43L,
      has_encounter_id = TRUE,
      has_patient_id = FALSE,
      has_episode_id = FALSE,
      has_system_id = TRUE
    )
  }
  fake_query <- function(sql, params = NULL, connection = NULL) {
    if (grepl("D_Encounter", sql, fixed = TRUE)) {
      return(tibble::tibble(
        encounterId = 11L,
        patientId = 1L,
        episodeId = 101L,
        encounterNumber = "IEP-1",
        gender = "F",
        primaryDiagnosis = NA_character_,
        isArchived = FALSE,
        systemId = 7L
      ))
    }
    seen <<- list(sql = sql, params = params)
    tibble::tibble(encounterId = 11L, chartTime = as.POSIXct("2026-01-01"), value = 5)
  }

  out <- redsan:::.icca_get_source(
    "EVT-1",
    source = "DAR.PatientVentilation",
    reidentify = fake_reidentify,
    query = fake_query,
    metadata = fake_metadata
  )

  expect_match(seen$sql, "DAR].[PatientVentilation", fixed = TRUE)
  expect_identical(seen$params, 11L)
  expect_identical(out$EVTID, "EVT-1")
  expect_identical(out$value, 5)
})

test_that("generic retrieval can use another D_Encounter anchor explicitly", {
  object <- tibble::tibble(
    has_encounter_id = FALSE,
    has_patient_id = TRUE,
    has_episode_id = TRUE,
    has_system_id = FALSE
  )
  expect_identical(redsan:::.icca_choose_link(object, "patientId"), "patientId")
})

test_that("public get_icca no longer whitelists clinical sources", {
  expect_s3_class(get_icca(character(), source = "DAR.PatientVentilation"), "tbl_df")
  expect_s3_class(get_icca(character(), source = "dbo.PtLabResult"), "tbl_df")
  expect_s3_class(get_icca(character(), source = "assessment"), "tbl_df")
  expect_s3_class(get_icca(character(), source = "medication"), "tbl_df")
})
