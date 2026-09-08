test_that("interactive EDSaN credentials are cached in memory only", {
  redsan:::.edsan_ct_clear_session_auth()
  on.exit(redsan:::.edsan_ct_clear_session_auth(), add = TRUE)

  prompts <- 0L
  creds <- redsan:::.edsan_ct_interactive_credentials(
    prompt_user = function() {
      prompts <<- prompts + 1L
      "alice"
    },
    prompt_password = function() {
      prompts <<- prompts + 1L
      "secret"
    }
  )

  expect_identical(creds, list(usr = "alice", pwd = "secret"))
  expect_identical(prompts, 2L)

  cached <- redsan:::.edsan_ct_interactive_credentials(
    prompt_user = function() stop("username prompt should not run"),
    prompt_password = function() stop("password prompt should not run")
  )
  expect_identical(cached, creds)

  redsan:::.edsan_ct_clear_session_auth()
  expect_false(exists(
    "credentials",
    envir = redsan:::.edsan_ct_session_auth,
    inherits = FALSE
  ))
})

test_that("interactive EDSaN URL can come from the desktop environment", {
  old <- Sys.getenv("REDSAN_EDSAN_CT_URL", unset = NA_character_)
  old_option <- getOption("redsan.edsan_ct_url")
  on.exit({
    if (is.na(old)) {
      Sys.unsetenv("REDSAN_EDSAN_CT_URL")
    } else {
      Sys.setenv(REDSAN_EDSAN_CT_URL = old)
    }
    options(redsan.edsan_ct_url = old_option)
  }, add = TRUE)

  options(redsan.edsan_ct_url = NULL)
  Sys.setenv(REDSAN_EDSAN_CT_URL = "https://edsan.example/EDSaNCTService/REST/")

  expect_identical(
    redsan:::.edsan_ct_configured_url(
      env = "edsan-ct",
      ks_path = tempfile("missing-keystore-")
    ),
    "https://edsan.example/EDSaNCTService/REST"
  )
})

test_that("an R option overrides the EDSaN desktop environment URL", {
  old <- Sys.getenv("REDSAN_EDSAN_CT_URL", unset = NA_character_)
  old_option <- getOption("redsan.edsan_ct_url")
  on.exit({
    if (is.na(old)) {
      Sys.unsetenv("REDSAN_EDSAN_CT_URL")
    } else {
      Sys.setenv(REDSAN_EDSAN_CT_URL = old)
    }
    options(redsan.edsan_ct_url = old_option)
  }, add = TRUE)

  Sys.setenv(REDSAN_EDSAN_CT_URL = "https://env.example/REST")
  options(redsan.edsan_ct_url = "https://option.example/REST/")

  expect_identical(
    redsan:::.edsan_ct_configured_url(
      env = "edsan-ct",
      ks_path = tempfile("missing-keystore-")
    ),
    "https://option.example/REST"
  )
})

test_that("EDSaN REST URLs match the d2imr route layout", {
  expect_identical(
    redsan:::.edsan_ct_build_url(
      "https://edsan.example/REST/",
      "getEDSaNToHISCorrespondences",
      "CPAGE",
      "123,456"
    ),
    "https://edsan.example/REST/getEDSaNToHISCorrespondences/CPAGE/123,456"
  )

  expect_identical(
    redsan:::.edsan_ct_build_url(
      "https://edsan.example/REST",
      "getPatientReidentificationInformations",
      api_query = "789"
    ),
    "https://edsan.example/REST/getPatientReidentificationInformations/789"
  )
})

test_that("empty interactive credentials are rejected", {
  redsan:::.edsan_ct_clear_session_auth()
  on.exit(redsan:::.edsan_ct_clear_session_auth(), add = TRUE)

  expect_error(
    redsan:::.edsan_ct_interactive_credentials(
      prompt_user = function() "",
      prompt_password = function() "secret"
    ),
    "cancelled or left empty"
  )
})
