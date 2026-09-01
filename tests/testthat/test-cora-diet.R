test_that("CORA IEP validation is strict", {
  expect_identical(
    redsan:::.cora_validate_ieps(c(" 745068610 ", "745068610")),
    "745068610"
  )
  expect_error(redsan:::.cora_validate_ieps(745068610), "character")
  expect_error(
    redsan:::.cora_validate_ieps(c("745068610", NA_character_)),
    "missing"
  )
  expect_error(redsan:::.cora_validate_ieps("IEP-1"), "digits only")
})

test_that("CORA stay identifier validation supports IEP and EVTID", {
  expect_identical(
    redsan:::.cora_validate_stay_ids(c(" 745068610 ", "745068610"), "IEP"),
    "745068610"
  )
  expect_identical(
    redsan:::.cora_validate_stay_ids(c("199196450", "199196450"), "EVTID"),
    "199196450"
  )
  expect_error(redsan:::.cora_validate_stay_ids(199196450, "EVTID"), "character")
  expect_error(redsan:::.cora_validate_stay_ids("EVT-1", "EVTID"), "digits only")
})

test_that("CORA Diet maps EVTID input to IEP", {
  fake_reidentify <- function(ids, id_type, env, ks_path) {
    expect_identical(id_type, "EVTID")
    tibble::tibble(EVTID = ids, IEP = "745068610")
  }

  out <- redsan:::.cora_diet_id_map(
    "199196450",
    id_type = "EVTID",
    reidentify = fake_reidentify
  )

  expect_identical(out$EVTID, "199196450")
  expect_identical(out$IEP, "745068610")
})

test_that("CORA Diet maps IEP input to EVTID", {
  fake_pseudonymize <- function(ids, id_type, env, ks_path) {
    expect_identical(id_type, "IEP")
    tibble::tibble(
      HIS_ID = ids,
      HIS_TYPE = "IEP",
      EDSAN_ID = "199196450",
      EDSAN_TYPE = "EVTID",
      status = "matched",
      n_matches = 1L
    )
  }

  out <- redsan:::.cora_diet_id_map(
    "745068610",
    id_type = "IEP",
    pseudonymize = fake_pseudonymize
  )

  expect_identical(out$EVTID, "199196450")
  expect_identical(out$IEP, "745068610")
})

test_that("CORA Diet keeps IEP when CT reports no EVTID", {
  fake_pseudonymize <- function(ids, id_type, env, ks_path) {
    tibble::tibble(
      HIS_ID = ids,
      HIS_TYPE = "IEP",
      EDSAN_ID = NA_character_,
      EDSAN_TYPE = "EVTID",
      status = "not_found",
      n_matches = 0L
    )
  }

  out <- redsan:::.cora_diet_id_map(
    "745068610",
    id_type = "IEP",
    pseudonymize = fake_pseudonymize
  )

  expect_true(is.na(out$EVTID))
  expect_identical(out$IEP, "745068610")
})

test_that("CORA document keys reject unsafe values", {
  expect_identical(
    redsan:::.cora_validate_document_key("0270663684", "W"),
    list(nodocument = "0270663684", typedoc = "W")
  )
  expect_error(
    redsan:::.cora_validate_document_key("0270663684' OR 1=1 --", "W"),
    "Invalid"
  )
  expect_error(
    redsan:::.cora_validate_document_key("0270663684", "WW"),
    "Invalid"
  )
})

test_that("hex conversion preserves bytes", {
  expect_identical(
    redsan:::.cora_hex_to_raw("1F8B0800"),
    as.raw(c(0x1f, 0x8b, 0x08, 0x00))
  )
})

test_that("CORA GZIP payloads decode to UTF-8 text", {
  original <- charToRaw(iconv(
    "Poids actuel 75 Kg\nDénutrition modérée",
    from = "UTF-8",
    to = "latin1"
  ))
  compressed <- memCompress(original, type = "gzip")

  expect_identical(
    redsan:::.cora_decode_gzip(compressed),
    "Poids actuel 75 Kg\nDénutrition modérée"
  )
})

test_that("non-GZIP CORA payloads fail explicitly", {
  expect_error(
    redsan:::.cora_decode_gzip(charToRaw("plain text")),
    "expected GZIP"
  )
})

test_that("empty stay input never opens CORA or CT", {
  expect_identical(nrow(get_cora_diet(character(), id_type = "IEP")), 0L)
  expect_identical(nrow(get_cora_diet(character(), id_type = "EVTID")), 0L)
})
