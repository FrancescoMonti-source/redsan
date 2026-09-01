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

test_that("empty IEP input never opens CORA", {
  expect_identical(nrow(get_cora_diet(character())), 0L)
})
