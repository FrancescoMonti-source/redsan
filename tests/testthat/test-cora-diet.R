test_that("CORA IEP validation is strict", {
  expect_identical(redsan:::.cora_validate_ieps(c(" 745068610 ", "745068610")), "745068610")
  expect_error(redsan:::.cora_validate_ieps(745068610), "character")
  expect_error(redsan:::.cora_validate_ieps(c("745068610", NA_character_)), "missing")
  expect_error(redsan:::.cora_validate_ieps("IEP-1"), "digits only")
})

test_that("hex conversion preserves bytes", {
  expect_identical(
    redsan:::.cora_hex_to_raw("1F8B0800"),
    as.raw(c(0x1f, 0x8b, 0x08, 0x00))
  )
})

test_that("Diet blob reader decompresses chunked GZIP payload", {
  original <- charToRaw("Poids actuel 75 Kg\nDénutrition modérée")
  compressed <- memCompress(original, type = "gzip")

  fake_length <- function(connection, nodocument, typedoc) length(compressed)
  fake_chunk <- function(connection, nodocument, typedoc, amount, offset) {
    raw <- compressed[offset:min(length(compressed), offset + amount - 1L)]
    paste(sprintf("%02X", as.integer(raw)), collapse = "")
  }

  old_length <- redsan:::.cora_blob_length
  old_chunk <- redsan:::.cora_blob_chunk_hex
  on.exit({
    assign(".cora_blob_length", old_length, envir = asNamespace("redsan"))
    assign(".cora_blob_chunk_hex", old_chunk, envir = asNamespace("redsan"))
  }, add = TRUE)

  unlockBinding(".cora_blob_length", asNamespace("redsan"))
  assign(".cora_blob_length", fake_length, envir = asNamespace("redsan"))
  lockBinding(".cora_blob_length", asNamespace("redsan"))
  unlockBinding(".cora_blob_chunk_hex", asNamespace("redsan"))
  assign(".cora_blob_chunk_hex", fake_chunk, envir = asNamespace("redsan"))
  lockBinding(".cora_blob_chunk_hex", asNamespace("redsan"))

  out <- redsan:::.cora_read_diet_blob(
    connection = structure(list(), class = "fake_connection"),
    nodocument = "0270663684",
    typedoc = "W",
    chunk_size = 7L
  )

  expect_identical(out, "Poids actuel 75 Kg\nDénutrition modérée")
})

test_that("empty IEP input never opens CORA", {
  expect_identical(nrow(get_cora_diet(character())), 0L)
})
