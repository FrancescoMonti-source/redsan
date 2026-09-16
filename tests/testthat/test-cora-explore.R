test_that("cora_describe_table uses qualified Oracle metadata", {
  seen_sql <- NULL
  query <- function(sql, connection = NULL, ojdbc_jar = NULL) {
    seen_sql <<- sql
    tibble::tibble(
      OWNER = c("CORA_REC", "CORA_REC"),
      TABLE_NAME = c("TB_SEJOUR", "TB_SEJOUR"),
      COLUMN_ID = c(1, 2),
      COLUMN_NAME = c("ID_SEJOUR", "ID_PATIENT"),
      DATA_TYPE = c("NUMBER", "NUMBER"),
      NULLABLE = c("N", "N"),
      DATA_LENGTH = c(22, 22),
      DATA_PRECISION = c(8, 8),
      DATA_SCALE = c(0, 0),
      CHAR_LENGTH = c(0, 0)
    )
  }

  out <- redsan:::.cora_describe_table(
    "cora_rec.tb_sejour",
    query = query
  )

  expect_match(seen_sql, "UPPER\\(table_name\\) = 'TB_SEJOUR'")
  expect_match(seen_sql, "UPPER\\(owner\\) = 'CORA_REC'")
  expect_identical(
    names(out),
    c(
      "COLUMN_ID", "COLUMN_NAME", "DATA_TYPE", "NULLABLE",
      "DATA_LENGTH", "DATA_PRECISION", "DATA_SCALE", "CHAR_LENGTH"
    )
  )
  expect_identical(out$COLUMN_NAME, c("ID_SEJOUR", "ID_PATIENT"))
})

test_that("cora_describe_table rejects ambiguous unqualified names", {
  query <- function(sql, connection = NULL, ojdbc_jar = NULL) {
    tibble::tibble(
      OWNER = c("CORA_REC", "OTHER"),
      TABLE_NAME = c("TB_SEJOUR", "TB_SEJOUR"),
      COLUMN_ID = c(1, 1),
      COLUMN_NAME = c("ID_SEJOUR", "ID_SEJOUR"),
      DATA_TYPE = c("NUMBER", "NUMBER"),
      NULLABLE = c("N", "N"),
      DATA_LENGTH = c(22, 22),
      DATA_PRECISION = c(8, 8),
      DATA_SCALE = c(0, 0),
      CHAR_LENGTH = c(0, 0)
    )
  }

  expect_error(
    redsan:::.cora_describe_table("TB_SEJOUR", query = query),
    "multiple accessible schemas"
  )
})

test_that("cora_describe_table rejects unsupported identifiers", {
  expect_error(
    redsan:::.cora_describe_table(
      "CORA_REC.TB_SEJOUR;DROP",
      query = function(...) stop("should not query")
    ),
    "standard unquoted Oracle identifier"
  )
})

test_that("cora_dig searches all accessible schemas by default", {
  seen_sql <- NULL
  query <- function(sql, connection = NULL, ojdbc_jar = NULL) {
    seen_sql <<- sql
    tibble::tibble(
      OWNER = "CORA_REC",
      OBJECT_TYPE = "TABLE",
      TABLE_NAME = "TB_SEJOUR",
      COLUMN_NAME = "ID_SEJOUR",
      DATA_TYPE = "NUMBER",
      MATCHED_ON = "COLUMN",
      KEY_TYPE = "PK"
    )
  }

  out <- redsan:::.cora_dig("id_sejour", query = query)

  expect_match(seen_sql, "INSTR\\(UPPER\\(c.column_name\\), 'ID_SEJOUR'\\) > 0")
  expect_match(seen_sql, "constraint_type = 'P'")
  expect_false(grepl("CORA_REC", seen_sql, fixed = TRUE))
  expect_identical(out$KEY_TYPE, "PK")
})

test_that("cora_dig can scope owner and require exact matches", {
  seen_sql <- NULL
  query <- function(sql, connection = NULL, ojdbc_jar = NULL) {
    seen_sql <<- sql
    tibble::tibble(
      OWNER = character(),
      OBJECT_TYPE = character(),
      TABLE_NAME = character(),
      COLUMN_NAME = character(),
      DATA_TYPE = character(),
      MATCHED_ON = character(),
      KEY_TYPE = character()
    )
  }

  redsan:::.cora_dig(
    "TB_SEJOUR",
    owner = "cora_rec",
    exact = TRUE,
    query = query
  )

  expect_match(seen_sql, "UPPER\\(o.object_name\\) = 'TB_SEJOUR'")
  expect_match(seen_sql, "UPPER\\(o.owner\\) = 'CORA_REC'")
  expect_match(seen_sql, "UPPER\\(c.owner\\) = 'CORA_REC'")
})
