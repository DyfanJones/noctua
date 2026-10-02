test_that("dbplyr_version records whether dbplyr is available", {
  skip_if_package_not_avialable("dbplyr")

  noctua:::dbplyr_version()

  expect_true(isTRUE(noctua:::dbplyr_env$available))
})

test_that("dbQuoteString auto-detects date/timestamp literals when dbplyr is available", {
  skip_if_no_env()
  skip_if_package_not_avialable("dbplyr")

  # Test connection is using AWS CLI to set profile_name
  con <- dbConnect(athena())

  noctua:::dbplyr_env$available <- TRUE
  expect_equal(dbQuoteString(con, "2020-01-01"), "date '2020-01-01'")
  expect_equal(
    dbQuoteString(con, "2020-01-01 01:02:03"),
    "timestamp '2020-01-01 01:02:03.000'"
  )

  noctua:::dbplyr_env$available <- FALSE
  expect_false(grepl("^date ", dbQuoteString(con, "2020-01-01")))

  # restore for subsequent tests
  noctua:::dbplyr_version()
})
