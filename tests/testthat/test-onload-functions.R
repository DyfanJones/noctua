
test_that("test readr check", {
  skip_if_no_env()
  
  expect_null(noctua:::readr_check())
})

test_that("test if dbplyr major and minor versions are collected correctly", {
  skip_if_no_env()
  skip_if_package_not_avialable("dbplyr")
  
  library(dbplyr)
  
  dbplyr_major = packageVersion("dbplyr")$major
  dbplyr_minor = packageVersion("dbplyr")$minor
  
  noctua:::dbplyr_version()
  
  expect_equal(noctua:::dbplyr_env$major, dbplyr_major)
  expect_equal(noctua:::dbplyr_env$minor, dbplyr_minor)
})
