context("Region parsed")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

test_that("Check if region is passed correctly in dbConnect",{
  skip_if_no_env()
  con1 <- dbConnect(athena())
  con2 <- dbConnect(athena(), region = "us-east-1")

  expect_equal(con1@ptr$Athena$.internal$config$region[1], con1@info$region_name)
  expect_equal(con2@info$region_name, "us-east-1")
  expect_equal(con2@ptr$Athena$.internal$config$region[1], "us-east-1")
})
