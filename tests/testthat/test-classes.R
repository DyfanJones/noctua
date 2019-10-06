context("classes")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

test_that("Testing class formation", {
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena())

  res <- dbSendQuery(con, "show databases")
  DBI::dbClearResult(res)

  # testing components of s4 class
  expect_identical(names(attributes(con)), c("ptr", "info","class"))
  expect_identical(names(attributes(res)), c("connection", "info", "class"))
  expect_s4_class(con,"AthenaConnection")
  expect_s4_class(res,"AthenaResult")
  
  # clean up system environmental variables
  Sys.unsetenv("AWS_ACCESS_KEY_ID")
  Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
  Sys.unsetenv("AWS_SESSION_TOKEN")
  Sys.unsetenv("AWS_PROFILE")
  Sys.unsetenv("AWS_REGION")
})
