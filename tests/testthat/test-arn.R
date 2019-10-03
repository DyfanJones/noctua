context("ARN Connection")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("pawsathena_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("pawsathena_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("pawsathena_s3_tbl"): "s3://path/to/bucket/"

test_that("Check connection to Athena using ARN",{
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena(),
                   role_arn = Sys.getenv("pawsathena_arn"),
                   duration_seconds = 1000)
  
  output <- dbGetQuery(con, "show Databases")
  expect_equal(any(grepl("default", output)), TRUE)
  
  # clean up system environmental variables
  Sys.unsetenv("AWS_ACCESS_KEY_ID")
  Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
  Sys.unsetenv("AWS_SESSION_TOKEN")
})
