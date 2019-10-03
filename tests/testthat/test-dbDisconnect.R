context("Disconnect")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("pawsathena_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("pawsathena_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("pawsathena_s3_tbl"): "s3://path/to/bucket/"

s3.location <- paste0(Sys.getenv("pawsathena_s3_tbl"),"removable_table/")

test_that("Check if dbDisconnect working as intended",{
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena(),
                   s3_staging_dir = Sys.getenv("pawsathena_s3_query"))
  
  dbDisconnect(con)
  
  df <- data.frame(x = 1:10, y = letters[1:10], stringsAsFactors = F)
  
  expect_equal(dbIsValid(con), FALSE)
  expect_error(dbExistsTable(con, "removable_table"))
  expect_error(dbWriteTable(con, "removable_table", df, s3.location = s3.location))
  expect_error(dbRemoveTable(con, "removable_table"))
  expect_error(dbSendQuery(con, "select * removable_table"))
  expect_error(dbExecute(con, "select * removable_table"))
  expect_error(dbGetQuery(con, "select * reomovable_table"))
  
  # clean up system environmental variables
  Sys.unsetenv("AWS_ACCESS_KEY_ID")
  Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
  Sys.unsetenv("AWS_SESSION_TOKEN")
  Sys.unsetenv("AWS_PROFILE")
  Sys.unsetenv("AWS_REGION")
})