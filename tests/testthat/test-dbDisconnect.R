context("Disconnect")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

s3.location <- paste0(Sys.getenv("noctua_s3_tbl"),"removable_table/")

test_that("Check if dbDisconnect working as intended",{
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena(),
                   s3_staging_dir = Sys.getenv("noctua_s3_query"))
  
  res <- dbSendQuery(con, "select 'dummy'")
  
  dbDisconnect(con)
  
  df <- data.frame(x = 1:10, y = letters[1:10], stringsAsFactors = F)
  
  expect_equal(dbIsValid(con), FALSE)
  expect_equal(dbIsValid(res), FALSE)
  expect_error(dbGetQuery(con, "select dummy"), "Connection already closed.")
  expect_error(dbFetch(res), "Result already cleared.")
  expect_error(con_error_msg(con, "dummy message."), "dummy message.")
  expect_error(dbExistsTable(con, "removable_table"))
  expect_error(dbWriteTable(con, "removable_table", df, s3.location = s3.location))
  expect_error(dbRemoveTable(con, "removable_table"))
  expect_error(dbSendQuery(con, "select * removable_table"))
  expect_error(dbExecute(con, "select * removable_table"))
  expect_error(dbGetQuery(con, "select * reomovable_table"))
})
