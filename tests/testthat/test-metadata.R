context("Athena Metadata")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

df_col_info <- data.frame(field_name = c("w","x","y", "z", "timestamp"),
                          type = c("timestamp", "integer", "varchar", "boolean", "varchar"), stringsAsFactors = F)

test_that("Returning meta data from query",{
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena())
  
  res <- dbExecute(con, "select * from test_df")
  column_info <- dbColumnInfo(res)
  dbClearResult(res)
  dbDisconnect(con)
  
  expect_equal(column_info, df_col_info)
  
  # clean up system environmental variables
  Sys.unsetenv("AWS_ACCESS_KEY_ID")
  Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
  Sys.unsetenv("AWS_SESSION_TOKEN")
  Sys.unsetenv("AWS_PROFILE")
  Sys.unsetenv("AWS_REGION")
})