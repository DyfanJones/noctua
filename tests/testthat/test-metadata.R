context("Athena Metadata")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("pawsathena_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("pawsathena_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("pawsathena_s3_tbl"): "s3://path/to/bucket/"

df_col_info <- data.frame(field_name = c("x","y", "z", "timestamp"),
                          type = c("integer", "varchar", "boolean", "varchar"), stringsAsFactors = F)

test_that("Returning meta data from query",{
  skip_if_no_boto()
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(pawsathena::athena(),
                   profile_name = "pawsathena",
                   s3_staging_dir = Sys.getenv("pawsathena_s3_query"))
  
  res <- dbExecute(con, "select * from test_df")
  column_info <- dbColumnInfo(res)
  dbClearResult(res)
  dbDisconnect(con)
  
  expect_equal(column_info, df_col_info)
})