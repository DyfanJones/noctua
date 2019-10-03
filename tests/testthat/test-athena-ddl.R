context("Athena DDL")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("pawsathena_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("pawsathena_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("pawsathena_s3_tbl"): "s3://path/to/bucket/"

s3.location <- paste0(Sys.getenv("pawsathena_s3_tbl"),"test_df/")
df <- data.frame(x = 1:10, y = letters[1:10], stringsAsFactors = F)

test_that("Check if Athena DDL's are created correctly",{
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena())
  
  expect_ddl1 <- sqlCreateTable(con, "test_df", df, s3.location = s3.location, file.type = "csv")
  expect_ddl2 <- sqlCreateTable(con, "test_df", df, s3.location = s3.location, file.type = "tsv")
  expect_ddl3 <- sqlCreateTable(con, "test_df", df, s3.location = s3.location, file.type = "parquet")
  expect_ddl4 <- sqlCreateTable(con, "test_df", df, partition = "timestamp", s3.location = s3.location, file.type = "parquet")

  expect_equal(expect_ddl1, tbl_ddl$tbl1)
  expect_equal(expect_ddl2, tbl_ddl$tbl2)
  expect_equal(expect_ddl3, tbl_ddl$tbl3)
  expect_equal(expect_ddl4, tbl_ddl$tbl4)
  
  # clean up system environmental variables
  Sys.unsetenv("AWS_ACCESS_KEY_ID")
  Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
  Sys.unsetenv("AWS_SESSION_TOKEN")
  Sys.unsetenv("AWS_PROFILE")
  Sys.unsetenv("AWS_REGION")
})