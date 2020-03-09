context("Athena DDL")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

s3.location <- paste0(Sys.getenv("noctua_s3_tbl"),"test_df/")
df <- data.frame(x = 1:10, y = letters[1:10], stringsAsFactors = F)

test_that("Check if Athena DDL's are created correctly",{
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena())
  
  # CSV DDL
  expect_ddl1 <- sqlCreateTable(con, "test_df", df, s3.location = s3.location, file.type = "csv")
  expect_ddl2 <- sqlCreateTable(con, "test_df", df, s3.location = s3.location, file.type = "csv", compress = T)
  
  # TSV DDL
  expect_ddl3 <- sqlCreateTable(con, "test_df", df, s3.location = s3.location, file.type = "tsv")
  expect_ddl4 <- sqlCreateTable(con, "test_df", df, s3.location = s3.location, file.type = "tsv", compress = T)
  
  # Parquet DDL
  expect_ddl5 <- sqlCreateTable(con, "test_df", df, s3.location = s3.location, file.type = "parquet")
  expect_ddl6 <- sqlCreateTable(con, "test_df", df, partition = "timestamp", s3.location = s3.location, file.type = "parquet", compress = T)
  
  # JSON DDL
  expect_ddl7 <- sqlCreateTable(con, "test_df", df, s3.location = s3.location, file.type = "json")
  expect_ddl8 <- sqlCreateTable(con, "test_df", df, partition = "timestamp", s3.location = s3.location, file.type = "json", compress = T)
  
  
  expect_equal(expect_ddl1, tbl_ddl$tbl1)
  expect_equal(expect_ddl2, tbl_ddl$tbl2)
  expect_equal(expect_ddl3, tbl_ddl$tbl3)
  expect_equal(expect_ddl4, tbl_ddl$tbl4)
  expect_equal(expect_ddl5, tbl_ddl$tbl5)
  expect_equal(expect_ddl6, tbl_ddl$tbl6)
  expect_equal(expect_ddl7, tbl_ddl$tbl7)
  expect_equal(expect_ddl8, tbl_ddl$tbl8)
})