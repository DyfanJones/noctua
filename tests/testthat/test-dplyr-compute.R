context("dplyr compute")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("pawsathena_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("pawsathena_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("pawsathena_s3_tbl"): "s3://path/to/bucket/"

library(dplyr)
test_that("Check pawsathena s3 dplyr compute method",{
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena())
  
  athena_tbl <- tbl(con, sql("SELECT * FROM INFORMATION_SCHEMA.TABLES"))
  athena_tbl %>% compute("compute_tbl1", s3_location = paste0(Sys.getenv("pawsathena_s3_tbl"),"compute_tbl/"))
  athena_tbl %>% compute("compute_tbl2")
  result1 <- dbExistsTable(con, "compute_tbl1")
  result2 <- dbExistsTable(con, "compute_tbl2")
  
  # clean up athena
  dbRemoveTable(con, "compute_tbl1")
  dbRemoveTable(con, "compute_tbl2")
  
  # clean s3: Athena is unable to clean up s3
  delete_prefix_objects_v2(con, "compute_tbl/")
  
  expect_equal(result1, TRUE)
  expect_equal(result2, TRUE)
})
