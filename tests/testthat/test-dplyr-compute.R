context("dplyr compute")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

library(dplyr)
test_that("Check noctua s3 dplyr compute method",{
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena())
  
  # remove test tables if exist
  if(dbExistsTable(con, "compute_tbl1"))
    dbRemoveTable(con, "compute_tbl1", confirm = TRUE)
  if(dbExistsTable(con, "compute_tbl2"))
    dbRemoveTable(con, "compute_tbl2", confirm = TRUE)
  
  athena_tbl <- tbl(con, sql("SELECT * FROM INFORMATION_SCHEMA.TABLES"))
  athena_tbl %>% compute("compute_tbl1", s3_location = paste0(Sys.getenv("noctua_s3_tbl"),"compute_tbl/"))
  athena_tbl %>% compute("compute_tbl2")
  result1 <- dbExistsTable(con, "compute_tbl1")
  result2 <- dbExistsTable(con, "compute_tbl2")
  
  # clean up athena
  dbRemoveTable(con, "compute_tbl1", confirm = TRUE)
  dbRemoveTable(con, "compute_tbl2", confirm = TRUE)
  
  expect_equal(result1, TRUE)
  expect_equal(result2, TRUE)
})
