context("dplyr copy_to")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

library(dplyr)
test_that("Check noctua s3 dplyr copy_to method",{
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena())
  
  # creates Athena table and returns tbl_sql
  athena_mtcars <- copy_to(con, mtcars, s3_location = Sys.getenv("noctua_s3_tbl"), compress = T, overwrite = T)
  mtcars_filter <- athena_mtcars %>% filter(gear >=4)
  tbl_result <- is.tbl(mtcars_filter)
  # create another Athena table
  copy_to(con, mtcars_filter)
  
  result1 <- dbExistsTable(con, "mtcars")
  result2 <- dbExistsTable(con, "mtcars_filter")
  
  # clean up athena
  dbRemoveTable(con, "mtcars", confirm = TRUE)
  dbRemoveTable(con, "mtcars_filter", confirm = TRUE)
  
  expect_true(tbl_result)
  expect_true(result1)
  expect_true(result2)
})
