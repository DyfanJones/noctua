context("dplyr compute")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

test_that("Check noctua s3 dplyr compute method",{
  skip_if_no_env()
  skip_if_package_not_avialable("dplyr")
  
  library(dplyr)
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena())
  
  # remove test tables if exist
  if(dbExistsTable(con, "compute_tbl1"))
    dbRemoveTable(con, "compute_tbl1", confirm = TRUE)
  if(dbExistsTable(con, "compute_tbl2"))
    dbRemoveTable(con, "compute_tbl2", confirm = TRUE)
  
  athena_tbl <- tbl(con, sql("SELECT * FROM INFORMATION_SCHEMA.TABLES"))
  s3_uri = file.path(Sys.getenv("noctua_s3_tbl"),"compute_tbl/", fsep = "/")
  
  athena_tbl %>% compute("compute_tbl1", s3_location = s3_uri, temporary = F)
  athena_tbl %>% compute("compute_tbl2", temporary = F)
  
  noctua_options(unload = T)
  expect_error(athena_tbl %>% compute("compute_tbl2"))
  noctua_options()
  
  result1 <- dbExistsTable(con, "compute_tbl1")
  result2 <- dbExistsTable(con, "compute_tbl2")
  
  # clean up athena
  dbRemoveTable(con, "compute_tbl1", confirm = TRUE)
  dbRemoveTable(con, "compute_tbl2", confirm = TRUE)
  
  expect_equal(result1, TRUE)
  expect_equal(result2, TRUE)
})


# Error (test-dplyr-compute.R:23:3): Check noctua s3 dplyr compute method
# <rlib_error_dots_unused/rlib_error_dots/rlang_error/error/condition>
#   Error in `db_compute(x$src$con, name, sql, temporary = temporary, unique_indexes = unique_indexes, 
#                        indexes = indexes, analyze = analyze, ...)`: Arguments in `...` must be used.
# ✖ Problematic argument:
#   • s3_location = paste0(Sys.getenv("noctua_s3_tbl"), "compute_tbl/")
# ℹ Did you misspell an argument name?
#   Backtrace:
#   ▆
# 1. ├─athena_tbl %>% ... at test-dplyr-compute.R:23:2
# 2. ├─dplyr::compute(...)
# 3. └─dbplyr:::compute.tbl_sql(...)
# 4.   └─dbplyr::db_compute(...)
# 5.     └─rlang (local) `<fn>`()
# 6.       └─rlang:::check_dots(env, error, action, call)
# 7.         └─rlang:::action_dots(...)
# 8.           ├─base (local) try_dots(...)
# 9.           └─rlang (local) action(...)

