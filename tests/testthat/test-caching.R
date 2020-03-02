context("query id caching")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

test_that("Testing if caching returns the same query id", {
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena())
  
  res1 = dbSendStatement(con, "SELECT table_name FROM information_schema.tables limit 1")
  dbFetch(res1)
  res2 = dbExecute(con, "SELECT table_name FROM information_schema.tables limit 1")
  
  noctua_options(cache_size = 10)
  
  res3 = dbSendStatement(con, "SELECT table_name FROM information_schema.tables limit 1")
  dbFetch(res3)
  res4 = dbExecute(con, "SELECT table_name FROM information_schema.tables limit 1")
  
  # clear cached backend data
  noctua_options(clear_cache = T)
  
  # expect query ids not to be the same
  exp1 = res1@info$QueryExecutionId == res2@info$QueryExecutionId
  exp2 = res3@info$QueryExecutionId == res4@info$QueryExecutionId
  expect_false(exp1)
  expect_true(exp2)
  expect_error(noctua_options(cache_size = 101))
  expect_error(noctua_options(cache_size = -1))
  expect_true(nrow(noctua:::athena_option_env$cache_dt) == 0)
})
