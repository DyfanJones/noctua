context("keyboard interrupt")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

test_that("Check if Athena query has been successfully been cancelled",{
  skip_if_no_env()
  
  con <- dbConnect(athena(), keyboard_interrupt = T)
  
  res <- dbSendQuery(con, "SHOW TABLES IN default")
  query_id <- res@info[["QueryExecutionId"]]
  err_msg <- sprintf(
    "Query '%s' has been cancelled by user.",
    query_id)
  
  expect_error(noctua:::interrupt_athena(res), err_msg)
  
  status <- res@connection@ptr$Athena$get_query_execution(
    QueryExecutionId = query_id)$QueryExecution$Status$State
  
  expect_equal(status, "CANCELLED")
})

test_that("Check if Athena query has not been cancelled",{
  skip_if_no_env()
  
  con <- dbConnect(athena(), keyboard_interrupt = F)
  
  res <- dbSendQuery(con, "SHOW TABLES IN default")
  query_id <- res@info[["QueryExecutionId"]]
  err_msg <- sprintf(
    "Query '%s' has been cancelled by user but will carry on running in AWS Athena",
    query_id)
  
  expect_error(noctua:::interrupt_athena(res), err_msg)
  
  # give AWS Athena a chance to start query
  Sys.sleep(5)
  
  status <- res@connection@ptr$Athena$get_query_execution(
    QueryExecutionId = query_id)$QueryExecution$Status$State

  expect_true(status %in% c("RUNNING", "SUCCEEDED"))
  
  # tidy up query
  dbClearResult(res)
})
