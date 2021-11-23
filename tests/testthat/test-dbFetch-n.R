# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

con <- dbConnect(athena())

test_that("fetch athena table in batch 100 data.table", {
  skip_if_no_env()
  
  noctua_options()
  res = dbExecute(con, "select * from iris")
  
  fetch_iris = dbFetch(res, n = 100)
  
  expect_equal(dim(fetch_iris), c(100, 5))
  
  fetch_iris = dbFetch(res, n = 100)
  
  expect_equal(dim(fetch_iris), c(50, 5))
  expect_true(inherits(fetch_iris, "data.table"))
  
  dbClearResult(res)
})

test_that("fetch athena table in batch 100 tibble", {
  skip_if_no_env()
  
  noctua_options("vroom")
  
  res = dbExecute(con, "select * from iris")
  
  fetch_iris = dbFetch(res, n = 100)
  
  expect_equal(dim(fetch_iris), c(100, 5))
  
  fetch_iris = dbFetch(res, n = 100)
  
  expect_equal(dim(fetch_iris), c(50, 5))
  
  expect_true(inherits(fetch_iris, "tbl_df"))
  
  dbClearResult(res)
})

test_that("fetch athena table on closed connection", {
  skip_if_no_env()
  
  res = dbExecute(con, "select * from iris")
  
  fetch_iris = dbFetch(res, n = 100)
  
  expect_equal(dim(fetch_iris), c(100, 5))
  dbClearResult(res)
  
  expect_error(dbFetch(res, n = 100), "Result already cleared.")
})
