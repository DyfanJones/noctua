# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

test_that("fetch athena table in batch 100 data.table", {
  skip_if_no_env()
  
  con <- dbConnect(athena())
  
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
  
  con <- dbConnect(athena())
  
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
  
  con <- dbConnect(athena())
  
  res = dbExecute(con, "select * from iris")
  
  fetch_iris = dbFetch(res, n = 100)
  
  expect_equal(dim(fetch_iris), c(100, 5))
  dbClearResult(res)
  
  expect_error(dbFetch(res, n = 100), "Result already cleared.")
})

test_that("test dbGetQuery dbplyr ident", {
  skip_if_no_env()
  skip_if_package_not_avialable("dbplyr")
  library(dbplyr)
  
  con <- dbConnect(athena())
  
  noctua::noctua_options("data.table")
  
  empty_shell = dbGetQuery(con, dbplyr::ident("iris"))
  
  expect = c("sepal_length", "sepal_width", "petal_length", "petal_width", "species")
  
  expect_equal(names(empty_shell), expect)
})

test_that("test if dbGetQuery statistics returns named list correctly", {
  skip_if_no_env()
  
  con <- dbConnect(athena())
  
  stat_out = utils::capture.output({exp = dbGetQuery(con, "select * from iris", statistics = T)})
  
  for (i in expected_stat_output){
    expect_true(any(grepl(i, stat_out)))
  }
})

test_that("test athena unload",{
  
  noctua_options(unload = T)
  
  expect_true(athena_unload())
  noctua_options()
})


