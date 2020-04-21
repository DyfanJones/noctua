context("Testing retry function")

test_that("Check if retry_api is working as intended",{
  skip_if_no_env()
  # create a function that is designed to fail so many times
  fail_env <- new.env()
  fail_env$i <- 1
  
  fail_function <- function(i, j){
    if (i > j) i <- 1
    result <- (i == j)
    fail_env$i <- i + 1
    if(!result) stop(i, " does not equal 2")
    return(TRUE)
  }
  
  # this function will fail twice before succeeding
  expect_true(retry_api_call(fail_function(fail_env$i , 3)))
  
  # stop noctua retrying and expect error
  noctua_options(retry = 0)
  expect_error(retry_api_call(fail_function(fail_env$i, 3)))
  
  expect_error(noctua_options(retry = - 10))
  expect_error(noctua_options(retry_quiet = "blah"))
})
