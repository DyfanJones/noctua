context("upload file setup")

test_that("test file parser parameter setup",{
  init_args = list()
  
  arg_1 <- noctua:::update_args(file.type = "parquet", init_args)
  arg_2 <- noctua:::update_args(file.type = "parquet", init_args, compress = T)
  arg_3 <- noctua:::update_args(file.type = "json", init_args)
  
  # data.table parser
  noctua_options()
  arg_4 <- noctua:::update_args(file.type = "csv", init_args)
  arg_5 <- noctua:::update_args(file.type = "tsv", init_args)
  
  # vroom parser
  noctua_options(file_parser = "vroom")
  arg_6 <- noctua:::update_args(file.type = "csv", init_args)
  arg_7 <- noctua:::update_args(file.type = "tsv", init_args)
  
  expect_equal(arg_1, list(fun = arrow::write_parquet, compression = NULL))
  expect_equal(arg_2, list(fun = arrow::write_parquet, compression = "snappy"))
  expect_equal(arg_3, list(fun = jsonlite::stream_out, verbose = FALSE))
  expect_equal(arg_4, list(fun = data.table::fwrite, quote= FALSE, showProgress = FALSE, sep = ","))
  expect_equal(arg_5, list(fun = data.table::fwrite, quote= FALSE, showProgress = FALSE, sep = "\t"))
  expect_equal(arg_6, list(fun = vroom::vroom_write, quote= "none", progress = FALSE, escape = "none", delim = ","))
  expect_equal(arg_7, list(fun = vroom::vroom_write, quote= "none", progress = FALSE, escape = "none", delim = "\t"))
})

test_that("test data frame splitter vec",{
  # Test connection is using AWS CLI to set profile_name 
  vec_1 <- split_vec(iris, 10)
  
  dt = data.table(x = 1:2e6)
  vec_2 <- split_vec(dt, Inf)
  
  expect_equal(vec_1, c(1, 11, 21, 31, 41, 51, 61, 71, 81, 91, 101, 111, 121, 131, 141))
  expect_equal(vec_2, c(1, 1000001))
})
