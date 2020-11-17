context("upload file setup")

library(data.table)

test_that("test file parser parameter setup delimited",{
  skip_if_package_not_avialable("vroom")
  init_args = list()
  
  # data.table parser
  noctua_options()
  arg_4 <- noctua:::update_args(file.type = "csv", init_args)
  arg_5 <- noctua:::update_args(file.type = "tsv", init_args)
  
  # vroom parser
  noctua_options(file_parser = "vroom")
  arg_6 <- noctua:::update_args(file.type = "csv", init_args)
  arg_7 <- noctua:::update_args(file.type = "tsv", init_args)

  expect_equal(arg_4, list(fun = data.table::fwrite, quote= FALSE, showProgress = FALSE, sep = ","))
  expect_equal(arg_5, list(fun = data.table::fwrite, quote= FALSE, showProgress = FALSE, sep = "\t"))
  expect_equal(arg_6, list(fun = vroom::vroom_write, quote= "none", progress = FALSE, escape = "none", delim = ","))
  expect_equal(arg_7, list(fun = vroom::vroom_write, quote= "none", progress = FALSE, escape = "none", delim = "\t"))
})

test_that("test file parser parameter setup parquet",{
  skip_if_package_not_avialable("arrow")
  
  init_args = list()
  
  arg_1 <- noctua:::update_args(file.type = "parquet", init_args)
  arg_2 <- noctua:::update_args(file.type = "parquet", init_args, compress = T)
  expect_equal(arg_1, list(fun = arrow::write_parquet, use_deprecated_int96_timestamps = TRUE, compression = NULL))
  expect_equal(arg_2, list(fun = arrow::write_parquet, use_deprecated_int96_timestamps = TRUE, compression = "snappy"))
})

test_that("test file parser parameter setup json",{
  skip_if_package_not_avialable("jsonlite")
  init_args = list()
  
  arg_3 <- noctua:::update_args(file.type = "json", init_args)
  expect_equal(arg_3, list(fun = jsonlite::stream_out, verbose = FALSE))
})

default_split <- c(1, 1000001)
custom_split <- seq(1, 2e6, 100000)
custom_chunk <- 100000

test_that("test data frame is split correctly",{
  # Test connection is using AWS CLI to set profile_name 
  value = data.table(x = 1:2e6)
  max_row = nrow(value)

  vec_1 <- noctua:::dt_split(value, Inf, "csv", T)
  vec_2 <- noctua:::dt_split(value, Inf, "tsv", F)
  vec_3 <- noctua:::dt_split(value, custom_chunk, "tsv", T)
  vec_4 <- noctua:::dt_split(value, custom_chunk, "csv", F)
  vec_5 <- noctua:::dt_split(value, Inf, "parquet", T)
  vec_6 <- noctua:::dt_split(value, Inf, "parquet", F)
  vec_7 <- noctua:::dt_split(value, custom_chunk, "parquet", T)
  vec_8 <- noctua:::dt_split(value, custom_chunk, "parquet", F)
  vec_9 <- noctua:::dt_split(value, Inf, "json", T)
  vec_10 <- noctua:::dt_split(value, Inf, "json", F)
  vec_11 <- noctua:::dt_split(value, custom_chunk, "json", T)
  vec_12 <- noctua:::dt_split(value, custom_chunk, "json", F)
  
  expect_equal(vec_1, list(SplitVec = default_split, MaxBatch = 1e+06, MaxRow = max_row))
  expect_equal(vec_2, list(SplitVec = 1, MaxBatch = max_row, MaxRow = max_row))
  expect_equal(vec_3, list(SplitVec = custom_split, MaxBatch = custom_chunk, MaxRow = max_row))
  expect_equal(vec_4, list(SplitVec = custom_split, MaxBatch = custom_chunk, MaxRow = max_row))
  expect_equal(vec_5, list(SplitVec = 1, MaxBatch = max_row, MaxRow = max_row))
  expect_equal(vec_6, list(SplitVec = 1, MaxBatch = max_row, MaxRow = max_row))
  expect_equal(vec_7, list(SplitVec = custom_split, MaxBatch = custom_chunk, MaxRow = max_row))
  expect_equal(vec_8, list(SplitVec = custom_split, MaxBatch = custom_chunk, MaxRow = max_row))
  expect_equal(vec_9, list(SplitVec = 1, MaxBatch = max_row, MaxRow = max_row))
  expect_equal(vec_10, list(SplitVec = 1, MaxBatch = max_row, MaxRow = max_row))
  expect_equal(vec_11, list(SplitVec = custom_split, MaxBatch = custom_chunk, MaxRow = max_row))
  expect_equal(vec_12, list(SplitVec = custom_split, MaxBatch = custom_chunk, MaxRow = max_row))
})
