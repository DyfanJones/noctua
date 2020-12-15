context("bigint")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

s3.location1 <- paste0(Sys.getenv("noctua_s3_tbl"),"test_df/")
s3.location2 <- Sys.getenv("noctua_s3_tbl")

test_that("Testing data transfer between R and athena datatable", {
  skip_if_no_env()
  
  # default big integer as integer64
  con <- dbConnect(athena(),
                   s3_staging_dir = Sys.getenv("noctua_s3_query"))
  
  expect_equal(noctua:::athena_option_env$bigint, "integer64")
  
  noctua_options("vroom")
  
  expect_equal(noctua:::athena_option_env$bigint, "I")
  
  # big integer as integer
  noctua_options()
  con <- dbConnect(athena(),
                   s3_staging_dir = Sys.getenv("noctua_s3_query"),
                   bigint = "integer")
  
  expect_equal(noctua:::athena_option_env$bigint, "integer")
  
  noctua_options("vroom")
  
  expect_equal(noctua:::athena_option_env$bigint, "i")
  
  # big integer as numeric
  noctua_options()
  con <- dbConnect(athena(),
                   s3_staging_dir = Sys.getenv("noctua_s3_query"),
                   bigint = "numeric")
  
  expect_equal(noctua:::athena_option_env$bigint, "double")
  
  noctua_options("vroom")
  
  expect_equal(noctua:::athena_option_env$bigint, "d")
  
  # big integer as character
  noctua_options()
  con <- dbConnect(athena(),
                   s3_staging_dir = Sys.getenv("noctua_s3_query"),
                   bigint = "character")
  
  expect_equal(noctua:::athena_option_env$bigint, "character")
  
  noctua_options("vroom")
  
  expect_equal(noctua:::athena_option_env$bigint, "c")
})