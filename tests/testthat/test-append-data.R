context("Append to Existing")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

test_that("Testing if data is appended correctly", {
  skip_if_no_env()
  
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena(),
                   s3_staging_dir = Sys.getenv("noctua_s3_query"))
  
  DATE <- Sys.Date()
  dbWriteTable(con, "mtcars", mtcars, overwrite = T, compress = T,
               partition = c("timesTamp" = format(DATE, "%Y%m%d")))
  
  # don't specify to send data compressed
  expect_warning(dbWriteTable(con, "mtcars", mtcars, append = T, file.type = "parquet",
                              partition = c("timesTamp" = format(DATE+1, "%Y%m%d"))))

  dt <- dbGetQuery(con, "select timestamp, cast(count(*) as integer) as n from mtcars group by 1 order by 1")
  
  exp_dt <- data.table(timestamp = c(format(DATE, "%Y%m%d"), format(DATE+1, "%Y%m%d")),
                       n = as.integer(c(32,32)))
  
  expect_equal(dt, exp_dt)
})