context("dplyr sql_translate_env")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

library(dbplyr)
test_that("Check RAthena s3 dplyr sql_translate_env method",{
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena())
  
  t1 <- translate_sql(as.character(1), con = con)
  t2 <- translate_sql(as.numeric("1"), con = con)
  t3 <- translate_sql(as.double("1.2"), con = con)
  t4 <- translate_sql(as.integer(1.2), con = con)
  t5 <- translate_sql(as.integer64(1.2), con = con)
  t6 <- translate_sql(as.Date("2019-01-01"), con = con)
  t7 <- translate_sql(as.logical("true"), con = con)
  t8 <- translate_sql(as.raw(10), con = con)
  t9 <- translate_sql(tolower("HELLO"), con = con)
  t10 <- translate_sql(toupper("hello"), con = con)
  t11 <- translate_sql(pmax(1,2), con = con)
  t12 <- translate_sql(pmin(1,2), con = con)
  t13 <- translate_sql(is.finite("var1"), con = con)
  t14 <- translate_sql(is.infinite("var1"), con = con)
  t15 <- translate_sql(is.nan("var1"), con = con)
  
  expect_equal(t1 ,sql("CAST(1.0 AS VARCHAR)"))
  expect_equal(t2 ,sql("CAST('1' AS DOUBLE)"))
  expect_equal(t3 ,sql("CAST('1.2' AS DOUBLE)"))
  expect_equal(t4 ,sql("CAST(1.2 AS INTEGER)"))
  expect_equal(t5 ,sql("CAST(1.2 AS BIGINT)"))
  expect_equal(t6 ,sql("CAST('2019-01-01' AS DATE)"))
  expect_equal(t7 ,sql("CAST('true' AS BOOLEAN)"))
  expect_equal(t8 ,sql("CAST(10.0 AS VARBINARY)"))
  expect_equal(t9 ,sql("LOWER('HELLO')"))
  expect_equal(t10 ,sql("UPPER('hello')"))
  expect_equal(t11 ,sql("GREATEST(1.0, 2.0)"))
  expect_equal(t12 ,sql("LEAST(1.0, 2.0)"))
  expect_equal(t13 ,sql("IS_FINITE('var1')"))
  expect_equal(t14 ,sql("IS_FINITE('var1')"))
  expect_equal(t15 ,sql("IS_NAN('var1')"))
})