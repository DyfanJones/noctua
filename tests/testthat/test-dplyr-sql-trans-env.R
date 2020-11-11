context("dplyr sql_translate_env")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

library(dbplyr)

dt = data.table::data.table(Logicial = TRUE,
                            Integer = as.integer(1),
                            Integer64 = bit64::as.integer64(1),
                            Numeric = as.numeric(1),
                            Double = as.double(1),
                            Factor = factor(1),
                            Character = as.character(1),
                            List = list(1),
                            Date = Sys.Date(),
                            Posixct = as.POSIXct(Sys.Date()))
data_type1 = c("BOOLEAN", "INT", "BIGINT", "DOUBLE", "DOUBLE", 
              "STRING", "STRING", "STRING", "DATE", "TIMESTAMP")
names(data_type1) = c("Logicial", "Integer", "Integer64", "Numeric", "Double", "Factor", "Character", "List", "Date", "Posixct")
method <- "file_method"
type_names <- sapply(1:12, function(x) paste0("var_", x))
data_types <- list(list(Name = type_names,
                        Type = c("boolean", "int",
                                 "integer", "tinyint",
                                 "smallint", "bigint",
                                 "float", "decimal",
                                 "string", "varchar",
                                 "date", "timestamp")))
data_type2 = c("logical", "integer", "integer", "integer", "integer", "integer64", "double", "double", 
               "character", "character","Date","POSIXct")
data_type3 = c("l", "i", "i", "i", "i", "I", "d", "d", "c", "c", "D", "T")
names(data_type2) = type_names
names(data_type3) = type_names

test_that("Check RAthena s3 dplyr sql_translate_env method",{
  skip_if_no_env()
  skip_if_package_not_avialable("vroom")
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena())
  
  test_date <- as.Date("2020-01-01")
  
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
  t16 <- translate_sql(paste("hi","bye"), con = con)
  t17 <- translate_sql(paste("hi","bye", sep = "-"), con = con)
  t18 <- translate_sql(paste0("hi","bye"), con = con)
  t19 <- escape(test_date, con = con)
  t20 <- escape("2020-01-01", con = con)
  t21 <- escape("2020-13-01", con = con)
  t22 <- translate_sql(as.character("2020-01-01"), con = con)
  t23 <- translate_sql(c("2020-01-01", "2020-01-02"), con = con)
  t24 <- translate_sql(c("2020-01-01", "2020-13-02"), con = con)
  t25 <- translate_sql(as(1,"character"), con = con)
  t26 <- translate_sql(iris[["sepal_length"]], con = con)
  t27 <- translate_sql(iris[[1]], con = con)
  t28 <- grepl("^Athena.*\\[.*/.*\\]", dplyr::db_desc(con))
  t29 <- dbDataType(con, dt)
  t30 <- noctua:::AthenaToRDataType.athena_data.table(method, data_types)
  t31 <- noctua:::AthenaToRDataType.athena_vroom(method, data_types)
  t32 <- noctua:::sql_escape_date.AthenaConnection(con, "2020-01-01")
  t33 <- noctua:::sql_escape_datetime.AthenaConnection(con, "2020-01-01")
  
  expect_equal(t1 ,sql("CAST(1.0 AS VARCHAR)"))
  expect_equal(t2 ,sql("CAST('1' AS DOUBLE)"))
  expect_equal(t3 ,sql("CAST('1.2' AS DOUBLE)"))
  expect_equal(t4 ,sql("CAST(1.2 AS INTEGER)"))
  expect_equal(t5 ,sql("CAST(1.2 AS BIGINT)"))
  expect_equal(t6 ,sql("CAST(date '2019-01-01' AS DATE)"))
  expect_equal(t7 ,sql("CAST('true' AS BOOLEAN)"))
  expect_equal(t8 ,sql("CAST(10.0 AS VARBINARY)"))
  expect_equal(t9 ,sql("LOWER('HELLO')"))
  expect_equal(t10 ,sql("UPPER('hello')"))
  expect_equal(t11 ,sql("GREATEST(1.0, 2.0)"))
  expect_equal(t12 ,sql("LEAST(1.0, 2.0)"))
  expect_equal(t13 ,sql("IS_FINITE('var1')"))
  expect_equal(t14 ,sql("IS_FINITE('var1')"))
  expect_equal(t15 ,sql("IS_NAN('var1')"))
  expect_equal(t16 ,sql("('hi'||' '||'bye')"))
  expect_equal(t17 ,sql("('hi'||'-'||'bye')"))
  expect_equal(t18 ,sql("CONCAT('hi', 'bye')"))
  expect_equal(t19, sql("date '2020-01-01'"))
  expect_equal(t20, sql("date '2020-01-01'"))
  expect_equal(t21, sql("'2020-13-01'"))
  expect_equal(t22, sql("CAST(date '2020-01-01' AS VARCHAR)"))
  expect_equal(t23, sql("(date '2020-01-01', date '2020-01-02')"))
  expect_equal(t24, sql("('2020-01-01', '2020-13-02')"))
  expect_equal(t25, sql("CAST(1.0 AS STRING)"))
  expect_equal(t26, sql("\"iris\"['sepal_length']"))
  expect_equal(t27, sql('"iris"[1]'))
  expect_true(t28)
  expect_error(explain(tbl(con, "iris")))
  expect_equal(t29, data_type1)
  expect_equal(t30, data_type2)
  expect_equal(t31, data_type3)
  expect_equal(t32, "date '2020-01-01'")
  expect_equal(t33, sprintf("timestamp '%s'", strftime("2020-01-01", "%Y-%m-%d %H:%M:%OS %Z")))
})
