context("dplyr sql_translate_env")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

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
names(data_type1) = c("Logicial", "Integer", "Integer64", "Numeric",
                      "Double", "Factor", "Character", "List", "Date", "Posixct")
method <- "file_method"
data_types <- c("boolean", "int",
                "integer", "tinyint",
                "smallint", "bigint",
                "float", "real",
                "decimal","string",
                "varchar", "char",
                "date", "timestamp",
                "array", "row",
                "map", "json",
                "ipaddress", "varbinary",
                "timestamp with time zone")
type_names <- paste0("var_", seq_along(data_types))
names(data_types) <- type_names
data_type2 = c("logical", "integer",
               "integer", "integer",
               "integer", "integer64",
               "double", "double", 
               "double", "character",
               "character", "character",
               "Date", "POSIXct",
               "character", "character",
               "character", "character",
               "character", "character",
               "POSIXct")
data_type3 = c("l", "i", "i", "i", "i", "I", "d",
               "d", "d", "c", "c", "c", "D", "T",
               "c", "c", "c", "c", "c", "c", "c")
names(data_type2) = type_names
names(data_type3) = type_names

test_that("Check RAthena s3 dplyr sql_translate_env method",{
  skip_if_no_env()
  skip_if_package_not_avialable("vroom")
  skip_if_package_not_avialable("dbplyr")
  
  library(dbplyr)
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
  noctua_options()
  t30 <- noctua:::AthenaToRDataType.athena_data.table(method, data_types)
  noctua_options("vroom")
  t31 <- noctua:::AthenaToRDataType.athena_vroom(method, data_types)
  t32a <- dbplyr::sql_escape_date(con, "2020-01-01")
  t32b <- dbplyr::sql_escape_date(con, "2020/01/01")
  t33a <- dbplyr::sql_escape_datetime(con, "2020-01-01")
  t33b <- dbplyr::sql_escape_datetime(con, "2020/01/01")
  t33c <- dbplyr::sql_escape_datetime(con, "2020-01-01 12:10")
  t33d <- dbplyr::sql_escape_datetime(con, "2020-01-01 12:10:10")
  t33e <- dbplyr::sql_escape_datetime(con, "2020/01/01 12:10")
  t33f <- dbplyr::sql_escape_datetime(con, "2020/01/01 12:10:10")
  t34 <- translate_sql(grepl("dummy", data_types), con = con)
  t35 <- translate_sql(grepl("dummy", data_types, ignore.case=TRUE), con = con)
  t36 <- translate_sql(regexpr("dummy", data_types), con = con)
  # stringr functions
  t37 <- translate_sql(str_c("dummy1", "dummy2"), con = con)
  t38 <- translate_sql(str_locate("dummy1", "u"), con = con)
  t39 <- translate_sql(str_detect("dummy", "u"), con = con)
  t40 <- translate_sql(str_detect("dummy", "u", negate = TRUE), con = con)
  t41 <- translate_sql(str_replace("dummy1", "\\d+", "One"), con = con)
  t42 <- translate_sql(str_replace_all("dummy1", "\\d+", "One"), con = con)
  t43 <- translate_sql(str_squish("d u m m y 1"), con = con)
  t44 <- translate_sql(str_remove("dummy1", "\\d+"), con = con)
  t45 <- translate_sql(str_remove_all("dummy1", "\\d+"), con = con)
  t46 <- translate_sql(str_split("dummy1", "\\d+"), con = con)
  expect_error(translate_sql(str_split("dummy1", "\\d+", simplify=TRUE), con = con))
  t47 <- translate_sql(str_extract("dummy1", "\\d+"), con = con)
  t48 <- translate_sql(str_extract_all("dummy1", "\\d+"), con = con)
  expect_error(translate_sql(str_extract_all("dummy1", "\\d+", simplify = TRUE), con = con))
  # lubridate functions
  t49 <- translate_sql(month('2020-01-01'), con = con)
  t50 <- translate_sql(month('2020-01-01', label = TRUE), con = con)
  t51 <- translate_sql(month('2020-01-01', label = TRUE, abbr = FALSE), con = con)
  t52 <- translate_sql(quarter('2020-01-01'), con = con)
  t53 <- translate_sql(quarter('2020-01-01', with_year = TRUE), con = con)
  expect_error(translate_sql(quarter('2020-01-01', with_year = TRUE, fiscal_start = 2), con = con))
  t54 <- translate_sql(wday('2020-01-01'), con = con)
  t55 <- translate_sql(wday('2020-01-01', week_start = 3), con = con)
  t56 <- translate_sql(wday('2020-01-01', label = TRUE), con = con)
  t57 <- translate_sql(wday('2020-01-01', label = TRUE, abbr = FALSE), con = con)
  t58 <- translate_sql(yday('2020-01-01'), con = con)
  t59 <- translate_sql(seconds(1), con = con)
  t60 <- translate_sql(minutes(1), con = con)
  t61 <- translate_sql(hours(1), con = con)
  t62 <- translate_sql(days(1), con = con)
  t63 <- translate_sql(weeks(1), con = con)
  t64 <- translate_sql(months(1), con = con)
  t65 <- translate_sql(years(1), con = con)
  t66 <- translate_sql(floor_date(as_datetime('2021-07-26 14:45:50')), con = con)
  t67 <- translate_sql(floor_date(as_datetime('2021-07-26 14:45:50'), "month"), con = con)
  t68 <- translate_sql(as_datetime('2021-07-26 14:45:50', 'America/Los_Angeles'), con = con)
  t69 <- translate_sql(today(), con = con)
  t70 <- translate_sql(as_date('2020-01-01'), con = con)
  t71 <- translate_sql(now(), con = con)
  t72 <- translate_sql(now('America/Los_Angeles'), con = con)
  # base dates
  t73 <- translate_sql(Sys.Date(), con = con)
  t74 <- translate_sql(Sys.time(), con = con)
  t75 <- translate_sql(Sys.time('America/Los_Angeles'), con = con)
  t76 <- translate_sql(as.POSIXct("2020-01-01"), con=con)
  t77 <- translate_sql(as.POSIXct("2020-01-01", "UTC"), con=con)
  
  expect_warning(translate_sql(median("column"), con = con))
  t78 <- translate_sql(median("column", na.rm = TRUE), con = con)
  t79 <- translate_sql(quantile("column",0.25), con = con)
  expect_error(translate_sql(quantile("column", c(0.25, 0.5)), con = con))
  expect_error(translate_sql(quantile("column","0.25"), con = con))
  
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
  expect_equal(t16 ,sql("'hi'||' '||'bye'"))
  expect_equal(t17 ,sql("'hi'||'-'||'bye'"))
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
  # suppress information messages
  suppressMessages(expect_error(explain(tbl(con, "iris"))))
  expect_equal(t29, data_type1)
  expect_equal(t30, data_type2)
  expect_equal(t31, data_type3)
  if(identical(dbplyr_env$major, 1L)){
    expect_equal(t32a, "date '2020-01-01'")
    expect_equal(t33a, sprintf("timestamp '%s'", strftime("2020-01-01", "%Y-%m-%d %H:%M:%OS %Z")))
  } else {
    expect_equal(t32a, "date '2020-01-01'")
    expect_equal(t32b, "date '2020-01-01'")
    expect_equal(t32a, "date '2020-01-01'")
    expect_equal(t32b, "date '2020-01-01'")
    expect_equal(t33a, "timestamp '2020-01-01'")
    expect_equal(t33b, "timestamp '2020-01-01'")
    expect_equal(t33c, "timestamp '2020-01-01 12:10:00.000'")
    expect_equal(t33d, "timestamp '2020-01-01 12:10:10.000'")
    expect_equal(t33e, "timestamp '2020-01-01 12:10:00.000'")
    expect_equal(t33f, "timestamp '2020-01-01 12:10:10.000'")
  }
  expect_equal(t34, sql("REGEXP_LIKE(\"data_types\",'dummy')"))
  expect_equal(t35, sql("REGEXP_LIKE(\"data_types\",'(?i)dummy')"))
  expect_error(translate_sql(grepl("dummy", data_types, perl=TRUE), con = con))
  expect_equal(t36, sql("REGEXP_LIKE(\"data_types\",'dummy')"))
  # stringr functions
  expect_equal(t37, sql("'dummy1'||''||'dummy2'"))
  expect_equal(t38, sql("strpos('dummy1','u')"))
  expect_equal(t39, sql("REGEXP_LIKE('dummy','u')"))
  expect_equal(t40, sql("NOT REGEXP_LIKE('dummy','u')"))
  expect_equal(t41, sql("regexp_replace('dummy1','\\d+','One')"))
  expect_equal(t42, sql("regexp_replace('dummy1','\\d+','One')"))
  expect_equal(t43, sql("trim(regexp_replace('d u m m y 1', '\\s+', ' '))"))
  expect_equal(t44, sql("regexp_replace('dummy1','\\d+')"))
  expect_equal(t45, sql("regexp_replace('dummy1','\\d+')"))
  expect_equal(t46, sql("regexp_split('dummy1','\\d+')"))
  expect_equal(t47, sql("regexp_extract('dummy1','\\d+')"))
  expect_equal(t48, sql("regexp_extract_all('dummy1','\\d+')"))
  # lubridate functions
  expect_equal(t49, sql("date_format(date '2020-01-01', '%m')"))
  expect_equal(t50, sql("date_format(date '2020-01-01', '%b')"))
  expect_equal(t51, sql("date_format(date '2020-01-01', '%M')"))
  expect_equal(t52, sql("quarter(date '2020-01-01')"))
  expect_equal(t53, sql("year(date '2020-01-01') || '.' || quarter(date '2020-01-01')"))
  expect_equal(t54, sql("(dow(date '2020-01-01') + 0) %7 + 1"))
  expect_equal(t55, sql("(dow(date '2020-01-01') + 4) %7 + 1"))
  expect_equal(t56, sql("format_datetime(date '2020-01-01', 'E')"))
  expect_equal(t57, sql("format_datetime(date '2020-01-01', 'EEEE')"))
  expect_equal(t58, sql("doy(date '2020-01-01')"))
  expect_equal(t59, sql("INTERVAL '1' second"))
  expect_equal(t60, sql("INTERVAL '1' minute"))
  expect_equal(t61, sql("INTERVAL '1' hour"))
  expect_equal(t62, sql("INTERVAL '1' day"))
  expect_equal(t63, sql("INTERVAL '1' week"))
  expect_equal(t64, sql("INTERVAL '1' month"))
  expect_equal(t65, sql("INTERVAL '1' year"))
  if(identical(dbplyr_env$major, 1L)){
    posixct = strftime(as.POSIXct('2021-07-26 14:45:50'), "%Y-%m-%d %H:%M:%OS %Z")
    expect_equal(t66, sql(sprintf("date_trunc('second', timestamp '%s')", posixct)))
    expect_equal(t67, sql(sprintf("date_trunc('month', timestamp '%s')", posixct)))
    expect_equal(t68, sql(sprintf("timestamp '%s' at time zone 'America/Los_Angeles'", posixct)))
  } else {
    expect_equal(t66, sql("date_trunc('second', timestamp '2021-07-26 14:45:50.000')"))
    expect_equal(t67, sql("date_trunc('month', timestamp '2021-07-26 14:45:50.000')"))
    expect_equal(t68, sql("timestamp '2021-07-26 14:45:50.000' at time zone 'America/Los_Angeles'"))
  }
  expect_equal(t69, sql("current_date"))
  expect_equal(t70, sql("CAST(date '2020-01-01' AS DATE)"))
  expect_equal(t71, sql("now()"))
  expect_equal(t72, sql("now() at time zone 'America/Los_Angeles'"))
  # base dates
  expect_equal(t73, sql("current_date"))
  expect_equal(t74, sql("now()"))
  expect_equal(t75, sql("now() at time zone 'America/Los_Angeles'"))
  
  if(identical(dbplyr_env$major, 1L)){
    posixct = strftime(as.POSIXct("2020-01-01"), "%Y-%m-%d %H:%M:%OS %Z")
    expect_equal(t76, sql(sprintf("timestamp '%s'", posixct)))
    expect_equal(t77, sql(sprintf("timestamp '%s' at time zone 'UTC'", posixct)))
  } else {
    expect_equal(t76, sql("timestamp '2020-01-01 00:00:00.000'"))
    expect_equal(t77, sql("timestamp '2020-01-01 00:00:00.000' at time zone 'UTC'"))
  }
  expect_equal(t78, sql("APPROX_PERCENTILE('column', 0.5)"))
  expect_equal(t79, sql("APPROX_PERCENTILE('column', 0.25)"))
})

test_that("Raise error for unknown data types", {
  obj <- "dummy"
  class(obj) <- "dummy"
  
  expect_error(dbDataType(con, obj))
  expect_error(AthenaDataType(obj))
})

test_that("Explain Plan", {
  
  sql = "select * from iris"
  actual = athena_explain(con, sql)
  
  expected = "EXPLAIN (FORMAT text) 'select * from iris'"
  expect_equal(dplyr::sql(expected), actual)
})
