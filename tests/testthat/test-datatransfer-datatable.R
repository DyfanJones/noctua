context("data transfer data.table")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

s3.location1 <- paste0(Sys.getenv("noctua_s3_tbl"),"test_df/")
s3.location2 <- Sys.getenv("noctua_s3_tbl")

df <- data.frame(w = as.POSIXct((Sys.time()-9):Sys.time(), origin = "1970-01-01", tz = "UTC"),
                 x = 1:10,
                 y = c(letters[1:8], c(" \\t\\t\\n 123 \" \\t\\t\\n ", ",15 \"")), 
                 z = sample(c(TRUE, FALSE), 10, replace = T),
                 stringsAsFactors = F)

test_that("Testing data transfer between R and athena datatable", {
  skip_if_no_env()
  
  noctua_options()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena(),
                   s3_staging_dir = Sys.getenv("noctua_s3_query"))
  
  # testing if bigint is transferred correctly
  df2 <- data.frame(var1 = sample(letters, 10, replace = T),
                    var2 = bit64::as.integer64(1:10),
                    stringsAsFactors = F)
  
  DATE <- Sys.Date()
  dbWriteTable(con, "test_df", df, overwrite = T, partition = c("timesTamp" = format(DATE, "%Y%m%d")), s3.location = s3.location1)
  dbWriteTable(con, "test_df2", df, 
               overwrite = T,
               partition = c("year" = format(DATE, "%Y"),
                             "month" = format(DATE, "%m"),
                             "DAY" = format(DATE, "%d")),
               s3.location = s3.location2)
  
  dbWriteTable(con, "df_bigint", df2, overwrite = T, s3.location = s3.location2)
  dbWriteTable(con, "mtcars2", mtcars, overwrite = T, compress = T) # mtcars used to test data.frame with row.names
  
  # if data.table is available in namespace result returned as data.table
  test_df <- as.data.frame(dbGetQuery(con, paste0("select w, x, y, z from test_df where timestamp ='", format(DATE, "%Y%m%d"),"'")))
  test_df2 <- as.data.frame(dbGetQuery(con, paste0("select w, x, y, z from test_df2 where year = '", format(DATE, "%Y"), "' and month = '",format(DATE, "%m"), "' and day = '", format(DATE, "%d"),"'")))
  test_df3 <- as.data.frame(dbGetQuery(con, "select * from df_bigint"))
  test_df4 <- as.data.frame(dbGetQuery(con, "select * from mtcars2"))
  
  expect_equal(test_df, df)
  expect_equal(test_df2, df)
  expect_equal(test_df3,df2)
  expect_equal(test_df4, sqlData(con, mtcars))
})

test_that("Testing data transfer between R and athena json file", {
  skip_if_no_env()
  skip_if_package_not_avialable("jsonlite")
  
  con <- dbConnect(athena(),
                   s3_staging_dir = Sys.getenv("noctua_s3_query"))
  
  if(dbExistsTable(con, "test_df3")){
    dbRemoveTable(con, "test_df3", confirm = T)
  }
  
  dbWriteTable(con, "test_df3", df, overwrite = T, file.type = "json")
  
  test_df <- as.data.frame(dbGetQuery(con, "select * from test_df3"))
  expect_equal(test_df, df)
})

test_that("Test unload athena query data.table",{
  skip_if_no_env()
  skip_if_package_not_avialable("arrow")
  
  con <- dbConnect(
    athena(),
    s3_staging_dir = Sys.getenv("noctua_s3_query"))
  
  df = dbGetQuery(con, "select 1 as n", unload = T)
  
  expect_s3_class(df, "data.table")
  expect_equal(df$n, 1)
})

test_that("Write can handle an empty data frame", {
  skip_if_no_env()
  
  noctua_options()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena(),
                   s3_staging_dir = Sys.getenv("noctua_s3_query"))
  df <- data.frame(x = integer())
  
  if (dbExistsTable(con, "test_df")) {
    dbRemoveTable(con, "test_df", confirm = T)
  }
  
  # can create a new table
  dbWriteTable(con, "test_df", df)
  expect_equal(as.data.frame(dbReadTable(con, "test_df")), df)
  
  dbWriteTable(con, "test_df", df, append = TRUE)
  expect_equal(as.data.frame(dbReadTable(con, "test_df")), df)
})
