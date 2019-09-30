context("data transfer")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("pawsathena_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("pawsathena_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("pawsathena_s3_tbl"): "s3://path/to/bucket/"

s3.location1 <- paste0(Sys.getenv("pawsathena_s3_tbl"),"test_df/")
s3.location2 <- Sys.getenv("pawsathena_s3_tbl")

test_that("Testing data transfer between R and athena", {
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena())
  
  df <- data.frame(x = 1:10,
                   y = letters[1:10], 
                   z = sample(c(TRUE, FALSE), 10, replace = T),
                   stringsAsFactors = F)
  
  DATE <- Sys.Date()
  dbWriteTable(con, "test_df", df, overwrite = T, partition = c("timesTamp" = format(DATE, "%Y%m%d")), s3.location = s3.location1)
  dbWriteTable(con, "test_df2", df, 
               overwrite = T,
               partition = c("year" = format(DATE, "%Y"),
                             "month" = format(DATE, "%m"),
                             "DAY" = format(DATE, "%d")),
               s3.location = s3.location2)
  
  # if data.table is available in namespace result returned as data.table
  test_df <- as.data.frame(dbGetQuery(con, paste0("select x, y, z from test_df where timestamp ='",format(Sys.Date(), "%Y%m%d"),"'")))
  test_df2 <- as.data.frame(dbGetQuery(con, paste0("select x, y, z from test_df2 where year = '",format(DATE, "%Y"), "' and month = '",format(DATE, "%m"), "' and day = '", format(DATE, "%d"),"'")))
  expect_equal(test_df,df)
  expect_equal(test_df2,df)
})