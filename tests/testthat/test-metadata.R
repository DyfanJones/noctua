context("Athena Metadata")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

df_col_info <- data.frame(
  field_name = c("w","x","y", "z", "timestamp"),
  type = c("timestamp", "integer", "varchar", "boolean", "varchar"),
  stringsAsFactors = F
)
con_info = c(
  "profile_name",
  "s3_staging",
  "db.catalog",
  "dbms.name",
  "work_group",
  "poll_interval",
  "encryption_option",
  "kms_key",
  "expiration",
  "keyboard_interrupt",
  "region_name",
  "paws",
  "noctua",
  "timezone",
  "endpoint_override"
)
col_info_exp = c("w","x","y", "z", "timestamp")

test_that("Returning meta data",{
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con = dbConnect(athena())
  
  res1 = dbExecute(con, "select * from test_df")
  res2 = dbSendStatement(con, "select * from test_df")
  res_out2 = dbHasCompleted(res2)
  res_out1 = dbHasCompleted(res1)
  res_info = dbGetInfo(res1)
  res_stat = dbStatistics(res1)
  column_info1 = dbColumnInfo(res1)
  column_info2 = dbListFields(con, "test_df")
  con_info_exp = names(dbGetInfo(con))
  list_tbl1 = any(grepl("test_df", dbListTables(con, schema="default")))
  list_tbl2 = nrow(dbGetTables(con, schema="default")[TableName == "test_df"]) == 1
  list_tbl3 = nrow(dbGetTables(con)[Schema == "default" & TableName == "test_df"]) == 1
  list_tbl4 = any(grepl("test_df", dbListTables(con)))
  partition1 = grepl("timestamp", dbGetPartition(con, "test_df")[[1]])
  
  partition2 = names(dbGetPartition(con, "test_df", .format = T)) == "timestamp"
  noctua_options("vroom")
  partition3 = names(dbGetPartition(con, "test_df", .format = T)) == "timestamp"
  
  noctua_options()
  db_show_ddl = gsub(", \n  'transient_lastDdlTime'.*",")", dbShow(con, "test_df"))
  db_info = dbGetInfo(con)
  
  name1 <- db_detect(con, "table1")
  name2 <- db_detect(con, "mydatabase.table1")
  name3 <- db_detect(con, "mycatalog.mydatabase.table1")

  expect_equal(dbGetStatement(res2), "select * from test_df")
  
  dbClearResult(res1)
  dbDisconnect(con)
  
  expect_equal(column_info1, df_col_info)
  expect_equal(column_info2, col_info_exp)
  expect_equal(con_info[order(con_info)], con_info_exp[order(con_info_exp)])
  expect_true(list_tbl1)
  expect_true(list_tbl2)
  expect_true(list_tbl3)
  expect_true(list_tbl4)
  expect_true(partition1)
  expect_true(partition2)
  expect_true(partition3)
  expect_equal(db_show_ddl, show_ddl)
  expect_warning(noctua:::time_check(Sys.time() + 10))
  expect_error(noctua:::pkg_method("made_up", "made_up_pkg"))
  expect_false(noctua:::is.s3_uri(NULL))
  expect_true(is.list(db_info))
  expect_error(dbGetInfo(con))
  expect_true(res_out1)
  expect_true(inherits(res_out2, "logical"))
  expect_equal(
    sort(names(res_info)), 
    c("OutputLocation", "Query", "QueryExecutionId", "StateChangeReason", "StatementType",
      "Statistics", "Status", "UnloadDir", "WorkGroup"))
  expect_true(is.list(res_stat))
  expect_error(con_error_msg(res1, "dummy message"), "dummy message")
  expect_equal(name1, list("db.catalog" = "AwsDataCatalog", "dbms.name" = "default", "table" = "table1"))
  expect_equal(name2, list("db.catalog" = "AwsDataCatalog", "dbms.name" = "mydatabase", "table" = "table1"))
  expect_equal(name3, list("db.catalog" = "mycatalog", "dbms.name" = "mydatabase", "table" = "table1"))
})

test_that("test connection when timezone is NULL", {
  skip_if_no_env()
  
  con <- dbConnect(athena(), timezone = NULL)
  
  expect_equal(con@info$timezone, "UTC")
})

test_that("test endpoints", {
  skip_if_no_env()
  
  con1 = dbConnect(athena(), endpoint_override = "https://athena.eu-west-2.amazonaws.com/")
  con2 = dbConnect(
    athena(),
    region_name = "us-east-2",
    
    # Change default endpoints:
    # athena: "https://athena.us-east-2.amazonaws.com"
    # s3: "https://s3.us-east-2.amazonaws.com"
    # glue: "https://glue.us-east-2.amazonaws.com"
    
    endpoint_override = list(
      athena = "https://athena-fips.us-east-2.amazonaws.com/",
      s3 = "https://s3-fips.us-east-2.amazonaws.com/"
    )
  )
  
  expect_equal(as.character(con1@ptr$Athena$.internal$config$endpoint), "https://athena.eu-west-2.amazonaws.com/")
  expect_equal(as.character(con2@ptr$Athena$.internal$config$endpoint), "https://athena-fips.us-east-2.amazonaws.com/")
  expect_equal(as.character(con2@ptr$S3$.internal$config$endpoint), "https://s3-fips.us-east-2.amazonaws.com/")
})
