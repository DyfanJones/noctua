context("Athena Work Groups")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

test_that("Create and Delete Athena Work Groups",{
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena())
  
  output1 <- list_work_groups(con)
  work_groups1 <- sapply(output1, function(x) x$Name)
  
  create_work_group(con, "demo_work_group", description = "This is a demo work group",
                    tags = tag_options(key= "demo_work_group", value = "demo_01"))
  
  output2 <- list_work_groups(con)
  work_groups2 <- sapply(output2, function(x) x$Name)
  
  meta_data1 <- get_work_group(con, "demo_work_group")$Description
  update_work_group(con, "demo_work_group", description = "This is a demo work group update")
  meta_data2 <- get_work_group(con, "demo_work_group")$Description
  
  delete_work_group(con, "demo_work_group")
  
  output3 <- list_work_groups(con)
  work_groups3 <- sapply(output3, function(x) x$Name)
  
  dbDisconnect(con)
  
  expect_equal(any(grepl("demo_work_group", output1)), FALSE)
  expect_equal(any(grepl("demo_work_group", output2)), TRUE)
  expect_equal(any(grepl("demo_work_group", output3)), FALSE)
  expect_equal(meta_data1, "This is a demo work group")
  expect_equal(meta_data2, "This is a demo work group update")
  
  # clean up system environmental variables
  Sys.unsetenv("AWS_ACCESS_KEY_ID")
  Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
  Sys.unsetenv("AWS_SESSION_TOKEN")
  Sys.unsetenv("AWS_PROFILE")
  Sys.unsetenv("AWS_REGION")
})
