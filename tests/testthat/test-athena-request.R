context("Athena Request")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

test_that("Check if Athena Request created correctly",{
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con1 <- dbConnect(athena(),
                    encryption_option = "SSE_S3",
                    kms_key = "test_key",
                    work_group = "test_group",
                    s3_staging_dir = Sys.getenv("noctua_s3_query"))
  
  con2 <- dbConnect(athena(),
                    encryption_option = "SSE_S3",
                    work_group = "test_group",
                    s3_staging_dir = Sys.getenv("noctua_s3_query"))
  
  con3 <- dbConnect(athena(),
                    s3_staging_dir = Sys.getenv("noctua_s3_query"))
  
  R1 <- noctua:::ResultConfiguration(con1)
  R2 <- noctua:::ResultConfiguration(con2)
  R3 <- noctua:::ResultConfiguration(con3)

  expect_equal(R1, athena_test_req1)
  expect_equal(R2, athena_test_req2)
  expect_equal(R3, athena_test_req3)
  
  # clean up system environmental variables
  Sys.unsetenv("AWS_ACCESS_KEY_ID")
  Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
  Sys.unsetenv("AWS_SESSION_TOKEN")
  Sys.unsetenv("AWS_PROFILE")
  Sys.unsetenv("AWS_REGION")
})