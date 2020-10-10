context("S3 upload location")

test_that("Check if the S3 upload location is correctly built",{
  # Test connection is using AWS CLI to set profile_name 
  conn <- dbConnect(noctua::athena(),
                    s3_staging_dir = Sys.getenv("noctua_s3_query"))
  
  # schema and name not s3 location 
  name <- "dummy_table"
  s3.location <- "s3://bucket/path/to/file"
  partition <- c("YEAR"= 2000)
  s3_1 <- noctua:::s3_upload_location(conn, s3.location, name, partition)
  s3_2 <- noctua:::s3_upload_location(conn, s3.location, name, NULL)

  # schema in s3 location 
  s3.location <- "s3://bucket/path/to/file/schema"
  name <- "schema.dummy_table"
  s3_3 <- noctua:::s3_upload_location(conn, s3.location, name, partition)
  s3_4 <- noctua:::s3_upload_location(conn, s3.location, name, NULL)
  
  # name in s3 location 
  s3.location <- "s3://bucket/path/to/file/dummy_table"
  name <- "schema.dummy_table"
  s3_3 <- noctua:::s3_upload_location(conn, s3.location, name, partition)
  s3_4 <- noctua:::s3_upload_location(conn, s3.location, name, NULL)

  # schema different s3 location
  s3.location <- "s3://bucket/path/schema/to/file"
  name <- "schema.dummy_table"
  s3_5 <- noctua:::s3_upload_location(conn, s3.location, name, partition)
  s3_6 <- noctua:::s3_upload_location(conn, s3.location, name, NULL)
  
  # schema and table in s3 location
  s3.location <- "s3://bucket/path/to/file/schema/dummy_table"
  name <- "schema.dummy_table"
  s3_7 <- noctua:::s3_upload_location(conn, s3.location, name, partition)
  s3_8 <- noctua:::s3_upload_location(conn, s3.location, name, NULL)
  
  # s3 location for existing table (should ignore schema/name/ partition)
  s3.location <- "s3://bucket/path/to/file/dummy_table"
  name <- "schema.dummy_table"
  s3_9 <- noctua:::s3_upload_location(conn, s3.location, name, partition, TRUE)
  
  expect_equal(s3_1, list(Bucket = "bucket", Key = "path/to/file", Schema = "default", Name = "dummy_table", Partition = "YEAR=2000"))
  expect_equal(s3_2, list(Bucket = "bucket", Key = "path/to/file", Schema = "default", Name = "dummy_table", Partition = NULL))
  expect_equal(s3_3, list(Bucket = "bucket", Key = "path/to/file/dummy_table", Schema = "schema", Name = NULL, Partition = "YEAR=2000"))
  expect_equal(s3_4, list(Bucket = "bucket", Key = "path/to/file/dummy_table", Schema = "schema", Name = NULL, Partition = NULL))
  expect_equal(s3_5, list(Bucket = "bucket", Key = "path/schema/to/file", Schema = NULL, Name = "dummy_table", Partition = "YEAR=2000"))
  expect_equal(s3_6, list(Bucket = "bucket", Key = "path/schema/to/file", Schema = NULL, Name = "dummy_table", Partition = NULL))
  expect_equal(s3_7, list(Bucket = "bucket", Key = "path/to/file/schema/dummy_table", Schema = NULL, Name = NULL, Partition = "YEAR=2000"))
  expect_equal(s3_8, list(Bucket = "bucket", Key = "path/to/file/schema/dummy_table", Schema = NULL, Name = NULL, Partition = NULL))
  expect_equal(s3_9, list(Bucket = "bucket", Key = "path/to/file/dummy_table", Schema = NULL, Name = NULL, Partition = NULL))
})
