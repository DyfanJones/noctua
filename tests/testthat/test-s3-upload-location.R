context("S3 upload location")

test_that("Check if the S3 upload location is correctly built",{
  # Test connection is using AWS CLI to set profile_name 
  
  x <- c("dummy_file")
  schema <- "test"
  name <- "dummy_file"
  s3.location <- "s3://bucket/path/to/file"
  partition <- c("YEAR"= 2000)
  
  # schema and name not s3 location 
  s3_1 <- RAthena:::s3_upload_location(x, schema, name, file.type = "csv", compress = F, s3.location = s3.location, append = F)
  s3_2 <- RAthena:::s3_upload_location(x, schema, name, file.type = "csv", compress = T, partition = partition, s3.location = s3.location, append = T)
  
  x <- c("dummy_file","dummy_file")
  schema <- "test"
  name <- "dummy_file"
  s3.location <- "s3://bucket/path/to/test/dummy_file"
  partition <- c("YEAR"= 2000)
  
  # schema and name in s3 location 
  s3_3 <- RAthena:::s3_upload_location(x, schema, name, file.type = "tsv", compress = F, partition = partition, s3.location = s3.location, append = F)
  s3_4 <- RAthena:::s3_upload_location(x, schema, name, file.type = "tsv", compress = T, partition = partition, s3.location = s3.location, append = T)
  
  x <- c("dummy_file")
  schema <- "test"
  name <- "dummy_file"
  s3.location <- "s3://bucket/path/to/dummy_file/"
  partition <- c("YEAR"= 2000)
  
  # / at end of s3 location
  s3_5 <- RAthena:::s3_upload_location(x, schema, name, file.type = "parquet", compress = F, s3.location = s3.location, append = F)
  s3_6 <- RAthena:::s3_upload_location(x, schema, name, file.type = "parquet", compress = T, partition = partition, s3.location = s3.location, append = T)
  
  x <- c("dummy_file")
  schema <- "test"
  name <- "dummy_file"
  s3.location <- "s3://bucket/path/to/dummy_file/"
  partition <- c("YEAR"= 2000)
  
  # / at end of s3 location
  s3_7 <- RAthena:::s3_upload_location(x, schema, name, file.type = "json", compress = F, s3.location = s3.location, append = F)
  s3_8 <- RAthena:::s3_upload_location(x, schema, name, file.type = "json", compress = T, partition = partition, s3.location = s3.location, append = T)
  
  expect_equal(s3_1[[2]], s3_loc$exp_s3_1)
  expect_equal(s3_2[[2]], s3_loc$exp_s3_2)
  expect_equal(s3_3[[2]], s3_loc$exp_s3_3)
  expect_equal(s3_4[[2]], s3_loc$exp_s3_4)
  expect_equal(s3_5[[2]], s3_loc$exp_s3_5)
  expect_equal(s3_6[[2]], s3_loc$exp_s3_6)
  expect_equal(s3_7[[2]], s3_loc$exp_s3_7)
  expect_equal(s3_8[[2]], s3_loc$exp_s3_8)
})