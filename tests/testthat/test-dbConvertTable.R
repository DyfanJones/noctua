context("Convert Table")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

test_that("Check if table is converted correctly",{
  skip_if_no_env()
  
  # initial sql query checks
  # file type NULL
  obj1 <- ctas_sql_with(partition = NULL, s3.location = NULL, file.type = "NULL", compress = TRUE)
  obj2 <- ctas_sql_with(partition = "hi", s3.location = NULL, file.type = "NULL", compress = TRUE)
  obj3 <- ctas_sql_with(partition = "hi", s3.location = "s3://mybucket/myfile/", file.type = "NULL", compress = TRUE)
  
  # file type csv
  expect_warning(ctas_sql_with(partition = NULL, s3.location = "s3://mybucket/myfile/", file.type = "csv", compress = TRUE))
  obj4 <- ctas_sql_with(partition = "hi", s3.location = "s3://mybucket/myfile/", file.type = "csv", compress = FALSE)
  
  # file type tsv
  obj5 <- ctas_sql_with(partition = NULL, s3.location = "s3://mybucket/myfile/", file.type = "tsv", compress = FALSE)
  
  # file type parquet
  obj6 <- ctas_sql_with(partition = NULL, s3.location = "s3://mybucket/myfile/", file.type = "parquet", compress = TRUE)
  
  # file type json
  obj7 <- ctas_sql_with(partition = NULL, s3.location = "s3://mybucket/myfile/", file.type = "json", compress = FALSE)
  
  # file type orc
  obj8 <- ctas_sql_with(partition = NULL, s3.location = "s3://mybucket/myfile/", file.type = "orc", compress = TRUE)
  
  con <- dbConnect(athena(),
                   s3_staging_dir = Sys.getenv("noctua_s3_query"))
  
  # check if key tables exist or not
  if(!dbExistsTable(con, "iris")) dbWriteTable(con, "iris", iris)
  if(dbExistsTable(con, "iris_parquet")) dbRemoveTable(con, "iris_parquet", confirm = T)
  if(dbExistsTable(con, "iris_orc_partitioned")) dbRemoveTable(con, "iris_orc_partitioned", confirm = T)
  
  dbConvertTable(con, 
                 obj = "iris",
                 name = "iris_parquet",
                 file.type = "parquet")
  
  dbConvertTable(con,
                 obj = SQL("select 
                               iris.*, 
                               date_format(current_date, '%Y%m%d') as time_stamp 
                             from iris"),
                 name = "iris_orc_partitioned",
                 file.type = "orc",
                 partition = "time_stamp")
  
  obj9 <- sapply(c("iris_parquet", "iris_orc_partitioned"), dbExistsTable, conn = con)
  
  expect_equal("", obj1)
  expect_equal("WITH (partitioned_by = ARRAY['hi'])\n", obj2)
  expect_equal("WITH (external_location ='s3://mybucket/myfile/',\npartitioned_by = ARRAY['hi'])\n", obj3)
  expect_equal("WITH (format = 'TEXTFILE',\nfield_delimiter = ',',\nexternal_location ='s3://mybucket/myfile/',\npartitioned_by = ARRAY['hi'])\n", obj4)
  expect_equal("WITH (format = 'TEXTFILE',\nfield_delimiter = '\t',\nexternal_location ='s3://mybucket/myfile/')\n", obj5)
  expect_equal("WITH (format = 'PARQUET',\nparquet_compression = 'SNAPPY',\nexternal_location ='s3://mybucket/myfile/')\n", obj6)
  expect_equal("WITH (format = 'JSON',\nexternal_location ='s3://mybucket/myfile/')\n", obj7)
  expect_equal("WITH (format = 'ORC',\norc_compression = 'SNAPPY',\nexternal_location ='s3://mybucket/myfile/')\n", obj8)
  sapply(obj9, expect_true)
})
