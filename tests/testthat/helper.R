# helper function to skip test if noctua unit test environment variables not set
skip_if_no_env <- function(){
  have_arn <- Sys.getenv("noctua_arn") != "" 
  have_query <- is.s3_uri(Sys.getenv("noctua_s3_query"))
  have_tbl <- is.s3_uri(Sys.getenv("noctua_s3_tbl"))
  if(!have_arn || !have_query|| !have_tbl) skip("Environment variables are not set for testing")
}

# expected athena ddl's
tbl_ddl <- 
  list(tbl1 = 
DBI::SQL(paste0("CREATE EXTERNAL TABLE test_df (
  x INT,
  y STRING
)
ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
	ESCAPED BY '\\\\'
	LINES TERMINATED BY ", gsub("_","","'\\_n'"),
"\nLOCATION '",Sys.getenv("noctua_s3_tbl"),"test_df/'
TBLPROPERTIES (\"skip.header.line.count\"=\"1\");")),
tbl2 = 
DBI::SQL(paste0("CREATE EXTERNAL TABLE test_df (
  x INT,
  y STRING
)
ROW FORMAT DELIMITED
	FIELDS TERMINATED BY '\t'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY ", gsub("_","","'\\_n'"),
           "\nLOCATION '",Sys.getenv("noctua_s3_tbl"),"test_df/'
TBLPROPERTIES (\"skip.header.line.count\"=\"1\");")), 
tbl3 = 
  DBI::SQL(paste0("CREATE EXTERNAL TABLE test_df (
  x INT,
  y STRING
)
STORED AS PARQUET
LOCATION '",Sys.getenv("noctua_s3_tbl"),"test_df/'\n;")),
tbl4 = 
  DBI::SQL(paste0("CREATE EXTERNAL TABLE test_df (\n  x INT,\n  y STRING\n)
PARTITIONED BY (timestamp STRING)
STORED AS PARQUET
LOCATION '",Sys.getenv("noctua_s3_tbl"),"test_df/'\n;")))


# static Athena Query Request Tests
athena_test_req1 <-
         list(OutputLocation = Sys.getenv("noctua_s3_query"),
              EncryptionConfiguration = list(EncryptionOption = "SSE_S3",
                                             KmsKey = "test_key"))
athena_test_req2 <-
       list(OutputLocation = Sys.getenv("noctua_s3_query"),
            EncryptionConfiguration = list(EncryptionOption = "SSE_S3"))
athena_test_req3 <- list(OutputLocation = Sys.getenv("noctua_s3_query"))

# helper to delete all objects in prefix folder
delete_prefix_objects <- function(conn, Prefix, Quiet = F){
  bucket <- split_s3_uri(conn@info$s3_staging)$bucket
  content <- conn@ptr$S3$list_objects(Bucket = bucket, 
                                      Prefix = Prefix)
  content_list <- lapply(content$Contents, function(x) list(Key= x$Key))
  conn@ptr$S3$delete_objects(Bucket = bucket,
                             Delete = list(Objects = content_list, Quiet = Quiet))
}

delete_prefix_objects_v2 <- function(conn, Prefix){
  bucket <- split_s3_uri(conn@info$s3_staging)$bucket
  content <- conn@ptr$S3$list_objects(Bucket = bucket, 
                                      Prefix = Prefix)
  content_list <- sapply(content$Contents, function(x) x$Key)
  sapply(content_list, function(x) conn@ptr$S3$delete_object(Bucket = bucket, Key = x))
}