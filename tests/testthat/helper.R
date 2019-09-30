# helper function to skip test if pawsathena unit test environment variables not set
skip_if_no_env <- function(){
  have_arn <- Sys.getenv("pawsathena_arn") != "" 
  have_query <- is.s3_uri(Sys.getenv("pawsathena_s3_query"))
  have_tbl <- is.s3_uri(Sys.getenv("pawsathena_s3_tbl"))
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
"\nLOCATION '",Sys.getenv("pawsathena_s3_tbl"),"test_df/'
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
           "\nLOCATION '",Sys.getenv("pawsathena_s3_tbl"),"test_df/'
TBLPROPERTIES (\"skip.header.line.count\"=\"1\");")), 
tbl3 = 
  DBI::SQL(paste0("CREATE EXTERNAL TABLE test_df (
  x INT,
  y STRING
)
STORED AS PARQUET
LOCATION '",Sys.getenv("pawsathena_s3_tbl"),"test_df/'\n;")),
tbl4 = 
  DBI::SQL(paste0("CREATE EXTERNAL TABLE test_df (\n  x INT,\n  y STRING\n)
PARTITIONED BY (timestamp STRING)
STORED AS PARQUET
LOCATION '",Sys.getenv("pawsathena_s3_tbl"),"test_df/'\n;")))


# static Athena Query Request Tests
athena_test_req1 <-
  list(QueryString = "select * from test_query",
       QueryExecutionContext = list(Database = "default"),
       ResultConfiguration = list(OutputLocation = Sys.getenv("pawsathena_s3_query"),
                                  EncryptionConfiguration = list(EncryptionOption = "SSE_S3",
                                                                 KmsKey = "test_key")),
       WorkGroup = "test_group")
athena_test_req2 <-
  list(QueryString = "select * from test_query",
       QueryExecutionContext = list(Database = "default"),
       ResultConfiguration = list(OutputLocation = Sys.getenv("pawsathena_s3_query"),
                                  EncryptionConfiguration = list(EncryptionOption = "SSE_S3")),
       WorkGroup = "test_group")
athena_test_req3 <-
  list(QueryString = "select * from test_query",
       QueryExecutionContext = list(Database = "default"),
       ResultConfiguration = list(OutputLocation = Sys.getenv("pawsathena_s3_query")),
       WorkGroup = "test_group")
athena_test_req4 <-
  list(QueryString = "select * from test_query",
       QueryExecutionContext = list(Database = "default"),
       ResultConfiguration = list(OutputLocation = Sys.getenv("pawsathena_s3_query")))
