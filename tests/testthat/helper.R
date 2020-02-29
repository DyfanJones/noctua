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
DBI::SQL(paste0("CREATE EXTERNAL TABLE `default`.`test_df` (
  `x` INT,
  `y` STRING
)
ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
	LINES TERMINATED BY ", gsub("_","","'\\_n'"),
"\nLOCATION '",Sys.getenv("noctua_s3_tbl"),"default/test_df/'
TBLPROPERTIES (\"skip.header.line.count\"=\"1\");")),
tbl2 = 
DBI::SQL(paste0("CREATE EXTERNAL TABLE `default`.`test_df` (
  `x` INT,
  `y` STRING
)
ROW FORMAT DELIMITED
	FIELDS TERMINATED BY '\t'
	LINES TERMINATED BY ", gsub("_","","'\\_n'"),
         "\nLOCATION '",Sys.getenv("noctua_s3_tbl"),"default/test_df/'
TBLPROPERTIES (\"skip.header.line.count\"=\"1\");")), 
tbl3 = 
DBI::SQL(paste0("CREATE EXTERNAL TABLE `default`.`test_df` (
  `x` INT,
  `y` STRING
)
STORED AS PARQUET
LOCATION '",Sys.getenv("noctua_s3_tbl"),"default/test_df/'\n;")),
tbl4 = 
  DBI::SQL(paste0("CREATE EXTERNAL TABLE `default`.`test_df` (\n  `x` INT,\n  `y` STRING\n)
PARTITIONED BY (timestamp STRING)
STORED AS PARQUET
LOCATION '",Sys.getenv("noctua_s3_tbl"),"default/test_df/'\n;")))


# static Athena Query Request Tests
athena_test_req1 <-
         list(OutputLocation = Sys.getenv("noctua_s3_query"),
              EncryptionConfiguration = list(EncryptionOption = "SSE_S3",
                                             KmsKey = "test_key"))
athena_test_req2 <-
       list(OutputLocation = Sys.getenv("noctua_s3_query"),
            EncryptionConfiguration = list(EncryptionOption = "SSE_S3"))
athena_test_req3 <- list(OutputLocation = Sys.getenv("noctua_s3_query"))
athena_test_req4 <- list(OutputLocation = Sys.getenv("noctua_s3_query"))

# static s3 path location
s3_loc <- list(exp_s3_1 = "path/to/file/test/dummy_file/dummy_file.csv",
               exp_s3_2 = "path/to/file/YEAR=2000/dummy_file.csv.gz",
               exp_s3_3 = c("path/to/test/dummy_file/YEAR=2000/dummy_file_1.tsv", "path/to/test/dummy_file/YEAR=2000/dummy_file_2.tsv"),
               exp_s3_4 = c("path/to/test/dummy_file/YEAR=2000/dummy_file_1.tsv.gz","path/to/test/dummy_file/YEAR=2000/dummy_file_2.tsv.gz"),
               exp_s3_5 = "path/to/test/dummy_file/dummy_file.parquet",
               exp_s3_6 = "path/to/dummy_file/YEAR=2000/dummy_file.snappy.parquet")


show_ddl <- SQL('CREATE EXTERNAL TABLE `default.test_df`(\n  `w` timestamp, \n  `x` int, \n  `y` string, \n  `z` boolean)\nPARTITIONED BY ( \n  `timestamp` string)\nROW FORMAT DELIMITED \n  FIELDS TERMINATED BY \'\\t\' \n  LINES TERMINATED BY \'\\n\' \nSTORED AS INPUTFORMAT \n  \'org.apache.hadoop.mapred.TextInputFormat\' \nOUTPUTFORMAT \n  \'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\'\nLOCATION\n  \'s3://test-rathena/default/test_df\'\nTBLPROPERTIES (\n  \'skip.header.line.count\'=\'1\')')
