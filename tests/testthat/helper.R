# helper function to skip test if noctua unit test environment variables not set
skip_if_no_env <- function(){
  have_arn <- Sys.getenv("noctua_arn") != "" 
  have_query <- is.s3_uri(Sys.getenv("noctua_s3_query"))
  have_tbl <- is.s3_uri(Sys.getenv("noctua_s3_tbl"))
  if (!all(have_arn, have_query, have_tbl))
    skip("Environment variables are not set for testing")
}

# helper function to skip tests if we don't have the suggested package
skip_if_package_not_avialable <- function(pkg) {
  if (!nzchar(system.file(package = pkg)))
    skip(sprintf("`%s` not available for testing", pkg))
}

# expected athena ddl's
# expected athena ddl's
tbl_ddl <- 
  list(tbl1 = 
         DBI::SQL(paste0("CREATE EXTERNAL TABLE `AwsDataCatalog`.`default`.`test_df` (
  `x` INT,
  `y` STRING
)
ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
	LINES TERMINATED BY ", gsub("_","","'\\_n'"),
                  "\nLOCATION '",Sys.getenv("noctua_s3_tbl"),"test_df/default/test_df/'
TBLPROPERTIES (\"skip.header.line.count\"=\"1\");")),
tbl2 = 
  DBI::SQL(paste0("CREATE EXTERNAL TABLE `AwsDataCatalog`.`default`.`test_df` (
  `x` INT,
  `y` STRING
)
ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
	LINES TERMINATED BY ", gsub("_","","'\\_n'"),
           "\nLOCATION '",Sys.getenv("noctua_s3_tbl"),"test_df/default/test_df/'
TBLPROPERTIES (\"skip.header.line.count\"=\"1\",
\t\t'compressionType'='gzip');")),
tbl3 = 
  DBI::SQL(paste0("CREATE EXTERNAL TABLE `AwsDataCatalog`.`default`.`test_df` (
  `x` INT,
  `y` STRING
)
ROW FORMAT DELIMITED
\tFIELDS TERMINATED BY '	'
\tLINES TERMINATED BY ", gsub("_","","'\\_n'"),"
LOCATION '",Sys.getenv("noctua_s3_tbl"),"test_df/default/test_df/'
TBLPROPERTIES (\"skip.header.line.count\"=\"1\");")),
tbl4 = 
  DBI::SQL(paste0("CREATE EXTERNAL TABLE `AwsDataCatalog`.`default`.`test_df` (
  `x` INT,
  `y` STRING
)
ROW FORMAT DELIMITED
\tFIELDS TERMINATED BY '	'
\tLINES TERMINATED BY ", gsub("_","","'\\_n'"),"
LOCATION '",Sys.getenv("noctua_s3_tbl"),"test_df/default/test_df/'
TBLPROPERTIES (\"skip.header.line.count\"=\"1\",
\t\t'compressionType'='gzip');")), 
tbl5 = 
  DBI::SQL(paste0("CREATE EXTERNAL TABLE `AwsDataCatalog`.`default`.`test_df` (
  `x` INT,
  `y` STRING
)
STORED AS PARQUET
LOCATION '",Sys.getenv("noctua_s3_tbl"),"test_df/default/test_df/'\n;")),
tbl6 = 
  DBI::SQL(paste0("CREATE EXTERNAL TABLE `AwsDataCatalog`.`default`.`test_df` (
  `x` INT,
  `y` STRING
)
PARTITIONED BY (`timestamp` STRING)
STORED AS PARQUET
LOCATION '",Sys.getenv("noctua_s3_tbl"),"test_df/default/test_df/'
tblproperties (\"parquet.compress\"=\"SNAPPY\");")),
tbl7 = 
  DBI::SQL(paste0("CREATE EXTERNAL TABLE `AwsDataCatalog`.`default`.`test_df` (
  `x` INT,
  `y` STRING
)
ROW FORMAT  serde 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '",Sys.getenv("noctua_s3_tbl"),"test_df/default/test_df/'\n")),
tbl8 = 
  DBI::SQL(paste0("CREATE EXTERNAL TABLE `AwsDataCatalog`.`default`.`test_df` (
  `x` INT,
  `y` STRING
)
PARTITIONED BY (`timestamp` STRING)
ROW FORMAT  serde 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '",Sys.getenv("noctua_s3_tbl"),"test_df/default/test_df/'\n")))


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

show_ddl <- DBI::SQL(paste0('CREATE EXTERNAL TABLE `default.test_df`(\n  `w` timestamp, \n  `x` int, \n  `y` string, \n  `z` boolean)\nPARTITIONED BY ( \n  `timestamp` string)\nROW FORMAT DELIMITED \n  FIELDS TERMINATED BY \'\\t\' \n  LINES TERMINATED BY \'\\n\' \nSTORED AS INPUTFORMAT \n  \'org.apache.hadoop.mapred.TextInputFormat\' \nOUTPUTFORMAT \n  \'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\'\nLOCATION\n  \'' ,Sys.getenv("noctua_s3_tbl"), 'test_df/default/test_df\'\nTBLPROPERTIES (\n  \'skip.header.line.count\'=\'1\')'))

expected_stat_output = c(
  "EngineExecutionTimeInMillis",
  "DataScannedInBytes",
  "DataManifestLocation",
  "TotalExecutionTimeInMillis",
  "QueryQueueTimeInMillis",
  "QueryPlanningTimeInMillis",
  "ServiceProcessingTimeInMillis"
)
