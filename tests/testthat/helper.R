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

# test function to test the s3 upload location
test_s3_upload_location <- function(x, 
                               schema, 
                               name,
                               partition = NULL,
                               s3.location= NULL,
                               file.type = NULL,
                               compress = NULL,
                               append = FALSE){
  # formatting s3 partitions
  partition <- unlist(partition)
  partition <- paste(names(partition), unname(partition), sep = "=", collapse = "/")
  
  # s3_file name
  FileType <- if(compress) noctua:::Compress(file.type, compress) else file.type
  FileName <- paste(if (length(x) > 1) paste0(name,"_", 1:length(x)) else name, FileType, sep = ".")
  
  # s3 bucket and key split
  s3_info <- noctua:::split_s3_uri(s3.location)
  s3_info$key <- gsub("/$", "", s3_info$key)
  
  # Append data to existing s3 location
  if(append) {return(paste(s3_info$key, partition, FileName, sep = "/"))}
  
  if (partition != "") partition <- paste0(partition, "/")
  split_key <- unlist(strsplit(s3_info$key,"/"))
  
  # remove name from s3 key
  if(split_key[length(split_key)] == name || length(split_key) == 0)  split_key <- split_key[-length(split_key)]
  
  # remove schema from s3 key
  if(any(schema == split_key))  split_key <- split_key[-which(schema == split_key)]
  
  s3_info$key <- paste(split_key, collapse = "/")
  if (s3_info$key != "") s3_info$key <- paste0(s3_info$key, "/")
  
  # s3 folder
  schema <- paste0(schema, "/")
  name <- paste0(name, "/")
  
  # S3 new syntax #73
  sprintf("%s%s%s%s%s", s3_info$key, schema, name, partition, FileName)
}

s3_loc <- list(exp_s3_1 = "path/to/file/test/dummy_file/dummy_file.csv",
               exp_s3_2 = "path/to/file/YEAR=2000/dummy_file.csv.gz",
               exp_s3_3 = c("path/to/test/dummy_file/YEAR=2000/dummy_file_1.tsv", "path/to/test/dummy_file/YEAR=2000/dummy_file_2.tsv"),
               exp_s3_4 = c("path/to/test/dummy_file/YEAR=2000/dummy_file_1.tsv.gz","path/to/test/dummy_file/YEAR=2000/dummy_file_2.tsv.gz"),
               exp_s3_5 = "path/to/test/dummy_file/dummy_file.parquet",
               exp_s3_6 = "path/to/dummy_file/YEAR=2000/dummy_file.snappy.parquet")
