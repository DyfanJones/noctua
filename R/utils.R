# split s3 uri
split_s3_uri <- function(uri) {
  stopifnot(is.s3_uri(uri))
  path <- gsub('^s3://', '', uri)
  list(
    bucket = gsub('/.*$', '', path),
    key = gsub('^[a-z0-9][a-z0-9\\.-]+[a-z0-9]/', '', path)
  )
}

# validation check of s3 uri
is.s3_uri <- function(x) {
  if(is.null(x)) return(FALSE)
  regex <- '^s3://[a-z0-9][a-z0-9\\.-]+[a-z0-9](/(.*)?)?$'
  grepl(regex, x)
}

# holds functions until athena query competed
poll <- function(res){
  class_poll <- res@connection@info$poll_interval
  while (TRUE){
    poll_interval <- class_poll %||% rand_poll()
    tryCatch(query_execution <- res@connection@ptr$Athena$get_query_execution(QueryExecutionId = res@info$QueryExecutionId))
    if (query_execution$QueryExecution$Status$State %in% c("SUCCEEDED", "FAILED", "CANCELLED")){
      return (query_execution)
    } else {Sys.sleep(poll_interval)}
  }
}

# added a random poll wait time
rand_poll <- function() {runif(n = 1, min = 50, max = 100) / 100}

# checks if resource is active
resource_active <- function(dbObj){
  UseMethod("resource_active")
}

# checks is dbObj is active
resource_active.AthenaConnection <- function(dbObj){
  if(length(dbObj@ptr) != 0) return(TRUE) else return(FALSE)
}

resource_active.AthenaResult <- function(dbObj){
  if(length(dbObj@connection@ptr) !=0) return(TRUE) else (FALSE)
}

# set up athena request call
ResultConfiguration <- function(conn){
  # creating ResultConfiguration 
  ResultConfiguration = list(OutputLocation = conn@info$s3_staging)
  
  # adding EncryptionConfiguration to ResultConfiguration
  if(!is.null(conn@info$encryption_option)){
    EncryptionConfiguration = list("EncryptionOption" = conn@info$encryption_option)
    EncryptionConfiguration["KmsKey"] = conn@info$kms_key
    ResultConfiguration["EncryptionConfiguration"] <- list(EncryptionConfiguration)
  }
  
  ResultConfiguration
}

# set up work group configuration
work_group_config <- function(conn,
                              EnforceWorkGroupConfiguration = FALSE,
                              PublishCloudWatchMetricsEnabled = FALSE,
                              BytesScannedCutoffPerQuery = 10000000L){
  config <- list()
  ResultConfiguration <- list(OutputLocation = conn@info$s3_staging)
  if(!is.null(conn@info$encryption_option)){
    EncryptionConfiguration = list("EncryptionOption" = conn@info$encryption_option)
    EncryptionConfiguration["KmsKey"] = conn@info$kms_key
    ResultConfiguration["EncryptionConfiguration"] <- list(EncryptionConfiguration)
  }
  config["ResultConfiguration"] <- list(ResultConfiguration)
  config["EnforceWorkGroupConfiguration"] <- EnforceWorkGroupConfiguration
  config["PublishCloudWatchMetricsEnabled"] <- PublishCloudWatchMetricsEnabled
  config["BytesScannedCutoffPerQuery"] <- BytesScannedCutoffPerQuery
  config
}

# set up work group configuration update
work_group_config_update <- 
  function(conn,
           RemoveOutputLocation = FALSE,
           EnforceWorkGroupConfiguration = FALSE,
           PublishCloudWatchMetricsEnabled = FALSE,
           BytesScannedCutoffPerQuery = 10000000L){
    
    ConfigurationUpdates <- list()
    ResultConfigurationUpdates <- list(OutputLocation = conn@info$s3_staging,
                                       RemoveOutputLocation = RemoveOutputLocation)
    if(!is.null(conn@info$encryption_option)){
      EncryptionConfiguration = list("EncryptionOption" = conn@info$encryption_option)
      EncryptionConfiguration["KmsKey"] = conn@info$kms_key
      ResultConfigurationUpdates["EncryptionConfiguration"] <- list(EncryptionConfiguration)
    }
    
    ConfigurationUpdates["EnforceWorkGroupConfiguration"] <- EnforceWorkGroupConfiguration
    ConfigurationUpdates["ResultConfigurationUpdates"] <- list(ResultConfigurationUpdates)
    ConfigurationUpdates["PublishCloudWatchMetricsEnabled"] <- PublishCloudWatchMetricsEnabled
    ConfigurationUpdates["BytesScannedCutoffPerQuery"] <- BytesScannedCutoffPerQuery
    
    ConfigurationUpdates
  }

# Set aws environmental variable
set_aws_env <- function(x){
  creds <- x$Credentials
  Sys.setenv("AWS_ACCESS_KEY_ID" = creds$AccessKeyId)
  Sys.setenv("AWS_SECRET_ACCESS_KEY" = creds$SecretAccessKey)
  Sys.setenv("AWS_SESSION_TOKEN" = creds$SessionToken)
  Sys.setenv("AWS_EXPIRATION" = creds$Expiration)
}

# Return NULL if System environment variable doesnt exist
get_aws_env <- function(x) {
  x <- Sys.getenv(x)
  if(nchar(x) == 0) return(NULL) else return(x)}

`%||%` <- function(x, y) if (is.null(x)) return(y) else return(x)

# time check warning when connection will expire soon
time_check <- function(x){ 
  t <- Sys.time()
  attr(t, "tzone") <- attr(x,"tzone") # make system time on the same time zone as region
  x <- as.numeric(x - t, units = "secs") 
  m <- x %/% 60
  s <- round(x %% 60, 0)
  if(m < 15) 
    warning("Athena Connection will expire in " , time_format(m), ":", time_format(s) , " (mm:ss)", call. = F)
}

time_format <- function(x) if(x < 10) paste0(0,x) else x

# get parent pkg function and method
pkg_method <- function(fun, pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(fun,' requires the ', pkg,' package, please install it first and try again',
         call. = F)}
  fun_name <- utils::getFromNamespace(fun, pkg)
  return(fun_name)
}

# set credentials
cred_set <- function(aws_access_key_id,
                     aws_secret_access_key,
                     aws_session_token,
                     profile_name,
                     region_name){
  add_list <-function(x) if(length(x) == 0) NULL else x
  config <- list()
  credentials <- list()
  cred <- list()
  
  cred$access_key_id = aws_access_key_id
  cred$secret_access_key = aws_secret_access_key
  cred$session_token = aws_session_token
  
  credentials$creds <- add_list(cred)
  credentials$profile <- profile_name
  config$credentials <- add_list(credentials)
  config$region <- region_name
  
  config
}

# Format DataScannedInBytes to a more readable format: 
data_scanned <- 
  function (x) {
    base <- 1024
    units_map <- c("B", "KB", "MB", "GB", "TB", "PB")
    power <- if (x <= 0) 0L else min(as.integer(log(x, base = base)), length(units_map) - 1L)
    unit <- units_map[power + 1L]
    if (power == 0) unit <- "Bytes"
    paste(round(x/base^power, digits = 2), unit)
  }

# Write large raw connections in chunks
write_bin <- function(
  value,
  filename,
  chunk_size = 2L ^ 20L) {
  
  # if readr is avialable then use readr::write_file else loop writeBin
  if (requireNamespace("readr", quietly = TRUE)) {
    write_file <- pkg_method("write_file", "readr")
    write_file(value, filename)
    return(invisible(TRUE))}
  
  total_size <- length(value)
  split_vec <- seq(1, total_size, chunk_size)
  
  con <- file(filename, "a+b")
  on.exit(close(con))
  
  if (length(split_vec) == 1) writeBin(value,con) 
  else sapply(split_vec, function(x){writeBin(value[x:min(total_size,(x+chunk_size-1))],con)})
  invisible(TRUE)
}

# caching function to added metadata to cache data.table
cache_query = function(poll_result){
  cache_append = data.table("QueryId" = poll_result$QueryExecution$QueryExecutionId,
                            "Query" = poll_result$QueryExecution$Query,
                            "State"= poll_result$QueryExecution$Status$State,
                            "StatementType"= poll_result$QueryExecution$StatementType,
                            "WorkGroup" = poll_result$QueryExecution$WorkGroup)
  new_query = fsetdiff(cache_append, athena_option_env$cache_dt, all = TRUE)
  
  # As Athena doesn't scanned data with Failed queries. Failed queries wont be cached: https://aws.amazon.com/athena/pricing/
  if(nrow(new_query[get("State") != "FAILED"]) > 0) athena_option_env$cache_dt = head(rbind(cache_append, athena_option_env$cache_dt), athena_option_env$cache_size)
}

# check cached query ids
check_cache = function(query, work_group){
  query_id = athena_option_env$cache_dt[get("Query") == query & get("State") == "SUCCEEDED" & get("StatementType") == "DML" & get("WorkGroup") == work_group, get("QueryId")]
  if(length(query_id) == 0) return(NULL) else return(query_id[1])
}

# If api call fails retry call
retry_api_call <- function(expr){
  
  # if number of retries is equal to 0 then retry is skipped
  if (athena_option_env$retry == 0) {
    resp <- tryCatch(eval.parent(substitute(expr)), 
                     error = function(e) e)
  }
  
  for (i in seq_len(athena_option_env$retry)) {
    resp <- tryCatch(eval.parent(substitute(expr)), 
                     error = function(e) e)
    
    if(inherits(resp, "error")){
      
      # stop retry if statement is an invalid request
      if (grepl("InvalidRequestException", resp)) {stop(resp)}
      
      backoff_len <- runif(n=1, min=0, max=(2^i - 1))
      
      if(!athena_option_env$retry_quiet) message(resp, "Request failed. Retrying in ", round(backoff_len, 1), " seconds...")
      
      Sys.sleep(backoff_len)
    } else {break}
  }
  
  if (inherits(resp, "error")) stop(resp)
  
  resp
}


# Create table With parameters
ctas_sql_with <- function(partition = NULL, s3.location = NULL, file.type = "NULL", compress = TRUE){
  if(file.type!="NULL" || !is.null(s3.location) || !is.null(partition)){
    FILE <- switch(file.type,
                   "csv" = "format = 'TEXTFILE',\nfield_delimiter = ','",
                   "tsv" = "format = 'TEXTFILE',\nfield_delimiter = '\t'",
                   "parquet" = "format = 'PARQUET'",
                   "json" = "format = 'JSON'",
                   "orc" = "format = 'ORC'",
                   "")
    
    COMPRESSION <- ""
    if (compress) {
      if(file.type %in% c("tsv", "csv", "json")) warning("Can only compress parquet or orc files: https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html", call. = FALSE)
      COMPRESSION <- switch(file.type,
                            "parquet" = ",\nparquet_compression = 'SNAPPY'",
                            "orc" = ",\norc_compression = 'SNAPPY'",
                            "")
    }
    
    LOCATION <- if(!is.null(s3.location)){
      if(file.type == "NULL") paste0("external_location ='", s3.location, "'")
      else paste0(",\nexternal_location ='", s3.location, "'")
    } else ""
    
    PARTITION <- if(!is.null(partition)){
      partition <- paste(partition, collapse = "','")
      if(is.null(s3.location) && file.type == "NULL") paste0("partitioned_by = ARRAY['",partition,"']")
      else paste0(",\npartitioned_by = ARRAY['",partition,"']")
    } else ""
    
    paste0("WITH (", FILE, COMPRESSION, LOCATION, PARTITION,")\n")
  } else ""
}
