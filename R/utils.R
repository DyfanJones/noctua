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

# convert character to logical
char_log <- function(value, athena_class){
  col_want <- sapply(athena_class, function(x) x == "logical")
  for (i in seq_along(which(col_want))) {
    want = which(value[[which(col_want)[i]]] == "true")
    output = rep(FALSE, times = nrow(value))
    output[want] = TRUE
    value[[which(col_want)[i]]] <- output
  }
  value
}

# simple wrapper to correct miss classification
read_athena <- function(file, athena_class){
  output <- read.csv(file, col.names = names(athena_class), stringsAsFactors = F) 
  char_log(output, athena_class)
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