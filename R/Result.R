#' @include Connection.R
#' @include utils.R
#' @include fetch_utils.R
NULL

AthenaResult <- function(conn,
                         statement = NULL,
                         s3_staging_dir = NULL){
  
  stopifnot(is.character(statement))
  
  response <- new.env(parent = emptyenv())
  response[["Query"]] <- statement
  if (athena_option_env$cache_size > 0)
    response[["QueryExecutionId"]] <- check_cache(statement, conn@info$work_group)
  if (is.null(response[["QueryExecutionId"]])) {
    retry_api_call(
      response[["QueryExecutionId"]] <- conn@ptr$Athena$start_query_execution(
        ClientRequestToken = uuid::UUIDgenerate(),
        QueryString = statement,
        QueryExecutionContext = list(Database = conn@info$dbms.name),
        ResultConfiguration = ResultConfiguration(conn),
        WorkGroup = conn@info$work_group)$QueryExecutionId)}
  on.exit(if(!is.null(conn@info$expiration)) time_check(conn@info$expiration))
  new("AthenaResult", connection = conn, info = response)
}

#' @rdname AthenaConnection
#' @export
setClass(
  "AthenaResult",
  contains = "DBIResult",
  slots = list(
    connection = "AthenaConnection",
    info = "environment"
  )
)

#' Clear Results
#' 
#' Frees all resources (local and Athena) associated with result set. It does this by removing query output in AWS S3 Bucket,
#' stopping query execution if still running and removed the connection resource locally.
#' 
#' @note If a user does not have permission to remove AWS S3 resource from AWS Athena output location, then an AWS warning will be returned.
#'       It is better use query caching \code{\link{noctua_options}} so that the warning doesn't repeatedly show.
#' @name dbClearResult
#' @inheritParams DBI::dbClearResult
#' @param unload boolean to remove extra AWS S3 file objects \href{https://docs.aws.amazon.com/athena/latest/ug/unload.html}{AWS Athena UNLOAD}
#'          creates, default is set to \code{FALSE}.
#' @return \code{dbClearResult()} returns \code{TRUE}, invisibly.
#' @seealso \code{\link[DBI]{dbIsValid}}
#' @examples
#' \dontrun{
#' # Note: 
#' # - Require AWS Account to run below example.
#' # - Different connection methods can be used please see `noctua::dbConnect` documnentation
#' 
#' library(DBI)
#' 
#' # Demo connection to Athena using profile name 
#' con <- dbConnect(noctua::athena())
#' 
#' res <- dbSendQuery(con, "show databases")
#' dbClearResult(res)
#' 
#' # Check if connection if valid after closing connection
#' dbDisconnect(con)
#' }
#' @docType methods
NULL

#' @rdname dbClearResult
#' @export
setMethod(
  "dbClearResult", "AthenaResult",
  function(res, unload = FALSE, ...){
    stopifnot(is.logical(unload))
    if (!dbIsValid(res)) {
      warning("Result already cleared", call. = FALSE)
    } else {
      # stops resource if query is still running
      retry_api_call(res@connection@ptr$Athena$stop_query_execution(
        QueryExecutionId = res@info[["QueryExecutionId"]]))
      
      # checks status of query
      if(is.null(res@info[["Status"]])) {
        retry_api_call(query_execution <- res@connection@ptr$Athena$get_query_execution(
          QueryExecutionId = res@info[["QueryExecutionId"]]))
        res@info[["OutputLocation"]] <- 
          query_execution[["QueryExecution"]][["ResultConfiguration"]][["OutputLocation"]]
        res@info[["StatementType"]] <- 
          query_execution[["QueryExecution"]][["StatementType"]]
      }
      
      # for caching s3 data is still required
      if (athena_option_env[["cache_size"]] == 0){
        result_info <- split_s3_uri(res@info[["OutputLocation"]])
        
        # Output error as warning if S3 resource can't be dropped
        tryCatch(
          res@connection@ptr$S3$delete_object(
            Bucket = result_info$bucket, Key = paste0(result_info$key, ".metadata")),
          error = function(e) warning(e, call. = F)
        )
        
        # remove manifest csv created with CTAS statements 
        tryCatch({
          res@connection@ptr$S3$delete_object(
            Bucket = result_info[["bucket"]],
            Key = paste0(result_info[["key"]], "-manifest.csv"))},
          error = function(e) NULL)
        
        # remove AWS Athena results      
        if(!unload){
          
          tryCatch(
            res@connection@ptr$S3$delete_object(
              Bucket = result_info$bucket,
              Key = result_info$key),
            error = function(e) NULL)
          
        } else {
          # Check S3 Prefix for AWS Athena results
          result_info <- split_s3_uri(res@connection@info[["s3_staging"]])
          result_info$key <- file.path(gsub("/$", "", result_info$key), res@info$unload_dir)
          all_keys <- list()
          token <- NULL
          # Get all s3 objects linked to table
          while(is.null(token) || length(token) != 0) {
            objects <- res@connection@ptr$S3$list_objects_v2(Bucket=result_info$bucket, Prefix=result_info$key, ContinuationToken = token)
            token <- objects$NextContinuationToken
            all_keys <- c(all_keys, lapply(objects$Contents, function(x) list(Key=x$Key)))
          }
          
          # Only remove if files are found
          if(length(all_keys) > 0){
            # Delete S3 files in batch size 1000
            key_parts <- split_vec(all_keys, 1000)
            for(i in seq_along(key_parts)){
              tryCatch(
                res@connection@ptr$S3$delete_objects(
                  Bucket = result_info$bucket,
                  Delete = list(Objects = key_parts[[i]])),
                error = function(e) NULL)
            }
          }
        }
      }
      # remove query information
      rm(list = ls(all.names = TRUE, envir = res@info), envir = res@info)
    }
    return(invisible(TRUE))
})

#' Fetch records from previously executed query
#' 
#' Currently returns the top n elements (rows) from result set or returns entire table from Athena.
#' @name dbFetch
#' @param n maximum number of records to retrieve per fetch. Use \code{n = -1} or \code{n = Inf} to retrieve all pending records.
#'          Some implementations may recognize other special values. If entire dataframe is required use \code{n = -1} or \code{n = Inf}.
#' @param unload boolean uses `arrow::read_parquet` to return \href{https://docs.aws.amazon.com/athena/latest/ug/unload.html}{AWS Athena UNLOAD}
#'          queries back to `R`, default is set to \code{FALSE}.
#' @inheritParams DBI::dbFetch
#' @return \code{dbFetch()} returns a data frame.
#' @seealso \code{\link[DBI]{dbFetch}}
#' @examples
#' \dontrun{
#' # Note: 
#' # - Require AWS Account to run below example.
#' # - Different connection methods can be used please see `noctua::dbConnect` documnentation
#' 
#' library(DBI)
#' 
#' # Demo connection to Athena using profile name 
#' con <- dbConnect(noctua::athena())
#' 
#' res <- dbSendQuery(con, "show databases")
#' dbFetch(res)
#' dbClearResult(res)
#' 
#' # Disconnect from Athena
#' dbDisconnect(con)
#' }
#' @docType methods
NULL

#' @rdname dbFetch
#' @export
setMethod(
  "dbFetch", "AthenaResult",
  function(res, n = -1, unload = FALSE, ...){
    con_error_msg(res, msg = "Result already cleared.")
    stopifnot(is.logical(unload))
    
    # check status of query, skip poll if status found
    if(is.null(res@info[["Status"]]))
      poll(res)
    
    # if query failed stop
    if(res@info[["Status"]] == "FAILED")
      stop(res@info[["StateChangeReason"]], call. = FALSE)
    
    # cache query metadata if caching is enabled
    if (athena_option_env[["cache_size"]] > 0)
      cache_query(res)
    
    # return metadata of athena data types
    retry_api_call(result_class <- res@connection@ptr$Athena$get_query_results(
      QueryExecutionId = res@info[["QueryExecutionId"]],
      MaxResults = as.integer(1))[["ResultSet"]][["ResultSetMetadata"]][["ColumnInfo"]])
    
    if(n >= 0 && n !=Inf){
      return(.fetch_n(res, result_class, n))
    }
    
    # Added data scan information when returning data from athena
    message("Info: (Data scanned: ", data_scanned(
      res@info[["Statistics"]][["DataScannedInBytes"]]),")")
    
    if (unload){
      .fetch_unload(res)
    } else {
      .fetch_file(res, result_class)
    }
})
  
#' Completion status
#' 
#' This method returns if the query has completed. 
#' @name dbHasCompleted
#' @inheritParams DBI::dbHasCompleted
#' @return \code{dbHasCompleted()} returns a logical scalar. \code{TRUE} if the query has completed, \code{FALSE} otherwise.
#' @seealso \code{\link[DBI]{dbHasCompleted}}
#' @examples
#' \dontrun{
#' # Note: 
#' # - Require AWS Account to run below example.
#' # - Different connection methods can be used please see `noctua::dbConnect` documnentation
#' 
#' library(DBI)
#' 
#' # Demo connection to Athena using profile name 
#' con <- dbConnect(noctua::athena())
#' 
#' # Check if query has completed
#' res <- dbSendQuery(con, "show databases")
#' dbHasCompleted(res)
#'
#' dbClearResult(res)
#' 
#' # Disconnect from Athena
#' dbDisconnect(con)
#' }
#' @docType methods
NULL

#' @rdname dbHasCompleted
#' @export
setMethod(
  "dbHasCompleted", "AthenaResult",
  function(res, ...) {
    con_error_msg(res, msg = "Result already cleared.")
    output <- TRUE
    # if status has already return then output TRUE
    if(!is.null(res@info[["Status"]]))
      return(output)
    
    # get status of query
    retry_api_call(query_execution <- res@connection@ptr$Athena$get_query_execution(
      QueryExecutionId = res@info[["QueryExecutionId"]]))
    
    if (query_execution[["QueryExecution"]][["Status"]][["State"]] %in% c("RUNNING", "QUEUED"))
      output <- FALSE
    
    return(output)
  })

#' @rdname dbIsValid
#' @export
setMethod(
  "dbIsValid", "AthenaResult",
  function(dbObj, ...){
    resource_active(dbObj)
  }
)

#' @rdname dbGetInfo
#' @inheritParams DBI::dbGetInfo
#' @export
setMethod(
  "dbGetInfo", "AthenaResult",
  function(dbObj, ...) {
    con_error_msg(dbObj, msg = "Result already cleared.")
    info <- as.list(dbObj@info)
    return(info)
  })

#' Information about result types
#' 
#' Produces a data.frame that describes the output of a query. 
#' @name dbColumnInfo
#' @inheritParams DBI::dbColumnInfo
#' @return \code{dbColumnInfo()} returns a data.frame with as many rows as there are output fields in the result.
#'         The data.frame has two columns (field_name, type).
#' @seealso \code{\link[DBI]{dbHasCompleted}}
#' @examples
#' \dontrun{
#' # Note: 
#' # - Require AWS Account to run below example.
#' # - Different connection methods can be used please see `RAthena::dbConnect` documnentation
#' 
#' library(DBI)
#' 
#' # Demo connection to Athena using profile name 
#' con <- dbConnect(noctua::athena())
#' 
#' # Get Column information from query
#' res <- dbSendQuery(con, "select * from information_schema.tables")
#' dbColumnInfo(res)
#' dbClearResult(res)
#'  
#' # Disconnect from Athena
#' dbDisconnect(con)
#' }
#' @docType methods
NULL

#' @rdname dbColumnInfo
#'@export
setMethod(
  "dbColumnInfo", "AthenaResult",
  function(res, ...){
    con_error_msg(res, msg = "Result already cleared.")
    
    # check status of query, skip poll if status found
    if(is.null(res@info[["Status"]]))
      poll(res)
    
    # if query failed stop
    if(res@info[["Status"]] == "FAILED")
      stop(res@info[["StateChangeReason"]], call. = FALSE)
    
    retry_api_call(result <- res@connection@ptr$Athena$get_query_results(
      QueryExecutionId = res@info[["QueryExecutionId"]],
      MaxResults = as.integer(1)))
    
    Name <- vapply(result[["ResultSet"]][["ResultSetMetadata"]][["ColumnInfo"]],
                   function(x) x$Name, FUN.VALUE = character(1))
    Type <- vapply(result[["ResultSet"]][["ResultSetMetadata"]][["ColumnInfo"]], 
                   function(x) x$Type, FUN.VALUE = character(1))
    data.frame(field_name = Name,
               type = Type, stringsAsFactors = F)
  }
)

#' Show AWS Athena Statistics
#' 
#' @description Returns AWS Athena Statistics from execute queries \code{\link{dbSendQuery}}
#' @inheritParams DBI::dbColumnInfo
#' @name dbStatistics
#' @return \code{dbStatistics()} returns list containing Athena Statistics return from \code{paws}.
#' @examples 
#' \dontrun{
#' # Note: 
#' # - Require AWS Account to run below example.
#' # - Different connection methods can be used please see `RAthena::dbConnect` documnentation
#' 
#' library(DBI)
#' library(noctua)
#' 
#' # Demo connection to Athena using profile name 
#' con <- dbConnect(noctua::athena())
#' 
#' res <- dbSendQuery(con, "show databases")
#' dbStatistics(res)
#' 
#' # Clean up
#' dbClearResult(res)
#' 
#' }
#' @docType methods
NULL

#' @rdname dbStatistics
#' @export
setGeneric("dbStatistics",
           def = function(res, ...) standardGeneric("dbStatistics"))

#' @rdname dbStatistics
#'@export
setMethod(
  "dbStatistics", "AthenaResult",
  function(res, ...){
    con_error_msg(res, msg = "Result already cleared.")
    
    # check status of query, skip poll if status found
    if(is.null(res@info[["Status"]]))
      poll(res)
    
    # if query failed stop
    if(res@info[["Status"]] == "FAILED")
      stop(res@info[["StateChangeReason"]], call. = FALSE)

    return(res@info[["Statistics"]])
})


#' Get the statement associated with a result set
#'
#' Returns the statement that was passed to [dbSendQuery()]
#' or [dbSendStatement()].
#' @name dbGetStatement
#' @inheritParams DBI::dbGetStatement
#' @return \code{dbGetStatement()} returns a character.
#' @seealso \code{\link[DBI]{dbGetStatement}}
#' @examples
#' \dontrun{
#' # Note: 
#' # - Require AWS Account to run below example.
#' # - Different connection methods can be used please see `noctua::dbConnect` documnentation
#' 
#' library(DBI)
#' 
#' # Demo connection to Athena using profile name 
#' con <- dbConnect(noctua::athena())
#'
#' rs <- dbSendQuery(con, "SHOW TABLES in default")
#' dbGetStatement(rs)
#' }
#' @docType methods
NULL

#' @rdname dbGetStatement
#' @export
setMethod(
  "dbGetStatement", "AthenaResult",
  function(res, ...){
    con_error_msg(res, msg = "Result already cleared.")
    return(res@info[["Query"]])
})
