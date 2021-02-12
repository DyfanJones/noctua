#' @include Connection.R
NULL

AthenaResult <- function(conn,
                         statement = NULL,
                         s3_staging_dir = NULL){
  
  stopifnot(is.character(statement))
  
  response <- new.env(parent = emptyenv())
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
  function(res, ...){
    if (!dbIsValid(res)) {
      warning("Result already cleared", call. = FALSE)
    } else {

      # stops resource if query is still running
      retry_api_call(res@connection@ptr$Athena$stop_query_execution(
        QueryExecutionId = res@info[["QueryExecutionId"]]))
      
      if(is.null(res@info[["Status"]])) {
        # checks status of query
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
        
        # Out put error as warning if s3 resource can't be dropped
        tryCatch(res@connection@ptr$S3$delete_object(
          Bucket = result_info$bucket, Key = paste0(result_info$key, ".metadata")),
                 error = function(e) warning(e, call. = F))
        tryCatch(res@connection@ptr$S3$delete_object(Bucket = result_info$bucket,
                                                     Key = result_info$key),
                 error = function(e) cat(""))
        # remove manifest csv created with CTAS statements 
        if (res@info[["StatementType"]] == "DDL")
          tryCatch({
            res@connection@ptr$S3$delete_object(
              Bucket = result_info[["bucket"]],
              Key = paste0(result_info[["key"]], "-manifest.csv"))},
            error = function(e) cat(""))
        }
      
      # remove query information
      rm(list = ls(all.names = TRUE, envir = res@info), envir = res@info)
    }
    invisible(TRUE)
})

#' Fetch records from previously executed query
#' 
#' Currently returns the top n elements (rows) from result set or returns entire table from Athena.
#' @name dbFetch
#' @param n maximum number of records to retrieve per fetch. Use \code{n = -1} or \code{n = Inf} to retrieve all pending records.
#'          Some implementations may recognize other special values. If entire dataframe is required use \code{n = -1} or \code{n = Inf}.
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
  function(res, n = -1, ...){
    con_error_msg(res, msg = "Result already cleared.")
    
    # check status of query, skip poll if status found
    if(is.null(res@info[["Status"]]))
      poll(res)
    
    # if query failed stop
    if(res@info[["Status"]] == "FAILED")
      stop(res@info[["StateChangeReason"]], call. = FALSE)
    
    # cache query metadata if caching is enabled
    if (athena_option_env[["cache_size"]] > 0)
      cache_query(res)
    
    result_info <- split_s3_uri(res@info[["OutputLocation"]])
    
    # return metadata of athena data types
    retry_api_call(result_class <- res@connection@ptr$Athena$get_query_results(
      QueryExecutionId = res@info[["QueryExecutionId"]],
      MaxResults = as.integer(1))[["ResultSet"]][["ResultSetMetadata"]][["ColumnInfo"]])
    
    if(n >= 0 && n !=Inf){
      # assign token from AthenaResult class
      token <- res@info[["NextToken"]]
      
      if(length(token) == 0) n <- as.integer(n + 1)
      chunk <- as.integer(n)
      if (n > 1000L) chunk = 1000L
      
      iterate <- 1:ceiling(n/chunk)
      
      # create empty list shell
      dt_list <- list()
      length(dt_list) <- max(iterate)
      
      for (i in iterate){
        if(i == max(iterate)) chunk <- as.integer(n - (i-1) * chunk)
        
        # get chunk with retry api call if call fails
        retry_api_call(result <- res@connection@ptr$Athena$get_query_results(
          QueryExecutionId = res@info[["QueryExecutionId"]],
          NextToken = token,
          MaxResults = chunk))
        
        # process returned list
        output <- lapply(
          result[["ResultSet"]][["Rows"]], 
          function(x) (sapply(x$Data, function(x) if(length(x) == 0) NA else x)))
        suppressWarnings(staging_dt <- rbindlist(output, use.names = FALSE))
        
        # remove colnames from first row
        if (i == 1 && length(token) == 0) {
          staging_dt <- staging_dt[-1,]
        }
        
        # ensure rownames are not set
        rownames(staging_dt) <- NULL
        
        # added staging data.table to list
        dt_list[[i]] <- staging_dt
        
        # if token hasn't changed or if no more tokens are available then break loop
        if ((length(token) != 0
             && token == result[["NextToken"]])
             || length(result[["NextToken"]]) == 0) {
          break
        } else {
          token <- result[["NextToken"]]}
      }
      
      # combined all lists together
      dt <- rbindlist(dt_list, use.names = FALSE)
      
      # Update last token in s4 class
      res@info[["NextToken"]] <- result[["NextToken"]]
      
      # replace names with actual names
      Names <- sapply(result_class, function(x) x[["Name"]])
      colnames(dt) <- Names
      
      # convert data.table to tibble if using vroom as backend
      if(inherits(athena_option_env[["file_parser"]], "athena_vroom")) {
        as_tibble <- pkg_method("as_tibble", "tibble")
        dt <- as_tibble(dt)}
      
      return(dt)
    }
    
    # Added data scan information when returning data from athena
    message("Info: (Data scanned: ", data_scanned(
      res@info[["Statistics"]][["DataScannedInBytes"]]),")")
    
    #create temp file
    File <- tempfile()
    on.exit(unlink(File))
    
    # connect to s3 and create a bucket object
    # download athena output
    retry_api_call(obj <- res@connection@ptr$S3$get_object(
      Bucket = result_info[["bucket"]], Key = result_info[["key"]]))
    
    write_bin(obj$Body, File)
    
    if(grepl("\\.csv$", result_info[["key"]])){
      output <- athena_read(
        athena_option_env[["file_parser"]], File, result_class)
    } else {
      output <- athena_read_lines(
        athena_option_env[["file_parser"]], File, result_class)
    }
    
    return(output)
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
    
    # if status has already return then output TRUE
    if(!is.null(res@info[["Status"]]))
      return(TRUE)
    
    # get status of query
    retry_api_call(query_execution <- res@connection@ptr$Athena$get_query_execution(
      QueryExecutionId = res@info[["QueryExecutionId"]]))
    
    if(query_execution[["QueryExecution"]][["Status"]][["State"]] %in% c("SUCCEEDED", "FAILED", "CANCELLED"))
      return(TRUE)
    else if (query_executionp[["QueryExecution"]][["Status"]][["State"]] == "RUNNING")
      return(FALSE)
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
