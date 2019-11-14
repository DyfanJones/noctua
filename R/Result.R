#' @include Connection.R
NULL

AthenaResult <- function(conn,
                         statement = NULL,
                         s3_staging_dir = NULL){
  
  stopifnot(is.character(statement))

  tryCatch(response <- conn@ptr$Athena$start_query_execution(QueryString = statement,
                                                             QueryExecutionContext = list(Database = conn@info$dbms.name),
                                                             ResultConfiguration = ResultConfiguration(conn),
                                                             WorkGroup = conn@info$work_group))
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
    info = "list"
  )
)

#' Clear Results
#' 
#' Frees all resources (local and Athena) associated with result set. It does this by removing query output in AWS S3 Bucket,
#' stopping query execution if still running and removed the connection resource locally.
#' @name dbClearResult
#' @inheritParams DBI::dbClearResult
#' @return \code{dbClearResult()} returns \code{TRUE}, invisibly.
#' @seealso \code{\link[DBI]{dbIsValid}}
#' @examples
#' \donttest{
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
      
      # checks status of query
      tryCatch(query_execution <- res@connection@ptr$Athena$get_query_execution(QueryExecutionId = res@info$QueryExecutionId))
      
      # stops resource if query is still running
      if (!(query_execution$QueryExecution$Status$State %in% c("SUCCEEDED", "FAILED", "CANCELLED"))){
        tryCatch(res@connection@ptr$Athena$stop_query_execution(QueryExecutionId = res@info$QueryExecutionId))}
      
      # clear s3 athena output
      # split s3_uri
      result_info <- split_s3_uri(query_execution$QueryExecution$ResultConfiguration$OutputLocation)
      
      # remove class pointers
      eval.parent(substitute(res@connection@ptr <- list()))
      
      # Out put Python error as warning if s3 resource can't be dropped
      tryCatch(res@connection@ptr$S3$delete_object(Bucket = result_info$bucket,
                                                   Key = paste0(result_info$key, ".metadata")),
               error = function(e) warning(e, call. = F))
      tryCatch(res@connection@ptr$S3$delete_object(Bucket = result_info$bucket,
                                                   Key = result_info$key),
               error = function(e) cat(""))
    }
    invisible(TRUE)
  })


#' Fetch records from previously executed query
#' 
#' Currently returns the top n elements (rows) from result set or returns entire table from Athena.
#' @name dbFetch
#' @param n maximum number of records to retrieve per fetch. Use \code{n = -1} or \code{n = Inf} to retrieve all pending records.
#'          Some implementations may recognize other special values. Currently chunk sizes range from 0 to 999, 
#'          if entire dataframe is required use \code{n = -1} or \code{n = Inf}.
#' @inheritParams DBI::dbFetch
#' @return \code{dbFetch()} returns a data frame.
#' @seealso \code{\link[DBI]{dbFetch}}
#' @examples
#' \donttest{
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
    if (!dbIsValid(res)) {stop("Result already cleared", call. = FALSE)}
    # check status of query
    result <- poll(res)
    
    result_info <- split_s3_uri(result$QueryExecution$ResultConfiguration$OutputLocation)
    
    # if query failed stop
    if(result$QueryExecution$Status$State == "FAILED") {
      stop(result$QueryExecution$Status$StateChangeReason, call. = FALSE)
    }
    
    if(n >= 0 && n !=Inf){
      n = as.integer(n + 1)
      tryCatch(result <- res@connection@ptr$Athena$get_query_results(QueryExecutionId = res@info$QueryExecutionId, MaxResults = n))
      
      output <- lapply(result$ResultSet$Rows, function(x) (sapply(x$Data, function(x) if(length(x) == 0 ) NA else x)))
      df <- t(sapply(output, function(x) unlist(x)))
      colnames(df) <- tolower(unname(df[1,]))
      df <- data.frame(df)[-1,]
      rownames(df) <- NULL
      return(df)
    }
    
    #create temp file
    File <- tempfile()
    on.exit(unlink(File))
    
    # connect to s3 and create a bucket object
    # download athena output
    tryCatch(obj <- res@connection@ptr$S3$get_object(Bucket = result_info$bucket, Key = result_info$key))
    
    writeBin(obj$Body, con = File)
    
    # return metadata of athena data types
    tryCatch(result_class <- res@connection@ptr$Athena$get_query_results(QueryExecutionId = res@info$QueryExecutionId,
                                                                         MaxResults = as.integer(1)))
    
    Type <- AthenaToRDataType(result_class$ResultSet$ResultSetMetadata$ColumnInfo)
    
    if(grepl("\\.csv$",result_info$key)){
      # currently parameter data.table is left as default. If users require data.frame to be returned then parameter will be updated
      output <- data.table::fread(File, col.names = names(Type), colClasses = unname(Type), showProgress = F)
    } else{
      file_con <- file(File)
      output <- suppressWarnings(readLines(file_con))
      close(file_con)
      if(any(grepl("create|table", output, ignore.case = T))){
        output <-data.frame("TABLE_DDL" = paste0(output, collapse = "\n"), stringsAsFactors = FALSE)
      } else (output <- data.frame(var1 = trimws(output), stringsAsFactors = FALSE))
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
#' \donttest{
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
    if (!dbIsValid(res)) {stop("Result already cleared", call. = FALSE)}
    tryCatch(query_execution <- res@connection@ptr$Athena$get_query_execution(QueryExecutionId = res@info$QueryExecutionId))
    
    if(query_execution$QueryExecution$Status$State %in% c("SUCCEEDED", "FAILED", "CANCELLED")) TRUE
    else if (query_execution$QueryExecution$Status$State == "RUNNING") FALSE
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
    if (!dbIsValid(dbObj)) {stop("Result already cleared", call. = FALSE)}
    info <- dbObj@info
    info
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
#' \donttest{
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
    if (!dbIsValid(res)) {stop("Result already cleared", call. = FALSE)}
    result <- poll(res)
    if(result$QueryExecution$Status$State == "FAILED") {
      stop(result$QueryExecution$Status$StateChangeReason, call. = FALSE)
    }
    
    tryCatch(result <- res@connection@ptr$Athena$get_query_results(QueryExecutionId = res@info$QueryExecutionId,
                                                                   MaxResults = as.integer(1)))
    
    Name <- sapply(result$ResultSet$ResultSetMetadata$ColumnInfo, function(x) x$Name)
    Type <- sapply(result$ResultSet$ResultSetMetadata$ColumnInfo, function(x) x$Type)
    data.frame(field_name = Name,
               type = Type, stringsAsFactors = F)
  }
)
