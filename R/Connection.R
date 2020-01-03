#' @include Driver.R
NULL

#' Athena Connection Methods
#'
#' Implementations of pure virtual functions defined in the `DBI` package
#' for AthenaConnection objects.
#' @slot ptr a list of connecting objects from the SDK paws package.
#' @slot info a list of metadata objects
#' @slot connection contains the \code{AthenaConnection} class object
#' @slot quote syntax to quote sql query when creating athena ddl
#' @name AthenaConnection
#' @keywords internal
#' @inheritParams methods::show
NULL

class_cache <- new.env(parent = emptyenv())

AthenaConnection <-
  function(
    aws_access_key_id = NULL,
    aws_secret_access_key = NULL ,
    aws_session_token = NULL,
    schema_name = NULL,
    work_group = NULL,
    poll_interval = NULL,
    encryption_option = NULL,
    kms_key = NULL,
    s3_staging_dir = NULL,
    region_name = NULL,
    profile_name = NULL, 
    aws_expiration = NULL,...){
    
    # get lower level paws methods
    get_region <- pkg_method("get_region", "paws.common")
    get_profile_name <- pkg_method("get_profile_name", "paws.common")
    
    # get profile_name
    prof_name <- if(!(is.null(aws_access_key_id) || is.null(aws_secret_access_key) || is.null(aws_session_token))) NULL else get_profile_name(profile_name)
    config <- cred_set(aws_access_key_id, aws_secret_access_key, aws_session_token, prof_name, region_name %||% get_region(profile_name))
    
    tryCatch({Athena <- paws::athena(config)
              S3 <- paws::s3(config)
              glue <- paws::glue(config)})
    
    if(is.null(s3_staging_dir) && !is.null(work_group)){
      tryCatch(s3_staging_dir <- Athena$get_work_group(WorkGroup = work_group)$WorkGroup$Configuration$ResultConfiguration$OutputLocation)
    }
    
    # return a subset of api function to reduce object size
    ptr <-list(Athena = Athena[c("start_query_execution", "get_query_execution","stop_query_execution", "get_query_results",
                                 "get_work_group","list_work_groups","update_work_group","create_work_group","delete_work_group")],
                S3 = S3[c("put_object", "get_object","delete_object","delete_objects","list_objects")],
                glue = glue[c("get_databases","get_tables","get_table")])
    
    s3_staging_dir <- s3_staging_dir %||% get_aws_env("AWS_ATHENA_S3_STAGING_DIR")
    
    if(is.null(s3_staging_dir)) {stop("Please set `s3_staging_dir` either in parameter `s3_staging_dir`, environmental varaible `AWS_ATHENA_S3_STAGING_DIR`",
                                      "or when work_group is defined in `create_work_group()`", call. = F)}
    
    info <- list(profile_name = prof_name, s3_staging = s3_staging_dir,
                 dbms.name = schema_name, work_group = work_group,
                 poll_interval = poll_interval, encryption_option = encryption_option,
                 kms_key = kms_key, expiration = aws_expiration)
    
    res <- new("AthenaConnection",  ptr = ptr, info = info, quote = "`")
  }

#' @rdname AthenaConnection
#' @export
setClass(
  "AthenaConnection",
  contains = "DBIConnection",
  slots = list(
    ptr = "list",
    info = "list",
    quote = "character")
)

#' @rdname AthenaConnection
#' @export
setMethod(
  "show", "AthenaConnection",
  function(object){
    cat("AthenaConnection \n")
  }
)

#' Disconnect (close) an Athena connection
#' 
#' This closes the connection to Athena.
#' @name dbDisconnect
#' @inheritParams DBI::dbDisconnect
#' @return \code{dbDisconnect()} returns \code{TRUE}, invisibly.
#' @seealso \code{\link[DBI]{dbDisconnect}}
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
#' # Disconnect conenction
#' dbDisconnect(con)
#' }
#' @docType methods
NULL

#' @rdname dbDisconnect
#' @export
setMethod(
  "dbDisconnect", "AthenaConnection",
  function(conn, ...) {
    if (!dbIsValid(conn)) {
      warning("Connection already closed.", call. = FALSE)
    } else {
      on_connection_closed(conn)
      eval.parent(substitute(conn@ptr <- list()))}
    invisible(NULL)
  })

#' Is this DBMS object still valid?
#' 
#' This method tests whether the \code{dbObj} is still valid.
#' @name dbIsValid
#' @inheritParams DBI::dbIsValid
#' @return \code{dbIsValid()} returns logical scalar, \code{TRUE} if the object (\code{dbObj}) is valid, \code{FALSE} otherwise.
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
#' # Check is connection is valid
#' dbIsValid(con)
#' 
#' # Check is query is valid
#' res <- dbSendQuery(con, "show databases")
#' dbIsValid(res)
#' 
#' # Check if query is valid after clearing result
#' dbClearResult(res)
#' dbIsValid(res)
#' 
#' # Check if connection if valid after closing connection
#' dbDisconnect(con)
#' dbIsValid(con)
#' }
#' @docType methods
NULL

#' @rdname dbIsValid
#' @export
setMethod(
  "dbIsValid", "AthenaConnection",
  function(dbObj, ...){
    resource_active(dbObj)
  }
)


#' Execute a query on Athena
#' 
#' @description The \code{dbSendQuery()} and \code{dbSendStatement()} method submits a query to Athena but does not wait for query to execute. 
#'              \code{\link{dbHasCompleted}} method will need to ran to check if query has been completed or not.
#'              The \code{dbExecute()} method submits a query to Athena and waits for the query to be executed.
#' @name Query
#' @inheritParams DBI::dbSendQuery
#' @return Returns \code{AthenaResult} s4 class.
#' @seealso \code{\link[DBI]{dbSendQuery}}, \code{\link[DBI]{dbSendStatement}}, \code{\link[DBI]{dbExecute}}
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
#' # Sending Queries to Athena
#' res1 <- dbSendQuery(con, "show databases")
#' res2 <- dbSendStatement(con, "show databases")
#' res3 <- dbExecute(con, "show databases")
#' 
#' # Disconnect conenction
#' dbDisconnect(con)
#' }
#' @docType methods
NULL

#' @rdname Query
#' @export
setMethod(
  "dbSendQuery", c("AthenaConnection", "character"),
  function(conn,
           statement = NULL, ...){
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    s3_staging_dir <- conn@info$s3_staging
    res <- AthenaResult(conn=conn, statement= statement, s3_staging_dir = s3_staging_dir)
    res
  }
)

#' @rdname Query
#' @export
setMethod(
  "dbSendStatement", c("AthenaConnection", "character"),
  function(conn,
           statement = NULL, ...){
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    s3_staging_dir <- conn@info$s3_staging
    res <- AthenaResult(conn =conn, statement= statement, s3_staging_dir = s3_staging_dir)
    res
  }
)

#' @rdname Query
#' @export
setMethod(
  "dbExecute", c("AthenaConnection", "character"),
  function(conn,
           statement = NULL, ...){
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    s3_staging_dir <- conn@info$s3_staging
    res <- AthenaResult(conn =conn, statement= statement, s3_staging_dir = s3_staging_dir)
    poll(res)
    res
  }
)

#' Determine SQL data type of object
#' 
#' Returns a character string that describes the Athena SQL data type for the \code{obj} object.
#' @name dbDataType
#' @inheritParams DBI::dbDataType
#' @return \code{dbDataType} returns the Athena type that correspond to the obj argument as an non-empty character string.
#' @seealso \code{\link[DBI]{dbDataType}}
#' @examples
#' library(noctua)
#' dbDataType(athena(), 1:5)
#' dbDataType(athena(), 1)
#' dbDataType(athena(), TRUE)
#' dbDataType(athena(), Sys.Date())
#' dbDataType(athena(), Sys.time())
#' dbDataType(athena(), c("x", "abc"))
#' dbDataType(athena(), list(raw(10), raw(20)))
#' 
#' vapply(iris, function(x) dbDataType(noctua::athena(), x),
#'        FUN.VALUE = character(1), USE.NAMES = TRUE)
#' 
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
#' # Sending Queries to Athena
#' dbDataType(con, iris)
#' 
#' # Disconnect conenction
#' dbDisconnect(con)
#' }
#' @docType methods
NULL

#' @rdname dbDataType
#' @export
setMethod("dbDataType", "AthenaConnection", function(dbObj, obj, ...) {
  dbDataType(athena(), obj, ...)
})


#' @rdname dbDataType
#' @export
setMethod("dbDataType", c("AthenaConnection", "data.frame"), function(dbObj, obj, ...) {
  vapply(obj, AthenaDataType, FUN.VALUE = character(1), USE.NAMES = TRUE)
})


#' Quote Identifiers
#' 
#' Call this method to generate string that is suitable for use in a query as a column or table name.
#' @name dbQuote
#' @inheritParams DBI::dbQuoteString
#' @return Returns a character object, for more information please check out: \code{\link[DBI]{dbQuoteString}}, \code{\link[DBI]{dbQuoteIdentifier}}
#' @seealso \code{\link[DBI]{dbQuoteString}}, \code{\link[DBI]{dbQuoteIdentifier}}
#' @docType methods
NULL

#' @rdname dbQuote
#' @export
setMethod(
  "dbQuoteString", c("AthenaConnection", "character"),
  function(conn, x, ...) {
    # Optional
    getMethod("dbQuoteString", c("DBIConnection", "character"), asNamespace("DBI"))(conn, x, ...)
  })

#' @rdname dbQuote
#' @export
setMethod(
  "dbQuoteIdentifier", c("AthenaConnection", "SQL"),
  getMethod("dbQuoteIdentifier", c("DBIConnection", "SQL"), asNamespace("DBI")))


#' List Athena Tables
#'
#' Returns the unquoted names of Athena tables accessible through this connection.
#' @name dbListTables
#' @inheritParams DBI::dbListTables
#' @param database Athena database, default set to NULL to return all tables from all Athena databases
#' @aliases dbListTables
#' @return \code{dbListTables()} returns a character vector with all the tables from Athena.
#' @seealso \code{\link[DBI]{dbListTables}}
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
#' # Return list of tables in Athena
#' dbListTables(con)
#' 
#' # Disconnect conenction
#' dbDisconnect(con)
#' }
NULL

#' @rdname dbListTables
#' @export
setMethod(
  "dbListTables", "AthenaConnection",
  function(conn, database = NULL, ...){
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    if(is.null(database)){
      tryCatch(database <- sapply(conn@ptr$glue$get_databases()$DatabaseList,function(x) x$Name))}
    tryCatch(output <- lapply(database, function (x) conn@ptr$glue$get_tables(DatabaseName = x)$TableList))
    unlist(lapply(output, function(x) sapply(x, function(y) y$Name)))
  }
)


#' List Field names of Athena table
#'
#' @name dbListFields
#' @inheritParams DBI::dbListFields
#' @return \code{dbListFields()} returns a character vector with all the fields from an Athena table.
#' @seealso \code{\link[DBI]{dbListFields}}
#' @aliases dbListFields
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
#' # Write data.frame to Athena table
#' dbWriteTable(con, "mtcars", mtcars,
#'              partition=c("TIMESTAMP" = format(Sys.Date(), "%Y%m%d")),
#'              s3.location = "s3://mybucket/data/")
#'              
#' # Return list of fields in table
#' dbListFields(con, "mtcars")
#' 
#' # Disconnect conenction
#' dbDisconnect(con)
#' }
#' @docType methods
NULL

#' @rdname dbListFields
#' @export
setMethod("dbListFields", c("AthenaConnection", "character") ,
          function(conn, name, ...) {
            if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
            
            if (grepl("\\.", name)) {
              dbms.name <- gsub("\\..*", "" , name)
              Table <- gsub(".*\\.", "" , name)
            } else {
              dbms.name <- conn@info$dbms.name
              Table <- name}
            
            tryCatch(
              output <- conn@ptr$glue$get_table(DatabaseName = dbms.name,
                                       Name = Table)$Table$StorageDescriptor$Columns)
            sapply(output, function(y) y$Name)
          })

#' Does Athena table exist?
#' 
#' Returns logical scalar if the table exists or not. \code{TRUE} if the table exists, \code{FALSE} otherwise.
#' @name dbExistsTable
#' @inheritParams DBI::dbExistsTable
#' @return \code{dbExistsTable()} returns logical scalar. \code{TRUE} if the table exists, \code{FALSE} otherwise.
#' @seealso \code{\link[DBI]{dbExistsTable}}
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
#' # Write data.frame to Athena table
#' dbWriteTable(con, "mtcars", mtcars,
#'              partition=c("TIMESTAMP" = format(Sys.Date(), "%Y%m%d")),
#'              s3.location = "s3://mybucket/data/")
#'              
#' # Check if table exists from Athena
#' dbExistsTable(con, "mtcars")
#' 
#' # Disconnect conenction
#' dbDisconnect(con)
#' }
#' @docType methods
NULL

#' @rdname dbExistsTable
#' @export
setMethod(
  "dbExistsTable", c("AthenaConnection", "character"),
  function(conn, name, ...) {
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    
    if(grepl("\\.", name)){
      dbms.name <- gsub("\\..*", "" , name)
      Table <- gsub(".*\\.", "" , name)
    } else {dbms.name <- conn@info$dbms.name
    Table <- tolower(name)}
    
    tryerror <- try(conn@ptr$glue$get_table(DatabaseName = dbms.name, Name = Table), silent = TRUE)
    if(inherits(tryerror, "try-error") && !grepl(".*table.*not.*found.*", tryerror[1], ignore.case = T)){
      stop(gsub("^Error : ", "", tryerror[1]), call. = F)}
    !grepl(".*table.*not.*found.*", tryerror[1], ignore.case = T)
  })

#' Remove table from Athena
#' 
#' Removes Athena table but does not remove the data from Amazon S3 bucket.
#' @name dbRemoveTable
#' @return \code{dbRemoveTable()} returns \code{TRUE}, invisibly.
#' @inheritParams DBI::dbRemoveTable
#' @param delete_data Deletes S3 files linking to AWS Athena table
#' @param confirm Allows for S3 files to be deleted without the prompt check. It is recommend to leave this set to \code{FALSE}
#'                   to avoid deleting other S3 files when the table's definition points to the root of S3 bucket.
#' @seealso \code{\link[DBI]{dbRemoveTable}}
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
#' # Write data.frame to Athena table
#' dbWriteTable(con, "mtcars", mtcars,
#'              partition=c("TIMESTAMP" = format(Sys.Date(), "%Y%m%d")),
#'              s3.location = "s3://mybucket/data/")
#'              
#' # Remove Table from Athena
#' dbRemoveTable(con, "mtcars")
#' 
#' # Disconnect conenction
#' dbDisconnect(con)
#' }
#' @docType methods
NULL

#' @rdname dbRemoveTable
#' @export
setMethod(
  "dbRemoveTable", c("AthenaConnection", "character"),
  function(conn, name, delete_data = TRUE, confirm = FALSE, ...) {
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    stopifnot(is.logical(delete_data),
              is.logical(confirm))
    
    if (grepl("\\.", name)) {
      dbms.name <- gsub("\\..*", "" , name)
      Table <- gsub(".*\\.", "" , name)
    } else {
      dbms.name <- conn@info$dbms.name
      Table <- name}
    
    TableType <- conn@ptr$glue$get_table(DatabaseName = dbms.name, Name = Table)$Table$TableType
    
    if(delete_data && TableType == "EXTERNAL_TABLE"){
      nobjects <- 1000 # Only 1000 objects at a time
      while(nobjects >= 1000) {
        tryCatch(
          s3_path <- split_s3_uri(conn@ptr$glue$get_table(DatabaseName = dbms.name,
                                                          Name = Table)$Table$StorageDescriptor$Location))
        tryCatch(
          objects <- conn@ptr$S3$list_objects(Bucket=s3_path$bucket, Prefix=paste0(s3_path$key, "/"))$Contents)
        
        nobjects <- length(objects)
        all_keys <- sapply(objects, function(x) x$Key)
        
        message(paste0("Info: The S3 objects in prefix will be deleted:\n",
                       paste0("s3://", s3_path$bucket, "/", s3_path$key)))
        if(!confirm) {
          confirm <- readline(prompt = "Delete files (y/n)?: ")
          if(confirm != "y") {
            message("Info: Table deletion aborted.")
            return(NULL)}
        }
        
        sapply(all_keys, function(x) conn@ptr$S3$delete_object(Bucket = s3_path$bucket, Key = x))
      }
    }
    
    res <- dbExecute(conn, paste("DROP TABLE ", paste(dbms.name, Table, sep = "."), ";"))
    dbClearResult(res)
    
    if(!delete_data) message("Info: Only Athena table has been removed.")
    on_connection_updated(conn, Table)
    invisible(TRUE)
  })

#' Send query, retrieve results and then clear result set
#'
#' @name dbGetQuery
#' @inheritParams DBI::dbGetQuery
#' @return \code{dbGetQuery()} returns a dataframe.
#' @seealso \code{\link[DBI]{dbGetQuery}}
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
#' # Sending Queries to Athena
#' dbGetQuery(con, "show databases")
#'
#' # Disconnect conenction
#' dbDisconnect(con)
#' }
#' @docType methods
NULL

#' @rdname dbGetQuery
#' @export
setMethod(
  "dbGetQuery", c("AthenaConnection", "character"),
  function(conn,
           statement = NULL, ...){
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    rs <- dbSendQuery(conn, statement = statement)
    on.exit(dbClearResult(rs))
    dbFetch(res = rs, n = -1, ...)
  })


#' Get DBMS metadata
#' 
#' @inheritParams DBI::dbGetInfo
#' @name dbGetInfo
#' @return a named list
#' @seealso \code{\link[DBI]{dbGetInfo}}
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
#' # Returns metadata from connnection object
#' metadata <- dbGetInfo(con)
#' 
#' # Return metadata from Athena query object
#' res <- dbSendQuery(con, "show databases")
#' dbGetInfo(res)
#' 
#' # Clear result
#' dbClearResult(res)
#' 
#' # disconnect from Athena
#' dbDisconnect(con)
#' }
#' @docType methods
NULL

#' @rdname dbGetInfo
#' @export
setMethod(
  "dbGetInfo", "AthenaConnection",
  function(dbObj, ...) {
    if (!dbIsValid(dbObj)) {stop("Connection already closed.", call. = FALSE)}
    get_region <- pkg_method("get_region", "paws.common")
    info <- dbObj@info
    RegionName <- get_region(info$profile_name)
    paws <- as.character(packageVersion("paws"))
    noctua <- as.character(packageVersion("noctua"))
    info <- c(info, region_name = RegionName, paws = paws, noctua = noctua)
    info
  })

#' Athena table partitions
#' 
#' This method returns all partitions from Athena table.
#' @inheritParams DBI::dbExistsTable
#' @return data.frame that returns all partitions in table, if no partitions in Athena table then
#'         function will return error from Athena.
#' @name dbGetPartition
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
#' # write iris table to Athena                  
#' dbWriteTable(con, "iris",
#'              iris,
#'              partition = c("timestamp" = format(Sys.Date(), "%Y%m%d")),
#'              s3.location = "s3://path/to/store/athena/table/")
#' 
#' # return table partitions
#' noctua::dbGetPartition(con, "iris")
#' 
#' # disconnect from Athena
#' dbDisconnect(con)
#' }
#' @docType methods
NULL

#' @rdname dbGetPartition
#' @export
setGeneric("dbGetPartition",
           def = function(conn, name, ...) standardGeneric("dbGetPartition"),
           valueClass = "data.frame")

#' @rdname dbGetPartition
#' @export
setMethod(
  "dbGetPartition", "AthenaConnection",
  function(conn, name, ...) {
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    
    if(grepl("\\.", name)){
      dbms.name <- gsub("\\..*", "" , name)
      Table <- gsub(".*\\.", "" , name)
    } else {dbms.name <- conn@info$dbms.name
    Table <- name}
    dbGetQuery(conn, paste0("SHOW PARTITIONS ", dbms.name,".",Table))
  })

#' Show Athena table's DDL
#' 
#' @description Executes a statement to return the data description language (DDL) of the Athena table.
#' @inheritParams DBI::dbExistsTable
#' @name dbShow
#' @return \code{dbShow()} returns \code{\link[DBI]{SQL}} characters of the Athena table DDL.
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
#' # write iris table to Athena                  
#' dbWriteTable(con, "iris",
#'              iris,
#'              partition = c("timestamp" = format(Sys.Date(), "%Y%m%d")),
#'              s3.location = "s3://path/to/store/athena/table/")
#' 
#' # return table ddl
#' noctua::dbShow(con, "iris")
#' 
#' # disconnect from Athena
#' dbDisconnect(con)
#' }
#' @docType methods
NULL

#' @rdname dbShow
#' @export
setGeneric("dbShow",
           def = function(conn, name, ...) standardGeneric("dbShow"),
           valueClass = "character")

#' @rdname dbShow
#' @export
setMethod(
  "dbShow", "AthenaConnection",
  function(conn, name, ...) {
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    
    if(grepl("\\.", name)){
      dbms.name <- gsub("\\..*", "" , name)
      Table <- gsub(".*\\.", "" , name)
    } else {dbms.name <- conn@info$dbms.name
    Table <- name}
    SQL(dbGetQuery(conn, paste0("SHOW CREATE TABLE ", dbms.name,".",Table))[[1]])
  })
