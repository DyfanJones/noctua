#' S3 implementation of \code{db_desc} for Athena
#' 
#' This is a backend function for dplyr to retrieve meta data about Athena queries. Users won't be required to access and run this function.
#' @param x A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
#' @name db_desc
#' @return
#' Character variable containing Meta Data about query sent to Athena. The Meta Data is returned in the following format:
#' 
#' \code{"Athena <boto3 version> [<profile_name>@region/database]"}
db_desc.AthenaConnection <- function(x) {
  info <- dbGetInfo(x)
  profile <- if(!is.null(info$profile_name)) paste0(info$profile_name, "@")
  paste0("Athena ",info$paws," [",profile,info$region_name,"/", info$dbms.name,"]")
}

#' S3 implementation of \code{db_compute} for Athena
#' 
#' This is a backend function for dplyr's \code{compute} function. Users won't be required to access and run this function.
#' @param con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
#' @param table Table name, if left default paws.athena will use the default from \code{dplyr}'s \code{compute} function.
#' @param sql SQL code to be sent to the data
#' @param ... passes \code{paws.athena} table creation parameters: [\code{file_type},\code{s3_location},\code{partition}]
#' \itemize{
#'          \item{\code{file_type:} What file type to store data.frame on s3, paws.athena currently supports ["NULL","csv", "parquet", "json"]. 
#'                        \code{"NULL"} will let athena set the file_type for you.}
#'          \item{\code{s3_location:} s3 bucket to store Athena table, must be set as a s3 uri for example ("s3://mybucket/data/")}
#'          \item{\code{partition:} Partition Athena table, requires to be a partitioned variable from previous table.}}
#' @name db_compute
#' @return
#' db_compute returns table name
#' @seealso \code{\link{db_save_query}}
#' @examples 
#' \dontrun{
#' # Note: 
#' # - Require AWS Account to run below example.
#' # - Different connection methods can be used please see `paws.athena::dbConnect` documnentation
#' 
#' library(DBI)
#' library(dplyr)
#' 
#' # Demo connection to Athena using profile name 
#' con <- dbConnect(paws.athena::athena())
#' 
#' # Write data.frame to Athena table
#' copy_to(con, mtcars,
#'         s3_location = "s3://mybucket/data/")
#'              
#' # Write Athena table from tbl_sql
#' athena_mtcars <- tbl(con, "mtcars")
#' mtcars_filter <- athena_mtcars %>% filter(gear >=4)
#' 
#' # create athena with unique table name
#' mtcars_filer %>% 
#'   compute()
#' 
#' # create athena with specified name and s3 location
#' mtcars_filer %>% 
#'     compute("mtcars_filer",
#'             s3_location = "s3://mybucket/mtcars_filer/")
#' 
#' # Disconnect from Athena
#' dbDisconnect(con)
#' }
db_compute.AthenaConnection <- function(con,
                                        table,
                                        sql,
                                        ...) {
  db_save_query <- pkg_method("db_save_query", "dplyr")
  table <- db_save_query(con, sql, table, ...)
  table
}

#' S3 implementation of \code{db_save_query} for Athena
#' 
#' This is a backend method for dplyr function \code{db_save_query} to creating tables from \code{tbl} using \code{compute} function.
#' Users won't be required to access and run this function. However users may find it useful to know the extra
#' parameters \code{db_save_query} provided for \code{compute} function.
#' @param con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
#' @param sql SQL code to be sent to the data
#' @param name Table name if left default paws.athena will use default from 'dplyr''s \code{compute} function.
#' @param file_type What file type to store data.frame on s3, paws.athena currently supports ["NULL","csv", "parquet", "json"]. 
#'                  \code{"NULL"} will let athena set the file_type for you.
#' @param s3_location s3 bucket to store Athena table, must be set as a s3 uri for example ("s3://mybucket/data/")
#' @param partition Partition Athena table, requires to be a partitioned variable from previous table.
#' @param ... other parameters, currently not implemented
#' @name db_save_query
#' @return
#' db_save_query returns table name
db_save_query.AthenaConnection <- function(con, sql, name , 
                                           file_type = c("NULL","csv", "parquet", "json"),
                                           s3_location = NULL,
                                           partition = NULL,
                                           ...){
  stopifnot(is.null(s3_location) || is.s3_uri(s3_location))
  file_type = match.arg(file_type)
  tt_sql <- paste0("CREATE TABLE ",name, " ", db_save_query_with(file_type, s3_location, partition), "AS ",
                   sql, ";")
  res <- dbExecute(con, tt_sql)
  # check if execution failed
  query_execution <- res@connection@ptr$Athena$get_query_execution(QueryExecutionId = res@info$QueryExecutionId)
  if(query_execution$QueryExecution$Status$State == "FAILED") {
    stop(query_execution$QueryExecution$Status$StateChangeReason, call. = FALSE)
  }
  name
}

# helper function
db_save_query_with <- function(file_type, s3_location,partition){
  if(file_type!="NULL" || !is.null(s3_location) || !is.null(partition)){
    FILE <- switch(file_type,
                   "csv" = "format = 'TEXTFILE'",
                   "parquet" = "format = 'PARQUET'",
                   "json" = "format = 'JSON'",
                   "")
    LOCATION <- if(!is.null(s3_location)){
      if(file_type == "NULL") paste0("external_location ='", s3_location, "'")
      else paste0(",\nexternal_location ='", s3_location, "'")
    } else ""
    PARTITION <- if(!is.null(partition)){
      partition <- paste(partition, collapse = "','")
      if(is.null(s3_location) && file_type == "NULL") paste0("partitioned_by = ARRAY['",partition,"']")
      else paste0(",\npartitioned_by = ARRAY['",partition,"']")
    } else ""
    paste0("WITH (", FILE, LOCATION, PARTITION,")\n")
  } else ""
}

#' S3 implementation of \code{db_copy_to} for Athena
#' 
#' This is an Athena method for dbplyr function \code{db_copy_to} to create an Athena table from a \code{data.frame}.
#' @param con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
#' @param table A character string specifying a table name. Names will be
#'   automatically quoted so you can use any sequence of characters, not
#'   just any valid bare table name.
#' @param values A data.frame to write to the database.
#' @param overwrite Allow overwriting the destination table. Cannot be
#'   `TRUE` if `append` is also `TRUE`.
#' @param append Allow appending to the destination table. Cannot be
#'   `TRUE` if `overwrite` is also `TRUE`.
#' @param types Additional field types used to override derived types.
#' @param s3_location s3 bucket to store Athena table, must be set as a s3 uri for example ("s3://mybucket/data/")
#' @param partition Partition Athena table (needs to be a named list or vector) for example: \code{c(var1 = "2019-20-13")}
#' @param file_type What file type to store data.frame on s3, paws.athena currently supports ["csv", "tsv", "parquet"]. 
#'                  \strong{Note:} "parquet" format is supported by the \code{arrow} package and it will need to be installed to utilise the "parquet" format.
#' @param ... other parameters currently not supported in paws.athena
#' @name db_copy_to
#' @seealso \code{\link{AthenaWriteTables}}
#' @return
#' db_copy_to returns table name
#' @examples 
#' \dontrun{
#' # Note: 
#' # - Require AWS Account to run below example.
#' # - Different connection methods can be used please see `paws.athena::dbConnect` documnentation
#' 
#' library(DBI)
#' library(dplyr)
#' 
#' # Demo connection to Athena using profile name 
#' con <- dbConnect(paws.athena::athena())
#' 
#' # List existing tables in Athena
#' dbListTables(con)
#' 
#' # Write data.frame to Athena table
#' copy_to(con, mtcars,
#'         s3_location = "s3://mybucket/data/")
#'              
#' # Checking if uploaded table exists in Athena
#' dbExistsTable(con, "mtcars")
#'
#' # Write Athena table from tbl_sql
#' athena_mtcars <- tbl(con, "mtcars")
#' mtcars_filter <- athena_mtcars %>% filter(gear >=4)
#' 
#' copy_to(con, mtcars_filter)
#' 
#' # Checking if uploaded table exists in Athena
#' dbExistsTable(con, "mtcars_filter") 
#' 
#' # Disconnect from Athena
#' dbDisconnect(con)
#' }

db_copy_to.AthenaConnection <- function(con, table, values,
                                        overwrite = FALSE, append = FALSE,
                                        types = NULL, partition = NULL,
                                        s3_location = NULL, 
                                        file_type = c("csv", "tsv", "parquet"), ...){
  
  types <- types %||% dbDataType(con, values)
  names(types) <- names(values)
  
  file_type = match.arg(file_type)
  dbWriteTable(conn = con, name = table, value = values,
               overwrite = overwrite, append = append,
               field.types = types, partition = partition,
               s3.location = s3_location, file.type = file_type)
  table
}


