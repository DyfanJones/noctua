#' S3 implementation of \code{db_desc} for Athena
#' 
#' This is a backend function for dplyr to retrieve meta data about Athena queries. Users won't be required to access and run this function.
#' @param x A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
#' @name db_desc
#' @return
#' Character variable containing Meta Data about query sent to Athena. The Meta Data is returned in the following format:
#' 
#' \code{"Athena <paws version> [<profile_name>@region/database]"}
db_desc.AthenaConnection <- function(x) {
  info <- dbGetInfo(x)
  profile <- if(!is.null(info$profile_name)) paste0(info$profile_name, "@")
  paste0("Athena ",info$paws," [",profile,info$region_name,"/", info$dbms.name,"]")
}

#' S3 implementation of \code{db_compute} for Athena
#' 
#' This is a backend function for dplyr's \code{compute} function. Users won't be required to access and run this function.
#' @param con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
#' @param table Table name, if left default noctua will use the default from \code{dplyr}'s \code{compute} function.
#' @param sql SQL code to be sent to the data
#' @param ... passes \code{noctua} table creation parameters: [\code{file_type},\code{s3_location},\code{partition}]
#' \itemize{
#'          \item{\code{file_type:} What file type to store data.frame on s3, noctua currently supports ["NULL","csv", "parquet", "json"]. 
#'                        \code{"NULL"} will let Athena set the file_type for you.}
#'          \item{\code{s3_location:} s3 bucket to store Athena table, must be set as a s3 uri for example ("s3://mybucket/data/")}
#'          \item{\code{partition:} Partition Athena table, requires to be a partitioned variable from previous table.}}
#' @name db_compute
#' @return
#' \code{db_compute} returns table name
#' @seealso \code{\link{backend_dbplyr}}
#' @examples 
#' \dontrun{
#' # Note: 
#' # - Require AWS Account to run below example.
#' # - Different connection methods can be used please see `noctua::dbConnect` documnentation
#' 
#' library(DBI)
#' library(dplyr)
#' 
#' # Demo connection to Athena using profile name 
#' con <- dbConnect(noctua::athena())
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
  in_schema <- pkg_method("in_schema", "dbplyr")
  
  table <- db_save_query(con, sql, table, ...)
  if (grepl("\\.", table)) {
    schema <- gsub("\\..*", "" , table)
    table <- gsub(".*\\.", "" , table)
  } else {
    schema <- con@info$dbms.name
    table <- table}

  in_schema(schema, table)
}

#' Athena S3 implementation of dbplyr backend functions
#' 
#' These functions are used to build the different types of SQL queries. 
#' The AWS Athena implementation give extra parameters to allow access the to standard DBI Athena methods. They also
#' utilise AWS Glue to speed up sql query execution.
#' @param con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
#' @param sql SQL code to be sent to AWS Athena
#' @param name Table name if left default noctua will use default from 'dplyr''s \code{compute} function.
#' @param file_type What file type to store data.frame on s3, noctua currently supports ["NULL","csv", "tsv", "parquet", "json", "orc"]. 
#'                  \code{"NULL"} will let Athena set the file_type for you.
#' @param s3_location s3 bucket to store Athena table, must be set as a s3 uri for example ("s3://mybucket/data/")
#' @param partition Partition Athena table, requires to be a partitioned variable from previous table.
#' @param compress Compress Athena table, currently can only compress ["parquet", "orc"] \href{https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html}{AWS Athena CTAS}
#' @param ... other parameters, currently not implemented
#' @name backend_dbplyr
#' @return
#' \describe{
#' \item{db_save_query}{Returns table name}
#' \item{db_explain}{Raises an \code{error} as AWS Athena does not support \code{EXPLAIN} queries \href{https://docs.aws.amazon.com/athena/latest/ug/other-notable-limitations.html}{Athena Limitations}}
#' \item{db_query_fields}{Returns sql query column names}
#' }
db_save_query.AthenaConnection <- function(con, sql, name , 
                                           file_type = c("NULL","csv", "tsv", "parquet", "json", "orc"),
                                           s3_location = NULL,
                                           partition = NULL,
                                           compress = TRUE,
                                           ...){
  stopifnot(is.null(s3_location) || is.s3_uri(s3_location))
  file_type = match.arg(file_type)
  tt_sql <- SQL(paste0("CREATE TABLE ",paste0('"',unlist(strsplit(name,"\\.")),'"', collapse = '.'),
                       " ", ctas_sql_with(partition, s3_location, file_type, compress), "AS ",
                       sql, ";"))
  res <- dbExecute(con, tt_sql)
  dbClearResult(res)
  name
}

#' S3 implementation of \code{db_copy_to} for Athena
#' 
#' This is an Athena method for dbplyr function \code{db_copy_to} to create an Athena table from a \code{data.frame}.
#' @param con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
#' @param table A character string specifying a table name. Names will be
#'   automatically quoted so you can use any sequence of characters, not
#'   just any valid bare table name.
#' @param values A data.frame to write to the database.
#' @param overwrite Allows overwriting the destination table. Cannot be \code{TRUE} if \code{append} is also \code{TRUE}.
#' @param append Allow appending to the destination table. Cannot be \code{TRUE} if \code{overwrite} is also \code{TRUE}. Existing Athena DDL file type will be retained
#'               and used when uploading data to AWS Athena. If parameter \code{file.type} doesn't match AWS Athena DDL file type a warning message will be created 
#'               notifying user and \code{noctua} will use the file type for the Athena DDL. 
#' @param types Additional field types used to override derived types.
#' @param s3_location s3 bucket to store Athena table, must be set as a s3 uri for example ("s3://mybucket/data/")
#' @param partition Partition Athena table (needs to be a named list or vector) for example: \code{c(var1 = "2019-20-13")}
#' @param file_type What file type to store data.frame on s3, noctua currently supports ["tsv", "csv", "parquet"]. Default delimited file type is "tsv", in previous versions
#'                  of \code{noctua (=< 1.4.0)} file type "csv" was used as default. The reason for the change is that columns containing \code{Array/JSON} format cannot be written to 
#'                  Athena due to the separating value ",". This would cause issues with AWS Athena. 
#'                  \strong{Note:} "parquet" format is supported by the \code{arrow} package and it will need to be installed to utilise the "parquet" format.
#' @param compress \code{FALSE | TRUE} To determine if to compress file.type. If file type is ["csv", "tsv"] then "gzip" compression is used, for file type "parquet" 
#'                 "snappy" compression is used.
#' @param max_batch Split the data frame by max number of rows i.e. 100,000 so that multiple files can be uploaded into AWS S3. By default when compression
#'                  is set to \code{TRUE} and file.type is "csv" or "tsv" max.batch will split data.frame into 20 batches. This is to help the 
#'                  performance of AWS Athena when working with files compressed in "gzip" format. \code{max.batch} will not split the data.frame 
#'                  when loading file in parquet format. For more information please go to \href{https://github.com/DyfanJones/RAthena/issues/36}{link}
#' @param ... other parameters currently not supported in noctua
#' @name db_copy_to
#' @seealso \code{\link{AthenaWriteTables}}
#' @return
#' db_copy_to returns table name
#' @examples 
#' \dontrun{
#' # Note: 
#' # - Require AWS Account to run below example.
#' # - Different connection methods can be used please see `noctua::dbConnect` documnentation
#' 
#' library(DBI)
#' library(dplyr)
#' 
#' # Demo connection to Athena using profile name 
#' con <- dbConnect(noctua::athena())
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
                                        file_type = c("csv", "tsv", "parquet"), 
                                        compress = FALSE,
                                        max_batch = Inf, ...){
  
  types <- types %||% dbDataType(con, values)
  names(types) <- names(values)
  
  file_type = match.arg(file_type)
  dbWriteTable(conn = con, name = table, value = values,
               overwrite = overwrite, append = append,
               field.types = types, partition = partition,
               s3.location = s3_location, file.type = file_type,
               compress = compress,
               max.batch = max_batch)
  table
}

#' @rdname backend_dbplyr
db_explain.AthenaConnection <- function(con, sql, ...){
  # AWS Athena does not support Explain statements https://docs.aws.amazon.com/athena/latest/ug/other-notable-limitations.html
  stop("Athena does not support EXPLAIN statements.", call. = FALSE)
}

#' @rdname backend_dbplyr
db_query_fields.AthenaConnection <- function(con, sql, ...) {
  
  # check if sql is dbplyr ident
  is_ident <- inherits(sql, "ident")
  
  if(is_ident) { # If ident, get the fields from Glue
    
    if (grepl("\\.", sql)) {
      dbms.name <- gsub("\\..*", "" , sql)
      Table <- gsub(".*\\.", "" , sql)
    } else {
      dbms.name <- con@info$dbms.name
      Table <- sql}
    
    tryCatch(
      output <- con@ptr$glue$get_table(DatabaseName = dbms.name,
                                       Name = Table)$Table)
    
    col_names = vapply(output$StorageDescriptor$Columns, function(y) y$Name, FUN.VALUE = character(1))
    partitions = vapply(output$PartitionKeys,function(y) y$Name, FUN.VALUE = character(1))
    
    c(col_names, partitions)
    
  } else { # If a subquery, query Athena for the fields
    # return dplyr methods
    sql_select <- pkg_method("sql_select", "dplyr")
    sql_subquery <- pkg_method("sql_subquery", "dplyr")
    dplyr_sql <- pkg_method("sql", "dplyr")
    
    sql <- sql_select(con, dplyr_sql("*"), sql_subquery(con, sql), where = dplyr_sql("0 = 1"))
    qry <- dbSendQuery(con, sql)
    on.exit(dbClearResult(qry))
    
    res <- dbFetch(qry, 0)
    names(res)
  }
}
