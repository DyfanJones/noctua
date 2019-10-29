#' Convenience functions for reading/writing DBMS tables
#'
#' @param conn An \code{\linkS4class{AthenaConnection}} object, produced by
#'   [DBI::dbConnect()]
#' @param name A character string specifying a table name. Names will be
#'   automatically quoted so you can use any sequence of characters, not
#'   just any valid bare table name.
#' @param value A data.frame to write to the database.
#' @param overwrite Allow overwriting the destination table. Cannot be
#'   `TRUE` if `append` is also `TRUE`.
#' @param append Allow appending to the destination table. Cannot be
#'   `TRUE` if `overwrite` is also `TRUE`.
#' @param field.types Additional field types used to override derived types.
#' @param partition Partition Athena table (needs to be a named list or vector) for example: \code{c(var1 = "2019-20-13")}
#' @param s3.location s3 bucket to store Athena table, must be set as a s3 uri for example ("s3://mybucket/data/")
#' @param file.type What file type to store data.frame on s3, noctua currently supports ["csv", "tsv", "parquet"].
#'                  \strong{Note:} "parquet" format is supported by the \code{arrow} package and it will need to be installed to utilise the "parquet" format.
#' @inheritParams DBI::sqlCreateTable
#' @return \code{dbWriteTable()} returns \code{TRUE}, invisibly. If the table exists, and both append and overwrite
#'         arguments are unset, or append = TRUE and the data frame with the new data has different column names,
#'         an error is raised; the remote table remains unchanged.
#' @seealso \code{\link[DBI]{dbWriteTable}}
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
#' # List existing tables in Athena
#' dbListTables(con)
#' 
#' # Write data.frame to Athena table
#' dbWriteTable(con, "mtcars", mtcars,
#'              partition=c("TIMESTAMP" = format(Sys.Date(), "%Y%m%d")),
#'              s3.location = "s3://mybucket/data/")
#'              
#' # Read entire table from Athena
#' dbReadTable(con, "mtcars")
#'
#' # List all tables in Athena after uploading new table to Athena
#' dbListTables(con)
#' 
#' # Checking if uploaded table exists in Athena
#' dbExistsTable(con, "mtcars")
#'
#' # Disconnect from Athena
#' dbDisconnect(con)
#' }
#' @name AthenaWriteTables
NULL

Athena_write_table <-
  function(conn, name, value, overwrite=FALSE, append=FALSE,
           row.names = NA, field.types = NULL, 
           partition = NULL, s3.location = NULL, file.type = c("csv", "tsv", "parquet"), ...) {
    # variable checks
    stopifnot(is.character(name),
              is.data.frame(value),
              is.logical(overwrite),
              is.logical(append),
              is.s3_uri(s3.location))
    stopifnot(is.null(partition) || is.character(partition) || is.list(partition))
    
    sapply(tolower(names(partition)), function(x){if(x %in% tolower(names(value))){
      stop("partition ", x, " is a variable in data.frame ", deparse(substitute(value)), call. = FALSE)}})
    
    file.type = match.arg(file.type)
    
    # made everything lower case due to aws Athena issue: https://aws.amazon.com/premiumsupport/knowledge-center/athena-aws-glue-msck-repair-table/
    name <- tolower(name)
    s3.location <- tolower(s3.location)
    if(!is.null(partition) && is.null(names(partition))) stop("partition parameter requires to be a named vector or list", call. = FALSE)
    if(!is.null(partition)) {names(partition) <- tolower(names(partition))}
    
    if(!grepl("\\.", name)) Name <- paste(conn@info$dbms.name, name, sep = ".") 
    else{Name <- name
    name <- gsub(".*\\.", "", name)}
    
    if (overwrite && append) stop("overwrite and append cannot both be TRUE", call. = FALSE)
    
    if(append && is.null(partition)) stop("Athena requires the table to be partitioned to append", call. = FALSE)
    
    t <- tempfile()
    on.exit({unlink(t)
      if(!is.null(conn@info$expiration)) time_check(conn@info$expiration)})
    
    value <- sqlData(conn, value, row.names = row.names)
    
    # check if arrow is installed before attempting to create parquet
    if(file.type == "parquet"){
      if(!requireNamespace("arrow", quietly=TRUE))
        stop("The package arrow is required for R to utilise Apache Arrow to create parquet files.", call. = FALSE)
      else {arrow::write_parquet(value, t)}
    }
    
    # writes out csv/tsv, uses data.table for extra speed
    switch(file.type,
           "csv" = data.table::fwrite(value, t, showProgress = F),
           "tsv" = data.table::fwrite(value, t, sep = "\t", showProgress = F))
    
    found <- dbExistsTable(conn, Name)
    if (found && !overwrite && !append) {
      stop("Table ", Name, " exists in database, and both overwrite and",
           " append are FALSE", call. = FALSE)
    }
    
    if(!found && append){
      stop("Table ", Name, " does not exist in database and append is set to TRUE", call. = T)
    }
    
    if (found && overwrite) {
      suppressMessages(dbRemoveTable(conn, Name)) # suppressing info message
    }
    
    # send data over to s3 bucket
    upload_data(conn, t, name, partition, s3.location, file.type)
    
    if (!append) {
      sql <- sqlCreateTable(conn, Name, value, field.types = field.types, 
                            partition = names(partition),
                            s3.location = s3.location, file.type = file.type)
      # create Athena table
      rs <- dbExecute(conn, sql)
      dbClearResult(rs)}
    
    # Repair table
    res <- dbExecute(conn, paste0("MSCK REPAIR TABLE ", Name))
    dbClearResult(res)
    
    invisible(TRUE)
  }

# send data to s3 is Athena registered location
upload_data <- function(con, x, name, partition = NULL, s3.location= NULL,  file.type = NULL) {
  partition <- unlist(partition)
  partition <- paste(names(partition), unname(partition), sep = "=", collapse = "/")
  
  Name <- paste0(name, ".", file.type)
  s3_info <- split_s3_uri(s3.location)
  s3_info$key <- gsub("/$", "", s3_info$key)
  if(grepl(name, s3_info$key)){s3_info$key <- gsub(name, "", s3_info$key)
  s3_info$key <- gsub( "|/$", "", s3_info$key)}
  
  if(s3_info$key != "" && partition == ""){s3_key <- paste(s3_info$key,name, Name, sep = "/")}
  else if (s3_info$key == "" && partition != "") {s3_key <- paste(name, partition, Name, sep = "/")}
  else if (s3_info$key == "" && partition == "") {s3_key <- paste(name, Name, sep = "/")}
  else {s3_key <- paste(s3_info$key, name, partition, Name, sep = "/")}
  
  obj <- readBin(x, "raw", n = file.size(x))
  tryCatch(con@ptr$S3$put_object(Body = obj, Bucket = s3_info$bucket,Key = s3_key))
  invisible(TRUE)
}

#' @rdname AthenaWriteTables
#' @inheritParams DBI::dbWriteTable
#' @export
setMethod(
  "dbWriteTable", c("AthenaConnection", "character", "data.frame"),
  function(conn, name, value, overwrite=FALSE, append=FALSE,
           row.names = NA, field.types = NULL, 
           partition = NULL, s3.location = NULL, file.type = c("csv", "tsv", "parquet"), ...){
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    Athena_write_table(conn, name, value, overwrite, append,
                       row.names, field.types,
                       partition, s3.location, file.type)
  })

#' @rdname AthenaWriteTables
#' @export
setMethod(
  "dbWriteTable", c("AthenaConnection", "Id", "data.frame"),
  function(conn, name, value, overwrite=FALSE, append=FALSE,
           row.names = NA, field.types = NULL, 
           partition = NULL, s3.location = NULL, file.type = c("csv", "tsv", "parquet"), ...){
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    Athena_write_table(conn, name, value, overwrite, append,
                       row.names, field.types,
                       partition, s3.location, file.type)
  })

#' @rdname AthenaWriteTables
#' @export
setMethod(
  "dbWriteTable", c("AthenaConnection", "SQL", "data.frame"),
  function(conn, name, value, overwrite=FALSE, append=FALSE,
           row.names = NA, field.types = NULL, 
           partition = NULL, s3.location = NULL, file.type = c("csv", "tsv", "parquet"), ...){
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    Athena_write_table(conn, name, value, overwrite, append,
                       row.names, field.types,
                       partition, s3.location, file.type)
  })


#' Converts data frame into suitable format to be uploaded to Athena
#'
#' This method converts data.frame columns into the correct format so that it can be uploaded Athena.
#' @name sqlData
#' @inheritParams DBI::sqlData
#' @return \code{sqlData} returns a dataframe formatted for Athena. Currently converts \code{list} variable types into \code{character}
#'         split by \code{'|'}, similar to how \code{data.table} writes out to files.
#' @seealso \code{\link[DBI]{sqlData}}
NULL

#' @rdname sqlData
#' @export
setMethod("sqlData", "AthenaConnection", function(con, value, row.names = NA, ...) {
  stopifnot(is.data.frame(value))
  value <- sqlRownamesToColumn(value, row.names)
  names(value) <- tolower(gsub("\\.", "_", make.names(names(value), unique = TRUE)))
  for(i in seq_along(value)){
    if(is.list(value[[i]])){
      value[[i]] <- sapply(value[[i]], paste, collapse = "|")
    }
  }
  value
})

#' Creates query to create a simple Athena table
#' 
#' Creates an interface to compose \code{CREATE EXTERNAL TABLE}.
#' @name sqlCreateTable
#' @inheritParams DBI::sqlCreateTable
#' @param field.types Additional field types used to override derived types.
#' @param partition Partition Athena table (needs to be a named list or vector) for example: \code{c(var1 = "2019-20-13")}
#' @param s3.location s3 bucket to store Athena table
#' @param file.type What file type to store data.frame on s3, noctua currently supports ["csv", "tsv", "parquet"]
#' @return \code{sqlCreateTable} returns data.frame's \code{DDL} in the \code{\link[DBI]{SQL}} format.
#' @seealso \code{\link[DBI]{sqlCreateTable}}
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
#' # Create DDL for iris data.frame
#' sqlCreateTable(con, "iris", iris, s3.location = "s3://path/to/athena/table")
#' 
#' # Create DDL for iris data.frame with partition
#' sqlCreateTable(con, "iris", iris, 
#'                partition = c("timestamp" = format(Sys.Date(), "%Y%m%d")),
#'                s3.location = "s3://path/to/athena/table")
#'                
#' # Create DDL for iris data.frame with partition and file.type parquet
#' sqlCreateTable(con, "iris", iris, 
#'                partition = c("timestamp" = format(Sys.Date(), "%Y%m%d")),
#'                s3.location = "s3://path/to/athena/table",
#'                file.type = "parquet")
#' 
#' # Disconnect from Athena
#' dbDisconnect(con)
#' }
NULL

#' @rdname sqlCreateTable
#' @export
setMethod("sqlCreateTable", "AthenaConnection",
          function(con, table = NULL, fields = NULL, field.types = NULL, partition = NULL, s3.location= NULL, file.type = c("csv", "tsv", "parquet"), ...){
            if (!dbIsValid(con)) {stop("Connection already closed.", call. = FALSE)}
            stopifnot(is.character(table),
                      is.data.frame(fields),
                      is.null(field.types) || is.character(field.types),
                      is.null(partition) || is.character(partition) || is.list(partition),
                      is.s3_uri(s3.location))
            
            field <- createFields(con, fields, field.types = field.types)
            file.type <- match.arg(file.type)
            table1 <- gsub(".*\\.", "", table)
            
            s3.location <- gsub("/$", "", s3.location)
            if(grepl(table1, s3.location)){s3.location <- gsub(paste0("/", table1,"$"), "", s3.location)}
            s3.location <- paste0("'",s3.location,"/", table1,"/'")
            SQL(paste0(
              "CREATE EXTERNAL TABLE ", table, " (\n",
              "  ", paste(field, collapse = ",\n  "), "\n)\n",
              partitioned(partition),
              FileType(file.type), "\n",
              "LOCATION ",s3.location, "\n",
              header(file.type)
            ))
          }
)

# Helper functions: fields
createFields <- function(con, fields, field.types) {
  if (is.data.frame(fields)) {
    fields <- vapply(fields, function(x) DBI::dbDataType(con, x), character(1))
  }
  if (!is.null(field.types)) {
    fields[names(field.types)] <- field.types
  }
  
  field_names <- tolower(gsub("\\.", "_", make.names(names(fields), unique = TRUE)))
  message("Info: data.frame colnames have been converted to align with Athena DDL naming convertions: \n",paste0(field_names, collapse= ",\n"))
  field.types <- unname(fields)
  paste0(field_names, " ", field.types)
}

# Helper function partition
partitioned <- function(obj = NULL){
  if(!is.null(obj)) {
    obj <- paste(obj, "STRING", collapse = ", ")
    paste0("PARTITIONED BY (", obj, ")\n") }
}

FileType <- function(obj){
  switch(obj,
         csv = gsub("_","","ROW FORMAT DELIMITED\n\tFIELDS TERMINATED BY ','\n\tESCAPED BY '\\\\'\n\tLINES TERMINATED BY '\\_n'"),
         tsv = gsub("_","","ROW FORMAT DELIMITED\n\tFIELDS TERMINATED BY '\t'\n\tESCAPED BY '\\\\'\n\tLINES TERMINATED BY '\\_n'"),
         parquet = SQL("STORED AS PARQUET"))
}

header <- function(obj){
  switch(obj,
         csv = "TBLPROPERTIES (\"skip.header.line.count\"=\"1\");",
         tsv = "TBLPROPERTIES (\"skip.header.line.count\"=\"1\");",
         parquet = ";")
}

