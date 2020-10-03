#' Convenience functions for reading/writing DBMS tables
#'
#' @param conn An \code{\linkS4class{AthenaConnection}} object, produced by [DBI::dbConnect()]
#' @param name A character string specifying a table name. Names will be
#'   automatically quoted so you can use any sequence of characters, not
#'   just any valid bare table name.
#' @param value A data.frame to write to the database.
#' @param overwrite Allows overwriting the destination table. Cannot be \code{TRUE} if \code{append} is also \code{TRUE}.
#' @param append Allow appending to the destination table. Cannot be \code{TRUE} if \code{overwrite} is also \code{TRUE}. Existing Athena DDL file type will be retained
#'               and used when uploading data to AWS Athena. If parameter \code{file.type} doesn't match AWS Athena DDL file type a warning message will be created 
#'               notifying user and \code{noctua} will use the file type for the Athena DDL. When appending to an Athena DDL that has been created outside of \code{noctua}.
#'               \code{noctua} can support the following SerDes and Data Formats.
#' \itemize{
#' \item{\strong{csv/tsv:} \href{https://docs.aws.amazon.com/athena/latest/ug/lazy-simple-serde.html}{LazySimpleSerDe}}
#' \item{\strong{parquet:} \href{https://docs.aws.amazon.com/athena/latest/ug/parquet.html}{Parquet SerDe}}
#' \item{\strong{json:} \href{https://docs.aws.amazon.com/athena/latest/ug/json.html}{JSON SerDe Libraries}}
#' }
#' @param field.types Additional field types used to override derived types.
#' @param partition Partition Athena table (needs to be a named list or vector) for example: \code{c(var1 = "2019-20-13")}
#' @param s3.location s3 bucket to store Athena table, must be set as a s3 uri for example ("s3://mybucket/data/").
#'        By default, the s3.location is set to s3 staging directory from \code{\linkS4class{AthenaConnection}} object. \strong{Note:}
#'        When creating a table for the first time \code{s3.location} will be formatted from \code{"s3://mybucket/data/"} to the following 
#'        syntax \code{"s3://{mybucket/data}/{schema}/{table}/{parition}/"} this is to support tables with the same name but existing in different 
#'        schemas. If schema isn't specified in \code{name} parameter then the schema from \code{dbConnect} is used instead.
#' @param file.type What file type to store data.frame on s3, noctua currently supports ["tsv", "csv", "parquet", "json"]. Default delimited file type is "tsv", in previous versions
#'                  of \code{noctua (=< 1.4.0)} file type "csv" was used as default. The reason for the change is that columns containing \code{Array/JSON} format cannot be written to 
#'                  Athena due to the separating value ",". This would cause issues with AWS Athena. 
#'                  \strong{Note:} "parquet" format is supported by the \code{arrow} package and it will need to be installed to utilise the "parquet" format.
#'                  "json" format is supported by \code{jsonlite} package and it will need to be installed to utilise the "json" format.
#' @param compress \code{FALSE | TRUE} To determine if to compress file.type. If file type is ["csv", "tsv"] then "gzip" compression is used, for file type "parquet" 
#'                 "snappy" compression is used. Currently \code{noctua} doesn't support compression for "json" file type.
#' @param max.batch Split the data frame by max number of rows i.e. 100,000 so that multiple files can be uploaded into AWS S3. By default when compression
#'                  is set to \code{TRUE} and file.type is "csv" or "tsv" max.batch will split data.frame into 20 batches. This is to help the 
#'                  performance of AWS Athena when working with files compressed in "gzip" format. \code{max.batch} will not split the data.frame 
#'                  when loading file in parquet format. For more information please go to \href{https://github.com/DyfanJones/RAthena/issues/36}{link}
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
#' # using default s3.location
#' dbWriteTable(con, "iris", iris)
#' 
#' # Read entire table from Athena
#' dbReadTable(con, "iris")
#'
#' # List all tables in Athena after uploading new table to Athena
#' dbListTables(con)
#' 
#' # Checking if uploaded table exists in Athena
#' dbExistsTable(con, "iris")
#' 
#' # Disconnect from Athena
#' dbDisconnect(con)
#' }
#' @name AthenaWriteTables
NULL

Athena_write_table <-
  function(conn, name, value, overwrite=FALSE, append=FALSE,
           row.names = NA, field.types = NULL, 
           partition = NULL, s3.location = NULL, file.type = c("tsv", "csv", "parquet", "json"),
           compress = FALSE, max.batch = Inf, ...) {
    # variable checks
    stopifnot(is.character(name),
              is.data.frame(value),
              is.logical(overwrite),
              is.logical(append),
              is.null(s3.location) || is.s3_uri(s3.location),
              is.null(partition) || is.character(partition) || is.list(partition),
              is.logical(compress))
    
    sapply(tolower(names(partition)), function(x){if(x %in% tolower(names(value))){
      stop("partition ", x, " is a variable in data.frame ", deparse(substitute(value)), call. = FALSE)}})
    
    file.type = match.arg(file.type)
    
    if(max.batch < 0) stop("`max.batch` has to be greater than 0", call. = F)
    
    if(!is.infinite(max.batch) && file.type == "parquet") message("Info: parquet format is splittable and AWS Athena can read parquet format ",
                                                                  "in parallel. `max.batch` is used for compressed `gzip` format which is not splittable.")
    
    # use default s3_staging directory is s3.location isn't provided
    if (is.null(s3.location)) s3.location <- conn@info$s3_staging
    
    # made everything lower case due to aws Athena issue: https://aws.amazon.com/premiumsupport/knowledge-center/athena-aws-glue-msck-repair-table/
    name <- tolower(name)
    s3.location <- tolower(s3.location)
    if(!is.null(partition) && is.null(names(partition))) stop("partition parameter requires to be a named vector or list", call. = FALSE)
    if(!is.null(partition)) {names(partition) <- tolower(names(partition))}
    
    if(!grepl("\\.", name)) name <- paste(conn@info$dbms.name, name, sep = ".") 
    
    if (overwrite && append) stop("overwrite and append cannot both be TRUE", call. = FALSE)
    
    # Check if table already exists in the database
    found <- dbExistsTable(conn, name)
    
    if (found && !overwrite && !append) {
      stop("Table ", name, " exists in database, and both overwrite and",
           " append are FALSE", call. = FALSE)
    }
    
    if(!found && append){
      stop("Table ", name, " does not exist in database and append is set to TRUE", call. = T)
    }
    
    if (found && overwrite) {
      dbRemoveTable(conn, name, confirm = TRUE)
    }
    
    # Check file format if appending
    if(found && append){
      dbms.name <- gsub("\\..*", "" , name)
      Table <- gsub(".*\\.", "" , name)
      
      tryCatch(
      tbl_info <- conn@ptr$glue$get_table(DatabaseName = dbms.name,
                                 Name = Table)$Table)
      
      # Return correct file format when appending onto existing AWS Athena table
      File.Type <- switch(tbl_info$StorageDescriptor$SerdeInfo$SerializationLibrary, 
                          "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe" = switch(tbl_info$StorageDescriptor$SerdeInfo$Parameters$field.delim, 
                                                                                        "," = "csv",
                                                                                        "\t" = "tsv",
                                                                                        stop("noctua currently only supports csv and tsv delimited format")),
                          "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" = "parquet",
                          # json library support: https://docs.aws.amazon.com/athena/latest/ug/json.html#hivejson
                          "org.apache.hive.hcatalog.data.JsonSerDe" = "json",
                          "org.openx.data.jsonserde.JsonSerDe" = "json",
                          stop("Unable to append onto table: ", name,"\n", tbl_info$StorageDescriptor$SerdeInfo$SerializationLibrary,
                               ": Is currently not supported by noctua", call. = F))
      
      # Return if existing files are compressed or not
      compress = switch(File.Type,
                        "parquet" = {if(is.null(tbl_info$Parameters$parquet.compress)) FALSE else {
                          if(tolower(tbl_info$Parameters$parquet.compress) == "snappy") TRUE else 
                            stop("noctua currently only supports SNAPPY compression for parquet", call. = F)}
                        },
                        "tsv" = {if(is.null(tbl_info$Parameters$compressionType)) FALSE else {
                          if(tolower(tbl_info$Parameters$compressionType) == "gzip") TRUE else
                            stop("noctua currently only supports gzip compression for tsv", call. = F)}},
                        "csv" = {if(is.null(tbl_info$Parameters$compressionType)) FALSE else {
                          if(tolower(tbl_info$Parameters$compressionType) == "gzip") TRUE else
                            stop("noctua currently only supports gzip compression for csv", call. = F)}},
                        "json" = {if(is.null(tbl_info$Parameters$compressionType)) FALSE else {
                          if(!is.null(tbl_info$Parameters$compressionType)) 
                            stop("RAthena currently doesn't support compression for json", call. = F)}}
                        )
      if(file.type != File.Type) warning('Appended `file.type` is not compatible with the existing Athena DDL file type and has been converted to "', File.Type,'".', call. = FALSE)
      
      # get previous s3 location #73
      s3.location <- tolower(tbl_info$StorageDescriptor$Location)
      file.type <- File.Type
    }
    
    # return original Athena Types
    if(is.null(field.types)) field.types <- dbDataType(conn, value)
    value <- sqlData(conn, value, row.names = row.names, file.type = file.type)
    
    ############# write data frame to file type ###########################
    
    # create temp location
    temp_dir <- tempdir()

    # split data
    SplitVec <- split_vec(value, max.batch = max.batch)
    max_row <- nrow(value)
    
    FileLocation <- character(length(SplitVec))
    args <- list(dt = value,
                 max.batch = max.batch,
                 max_row = max_row,
                 path = temp_dir,
                 file.type = file.type,
                 compress = compress)
    args <- update_args(file.type, args)
    
    # write data.frame to backend in batch
    for(i in seq_along(SplitVec)){
      args$split_vec <- SplitVec[i]
      FileLocation[[i]] <- do.call(write_batch, args)
    }
    
    ############# update data to aws s3 ###########################
    
    # send data over to s3 bucket
    upload_data(conn, FileLocation, name, partition, s3.location, file.type, compress, append)
    
    if (!append) {
      sql <- sqlCreateTable(conn, table = name, fields = value, field.types = field.types, 
                            partition = names(partition),
                            s3.location = s3.location, file.type = file.type,
                            compress = compress)
      # create Athena table
      rs <- dbExecute(conn, sql)
      dbClearResult(rs)}
    
    # Repair table
    repair_table(conn, name, partition, s3.location, append)
    
    on.exit({lapply(FileLocation, unlink)
      if(!is.null(conn@info$expiration)) time_check(conn@info$expiration)})
    
    invisible(TRUE)
  }

# send data to s3 is Athena registered location
upload_data <- function(con, x, name, partition = NULL, s3.location= NULL,  file.type = NULL, compress = NULL, append = FALSE) {
  
  # Get schema and name
  if (grepl("\\.", name)) {
    schema <- gsub("\\..*", "" , name)
    name <- gsub(".*\\.", "" , name)
  } else {
    schema <- con@info$dbms.name
    name <- name}
  
  # create s3 location components
  s3_key <- s3_upload_location(x, schema, name, partition, s3.location, file.type, compress, append)
  
  for (i in 1:length(x)){
    obj <- readBin(x[i], "raw", n = file.size(x[i]))
    retry_api_call(con@ptr$S3$put_object(Body = obj, Bucket = s3_key[[1]], Key = s3_key[[2]][i]))}

  invisible(NULL)
}

#' @rdname AthenaWriteTables
#' @inheritParams DBI::dbWriteTable
#' @export
setMethod(
  "dbWriteTable", c("AthenaConnection", "character", "data.frame"),
  function(conn, name, value, overwrite=FALSE, append=FALSE,
           row.names = NA, field.types = NULL, 
           partition = NULL, s3.location = NULL, file.type = c("tsv", "csv", "parquet", "json"),
           compress = FALSE, max.batch = Inf, ...){
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    Athena_write_table(conn, name, value, overwrite, append,
                       row.names, field.types,
                       partition, s3.location, file.type, compress, max.batch)
  })

#' @rdname AthenaWriteTables
#' @export
setMethod(
  "dbWriteTable", c("AthenaConnection", "Id", "data.frame"),
  function(conn, name, value, overwrite=FALSE, append=FALSE,
           row.names = NA, field.types = NULL, 
           partition = NULL, s3.location = NULL, file.type = c("tsv", "csv", "parquet", "json"),
           compress = FALSE, max.batch = Inf, ...){
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    Athena_write_table(conn, name, value, overwrite, append,
                       row.names, field.types,
                       partition, s3.location, file.type, compress, max.batch)
  })

#' @rdname AthenaWriteTables
#' @export
setMethod(
  "dbWriteTable", c("AthenaConnection", "SQL", "data.frame"),
  function(conn, name, value, overwrite=FALSE, append=FALSE,
           row.names = NA, field.types = NULL, 
           partition = NULL, s3.location = NULL, file.type = c("tsv", "csv", "parquet", "json"),
           compress = FALSE, max.batch = Inf, ...){
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    Athena_write_table(conn, name, value, overwrite, append,
                       row.names, field.types,
                       partition, s3.location, file.type, compress, max.batch)
  })


#' Converts data frame into suitable format to be uploaded to Athena
#'
#' This method converts data.frame columns into the correct format so that it can be uploaded Athena.
#' @name sqlData
#' @inheritParams DBI::sqlData
#' @param file.type What file type to store data.frame on s3, noctua currently supports ["csv", "tsv", "parquet", "json"].
#'                  \strong{Note:} This parameter is used for format any special characters that clash with file type separator.
#' @return \code{sqlData} returns a dataframe formatted for Athena. Currently converts \code{list} variable types into \code{character}
#'         split by \code{'|'}, similar to how \code{data.table} writes out to files.
#' @seealso \code{\link[DBI]{sqlData}}
NULL

#' @rdname sqlData
#' @export
setMethod("sqlData", "AthenaConnection", 
          function(con, value, row.names = NA, file.type = c("tsv", "csv", "parquet", "json"),...) {
  stopifnot(is.data.frame(value))
  file.type = match.arg(file.type)
  Value <- copy(value)
  Value <- sqlRownamesToColumn(Value, row.names)
  field_names <- gsub("\\.", "_", make.names(names(Value), unique = TRUE))
  DIFF <- setdiff(field_names, names(Value))
  names(Value) <- field_names
  if (length(DIFF) > 0) message("Info: data.frame colnames have been converted to align with Athena DDL naming convertions: \n",paste0(DIFF, collapse= ",\n"))
  # get R col types
  col_types <- sapply(Value, class)
  
  # preprosing proxict format
  posixct_cols <- names(Value)[sapply(col_types, function(x) "POSIXct" %in% x)]
  # create timestamp in athena format: https://docs.aws.amazon.com/athena/latest/ug/data-types.html
  for (col in posixct_cols) set(Value, j=col, value=strftime(Value[[col]], format="%Y-%m-%d %H:%M:%OS3"))
  
  # preprocessing list format
  list_cols <- names(Value)[sapply(col_types, function(x) "list" %in% x)]
  for (col in list_cols) set(Value, j=col, value=sapply(Value[[col]], paste, collapse = "|"))
  
  # handle special characters in character and factor column types
  special_char <- names(Value)[col_types %in% c("character", "factor")]
  switch(file.type,
         csv = {# changed special character from "," to "." to avoid issue with parsing delimited files
           for (col in special_char) set(Value, j=col, value=gsub("," , "\\.", Value[[col]]))
           message("Info: Special character \",\" has been converted to \".\" to help with Athena reading file format csv")},
         tsv = {# changed special character from "\t" to " " to avoid issue with parsing delimited files
           for (col in special_char) set(Value, j=col, value=gsub("\t" , " ", Value[[col]]))
           message("Info: Special characters \"\\t\" has been converted to \" \" to help with Athena reading file format tsv")})
  
  Value
})

#' Creates query to create a simple Athena table
#' 
#' Creates an interface to compose \code{CREATE EXTERNAL TABLE}.
#' @name sqlCreateTable
#' @inheritParams DBI::sqlCreateTable
#' @param field.types Additional field types used to override derived types.
#' @param partition Partition Athena table (needs to be a named list or vector) for example: \code{c(var1 = "2019-20-13")}
#' @param s3.location s3 bucket to store Athena table, must be set as a s3 uri for example ("s3://mybucket/data/"). 
#'        By default s3.location is set s3 staging directory from \code{\linkS4class{AthenaConnection}} object.
#' @param file.type What file type to store data.frame on s3, noctua currently supports ["tsv", "csv", "parquet", "json"]. Default delimited file type is "tsv", in previous versions
#'                  of \code{noctua (=< 1.4.0)} file type "csv" was used as default. The reason for the change is that columns containing \code{Array/JSON} format cannot be written to 
#'                  Athena due to the separating value ",". This would cause issues with AWS Athena. 
#'                  \strong{Note:} "parquet" format is supported by the \code{arrow} package and it will need to be installed to utilise the "parquet" format.
#'                  "json" format is supported by \code{jsonlite} package and it will need to be installed to utilise the "json" format.
#' @param compress \code{FALSE | TRUE} To determine if to compress file.type. If file type is ["csv", "tsv"] then "gzip" compression is used, for file type "parquet" 
#'                 "snappy" compression is used. Currently \code{noctua} doesn't support compression for "json" file type.
#' @return \code{sqlCreateTable} returns data.frame's \code{DDL} in the \code{\link[DBI]{SQL}} format.
#' @seealso \code{\link[DBI]{sqlCreateTable}}
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
#' # Create DDL for iris data.frame
#' sqlCreateTable(con, "iris", iris, s3.location = "s3://path/to/athena/table")
#' 
#' # Create DDL for iris data.frame with partition
#' sqlCreateTable(con, "iris", iris, 
#'                partition = "timestamp",
#'                s3.location = "s3://path/to/athena/table")
#'                
#' # Create DDL for iris data.frame with partition and file.type parquet
#' sqlCreateTable(con, "iris", iris, 
#'                partition = "timestamp",
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
  function(con, table, fields, field.types = NULL, partition = NULL, s3.location= NULL, file.type = c("tsv", "csv", "parquet", "json"),
           compress = FALSE, ...){
    if (!dbIsValid(con)) {stop("Connection already closed.", call. = FALSE)}
    stopifnot(is.character(table),
              is.data.frame(fields),
              is.null(field.types) || is.character(field.types),
              is.null(partition) || is.character(partition) || is.list(partition),
              is.null(s3.location) || is.s3_uri(s3.location),
              is.logical(compress))
    
    field <- createFields(con, fields = fields, field.types = field.types)
    file.type <- match.arg(file.type)
    
    # use default s3_staging directory is s3.location isn't provided
    if (is.null(s3.location)) s3.location <- con@info$s3_staging
    
    if (grepl("\\.", table)) {
      schema <- gsub("\\..*", "" , table)
      table1 <- gsub(".*\\.", "" , table)
    } else {
      schema <- con@info$dbms.name
      table1 <- table}
    
    table <- paste0(quote_identifier(con,  c(schema,table1)), collapse = ".")
    
    s3.location <- gsub("/$", "", s3.location)
    if(grepl(table1, s3.location)){s3.location <- gsub(paste0("/", table1,"$"), "", s3.location)}
    s3.location <- paste0("'",s3.location,"/",schema,"/", table1,"/'")
    SQL(paste0(
      "CREATE EXTERNAL TABLE ", table, " (\n",
      "  ", paste(field, collapse = ",\n  "), "\n)\n",
      partitioned(con, partition),
      FileType(file.type), "\n",
      "LOCATION ",s3.location, "\n",
      header(file.type, compress)
    ))
  }
)

# Helper functions: fields
createFields <- function(con, fields, field.types) {
  if (is.data.frame(fields)) {
    fields <- vapply(fields, function(x) DBI::dbDataType(con, x), character(1))
  }
  if (!is.null(field.types)) {
    names(field.types) <- gsub("\\.", "_", make.names(names(field.types), unique = TRUE))
    fields[names(field.types)] <- field.types
  }
  
  field_names <- gsub("\\.", "_", make.names(names(fields), unique = TRUE))
  DIFF <- setdiff(field_names, names(fields))
  if (length(DIFF) > 0) message("Info: data.frame colnames have been converted to align with Athena DDL naming convertions: \n",paste0(DIFF, collapse= ",\n"))
  
  field_names <- quote_identifier(con, field_names)
  field.types <- unname(fields)
  paste0(field_names, " ", field.types)
}

# Helper function partition
partitioned <- function(con, obj = NULL){
  if(!is.null(obj)) {
    obj <- paste(quote_identifier(con, obj), "STRING", collapse = ", ")
    paste0("PARTITIONED BY (", obj, ")\n") }
}

FileType <- function(obj){
  switch(obj,
         csv = gsub("_","","ROW FORMAT DELIMITED\n\tFIELDS TERMINATED BY ','\n\tLINES TERMINATED BY '\\_n'"),
         tsv = gsub("_","","ROW FORMAT DELIMITED\n\tFIELDS TERMINATED BY '\t'\n\tLINES TERMINATED BY '\\_n'"),
         parquet = SQL("STORED AS PARQUET"),
         json = SQL("ROW FORMAT  serde 'org.apache.hive.hcatalog.data.JsonSerDe'"))
}

header <- function(obj, compress){
  compress <- if(!compress) "" else{switch(obj,
                                           csv = ",\n\t\t'compressionType'='gzip'",
                                           tsv = ",\n\t\t'compressionType'='gzip'",
                                           parquet = 'tblproperties ("parquet.compress"="SNAPPY")'
  )}
  switch(obj,
         csv = paste0('TBLPROPERTIES ("skip.header.line.count"="1"',compress,');'),
         tsv = paste0('TBLPROPERTIES ("skip.header.line.count"="1"',compress,');'),
         parquet = paste0(compress,";"))
}

Compress <- function(file.type, compress){
  if(compress){
    switch(file.type,
           "csv" = paste(file.type, "gz", sep = "."),
           "tsv" = paste(file.type, "gz", sep = "."),
           "parquet" = paste("snappy", file.type, sep = "."),
           "json" = {message("Info: json format currently doesn't support compression")
             file.type})
  } else {file.type}
}

# helper function to format column and table name
quote_identifier <- function(conn, x, ...) {
  if (length(x) == 0L) {
    return(DBI::SQL(character()))
  }
  if (any(is.na(x))) {
    stop("Cannot pass NA to sqlCreateTable()", call. = FALSE)
  }
  if (nzchar(conn@quote)) {
    x <- gsub(conn@quote, paste0(conn@quote, conn@quote), x, fixed = TRUE)
  }
  DBI::SQL(paste(conn@quote, encodeString(x), conn@quote, sep = ""))
}

# moved s3 component builder to separate helper function to allow for unit tests
s3_upload_location <- function(x, 
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
  FileType <- if(compress) Compress(file.type, compress) else file.type
  
  # Upload file with uuid to allow appending to same s3 location 
  FileName <- paste(uuid::UUIDgenerate(n = length(x)),  FileType, sep = ".")
  
  # s3 bucket and key split
  s3_info <- split_s3_uri(s3.location)
  s3_info$key <- gsub("/$", "", s3_info$key)
  
  # Append data to existing s3 location
  if(append) {return(list(s3_info$bucket,
                          paste(s3_info$key, partition, FileName, sep = "/")))}
  
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
  list(s3_info$bucket,
       sprintf("%s%s%s%s%s", s3_info$key, schema, name, partition, FileName))
}

# repair table using MSCK REPAIR TABLE for non partitioned and ALTER TABLE for partitioned tables
repair_table <- function(con, name, partition = NULL, s3.location = NULL, append = FALSE){
  if (grepl("\\.", name)) {
    schema <- gsub("\\..*", "" , name)
    table1 <- gsub(".*\\.", "" , name)
  } else {
    schema <- con@info$dbms.name
    table1 <- name}
  
  # format table name for special characters
  table <- paste0(quote_identifier(con,  c(schema,table1)), collapse = ".")
  
  if (is.null(partition)){
    query <- SQL(paste0("MSCK REPAIR TABLE ", table))
    res <- dbExecute(con, query)
    dbClearResult(res)
  } else {
    # formatting s3 partitions
    s3_partition <- unlist(partition)
    s3_partition <- paste(names(s3_partition), unname(s3_partition), sep = "=", collapse = "/")
    
    # s3 bucket and key split
    s3_info <- split_s3_uri(s3.location)
    s3_info$key <- gsub("/$", "", s3_info$key)
    
    # Append data to existing s3 location
    if(append) {s3.location <- sprintf("s3://%s/%s/%s/", s3_info$bucket, s3_info$key, s3_partition)
    } else {
      if (s3_partition != "") s3_partition <- paste0(s3_partition, "/")
      split_key <- unlist(strsplit(s3_info$key,"/"))
      
      # remove name from s3 key
      if(split_key[length(split_key)] == table1 || length(split_key) == 0)  split_key <- split_key[-length(split_key)]
      
      # remove schema from s3 key
      if(any(schema == split_key))  split_key <- split_key[-which(schema == split_key)]
      
      s3_info$key <- paste(split_key, collapse = "/")
      if (s3_info$key != "") s3_info$key <- paste0(s3_info$key, "/")
      
      # s3 folder
      schema <- paste0(schema, "/")
      table1 <- paste0(table1, "/")
      
      # S3 new syntax #73
      s3.location <- sprintf("s3://%s/%s%s%s%s", s3_info$bucket, s3_info$key, schema, table1, s3_partition)
    }
    
    partition_names <- quote_identifier(con, names(partition))
    partition <- dbQuoteString(con, partition)
    partition <- paste0(partition_names, " = ", partition, collapse = ", ")
    s3.location <- dbQuoteString(con, s3.location)
    
    query <- SQL(paste0("ALTER TABLE ", table, " ADD IF NOT EXISTS\nPARTITION (", partition, ")\nLOCATION ", s3.location))
    res <- dbSendQuery(con, query)
    poll_result <- poll(res)
    dbClearResult(res)
    # If query failed, due to glue permissions default back to msck repair table
    if(poll_result$QueryExecution$Status$State == "FAILED" && grepl(".*glue.*BatchCreatePartition.*AccessDeniedException", poll_result$QueryExecution$Status$StateChangeReason)) {
      query <- SQL(paste0("MSCK REPAIR TABLE ", table))
      res <- dbExecute(con, query)
      dbClearResult(res)
    } else if (poll_result$QueryExecution$Status$State == "FAILED") stop(poll_result$QueryExecution$Status$StateChangeReason, call. = FALSE)
    
  }
}
