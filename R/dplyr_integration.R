#' @include utils.R

######################################################################
# dplyr generic
######################################################################

#' @title S3 implementation of \code{db_compute} for Athena
#'
#' @description This is a backend function for dplyr's \code{compute} function. Users won't be required to access and run this function.
#' @param con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
#' @param table Table name, if left default noctua will use the default from \code{dplyr}'s \code{compute} function.
#' @param name Table name, if left default noctua will use the default from \code{dplyr}'s \code{compute} function.
#' @param sql SQL code to be sent to the data
#' @param ... passes \code{noctua} table creation parameters: [\code{file_type},\code{s3_location},\code{partition}]
#' @param overwrite Allows overwriting the destination table. Cannot be \code{TRUE} if \code{append} is also \code{TRUE}.
#' @param temporary if TRUE, will create a temporary table that is local to this connection and will be automatically deleted when the connection expires
#' @param unique_indexes a list of character vectors. Each element of the list will create a new unique index over the specified column(s). Duplicate rows will result in failure.
#' @param indexes a list of character vectors. Each element of the list will create a new index.
#' @param analyze if TRUE (the default), will automatically ANALYZE the new table so that the query optimiser has useful information.
#' @param in_transaction Should the table creation be wrapped in a transaction? This typically makes things faster, but you may want to suppress if the database doesn't support transactions, or you're wrapping in a transaction higher up (and your database doesn't support nested transactions.)
#' @param partition Partition Athena table (needs to be a named list or vector) for example: \code{c(var1 = "2019-20-13")}
#' @param s3_location s3 bucket to store Athena table, must be set as a s3 uri for example ("s3://mybucket/data/")
#' @param file_type What file type to store data.frame on s3, noctua currently supports ["tsv", "csv", "parquet"]. Default delimited file type is "tsv", in previous versions
#'                  of \code{noctua (=< 1.4.0)} file type "csv" was used as default. The reason for the change is that columns containing \code{Array/JSON} format cannot be written to
#'                  Athena due to the separating value ",". This would cause issues with AWS Athena.
#'                  \strong{Note:} "parquet" format is supported by the \code{arrow} package and it will need to be installed to utilise the "parquet" format.
#' @param compress \code{FALSE | TRUE} To determine if to compress file.type. If file type is ["csv", "tsv"] then "gzip" compression is used, for file type "parquet"
#'                 "snappy" compression is used.
#' @param with An optional WITH clause for the CREATE TABLE statement.
#' \itemize{
#'          \item{\code{file_type:} What file type to store data.frame on s3, noctua currently supports ["NULL","csv", "parquet", "json"].
#'                        \code{"NULL"} will let Athena set the file_type for you.}
#'          \item{\code{s3_location:} s3 bucket to store Athena table, must be set as a s3 uri for example ("s3://mybucket/data/")}
#'          \item{\code{partition:} Partition Athena table, requires to be a partitioned variable from previous table.}}
#' @name db_compute
#' @return
#' \code{db_compute} returns table name
#' @seealso \code{\link{AthenaWriteTables}} \code{\link{backend_dbplyr_v2}} \code{\link{backend_dbplyr_v1}}
#' @examples
#' \dontrun{
#' # Note:
#' # - Require AWS Account to run below example.
#' # - Different connection methods can be used please see `noctua::dbConnect` documentation
#'
#' library(DBI)
#' library(dplyr)
#'
#' # Demo connection to Athena using profile name
#' con <- dbConnect(noctua::athena())
#'
#' # Write data.frame to Athena table
#' copy_to(con, mtcars,
#'   s3_location = "s3://mybucket/data/"
#' )
#'
#' # Write Athena table from tbl_sql
#' athena_mtcars <- tbl(con, "mtcars")
#' mtcars_filter <- athena_mtcars %>% filter(gear >= 4)
#'
#' # create athena with unique table name
#' mtcars_filer %>%
#'   compute()
#'
#' # create athena with specified name and s3 location
#' mtcars_filer %>%
#'   compute("mtcars_filer",
#'     s3_location = "s3://mybucket/mtcars_filer/"
#'   )
#'
#' # Disconnect from Athena
#' dbDisconnect(con)
#' }
#' @export
db_compute.AthenaConnection <- function(con, table, sql, ..., 
                                        overwrite = FALSE,
                                        temporary = FALSE,
                                        unique_indexes = list(),
                                        indexes = list(),
                                        analyze = TRUE,
                                        in_transaction = FALSE,
                                        partition = NULL,
                                        s3_location = NULL,
                                        file_type = c("csv", "tsv", "parquet"),
                                        compress = FALSE) {
  if (isTRUE(temporary)) {
    stop(
      "Temporary table is not supported. ",
      "Use temporary = FALSE to create a permanent table.",
      call. = FALSE
    )
  }
  
  file_type = match.arg(file_type)
  if (athena_unload()) {
    stop(
      "Unable to create table when `noctua_options(unload = TRUE)`. Please run `noctua_options(unload = FALSE)` and try again.",
      call. = FALSE
    )
  }
  with <- ctas_sql_with(partition, s3_location, file_type, compress)
  if (dbplyr_env$version > "2.3.3") {
    db_save_query <- pkg_method("db_save_query", "dplyr")
    
    db_save_query(con, sql, table, temporary = temporary,
      overwrite = overwrite, with = with,
      ...
    )
  } else {
    in_schema <- pkg_method("in_schema", "dbplyr")
    sql <- athena_query_save(
      con=con, sql=sql, name=table, with, ...
    )
    ll <- db_detect(conn=con, name=table)
    in_schema(ll[["dbms.name"]], ll[["table"]])
  }
}

athena_query_save <- function(con, sql, name, with,
                              ...) {
  name <- paste0('"', strsplit(name, "\\.")[[1]], '"', collapse = ".")
  sql <- SQL(paste("CREATE TABLE", name, with, "AS", sql, ";"))
  res <- dbExecute(con, sql, unload = FALSE)
  dbClearResult(res)
  return(name)
}

#' @rdname db_compute
#' @export
sql_query_save.AthenaConnection <- function(
    con, sql, name, temporary = TRUE, with, ...) {
  as_from <- pkg_method("as_from", "dbplyr")
  as_table_ident <- pkg_method("as_table_ident", "dbplyr")
  glue_sql2 <- pkg_method("glue_sql2", "dbplyr")
  
  sql <- as_from(sql)
  name <- as_table_ident(name)
  
  sql <- glue_sql2(con, "CREATE ", if (temporary) sql("TEMPORARY "),
    "TABLE \n", "{.tbl {name}}\n{with}AS\n", "{.from sql}")
  return(sql)
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
#' @param temporary if TRUE, will create a temporary table that is local to this connection and will be automatically deleted when the connection expires
#' @param unique_indexes a list of character vectors. Each element of the list will create a new unique index over the specified column(s). Duplicate rows will result in failure.
#' @param indexes a list of character vectors. Each element of the list will create a new index.
#' @param analyze if TRUE (the default), will automatically ANALYZE the new table so that the query optimiser has useful information.
#' @param in_transaction Should the table creation be wrapped in a transaction? This typically makes things faster, but you may want to suppress if the database doesn't support transactions, or you're wrapping in a transaction higher up (and your database doesn't support nested transactions.)
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
#'   s3_location = "s3://mybucket/data/"
#' )
#'
#' # Checking if uploaded table exists in Athena
#' dbExistsTable(con, "mtcars")
#'
#' # Write Athena table from tbl_sql
#' athena_mtcars <- tbl(con, "mtcars")
#' mtcars_filter <- athena_mtcars %>% filter(gear >= 4)
#'
#' copy_to(con, mtcars_filter)
#'
#' # Checking if uploaded table exists in Athena
#' dbExistsTable(con, "mtcars_filter")
#'
#' # Disconnect from Athena
#' dbDisconnect(con)
#' }
#' @export
db_copy_to.AthenaConnection <- function(
    con, table, values, ..., partition = NULL,
    s3_location = NULL, file_type = c("csv", "tsv", "parquet"),
    compress = FALSE, max_batch = Inf, overwrite = FALSE, append = FALSE, types = NULL,
    temporary = TRUE, unique_indexes = NULL, indexes = NULL,
    analyze = TRUE, in_transaction = FALSE) {
  types <- types %||% dbDataType(con, values)
  names(types) <- names(values)
  file_type <- match.arg(file_type)

  if (dbplyr_env$version > "2.3.3") {
    dbplyr_db_copy_to <- pkg_method("db_copy_to.DBIConnection", "dbplyr")

    table <- dbplyr_db_copy_to(
      con = con,
      table = table,
      values = values,
      ...,
      partition = partition,
      s3.location = s3_location,
      file.type = file_type,
      compress = compress,
      max.batch = max_batch,
      overwrite = overwrite,
      append = append,
      types = types,
      temporary = temporary,
      unique_indexes = unique_indexes,
      indexes = indexes,
      analyze = analyze,
      in_transaction = in_transaction
    )
  } else {
    dbWriteTable(
      conn = con, name = table, value = values,
      overwrite = overwrite, append = append,
      field.types = types, partition = partition,
      s3.location = s3_location, file.type = file_type,
      compress = compress,
      max.batch = max_batch
    )
  }
  return(table)
}

sql_table_analyze.AthenaConnection <- function(con, table, ...) {
  NULL
}

######################################################################
# dplyr v2 api support
######################################################################

#' Declare which version of dbplyr API is being called.
#'
#' @param con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
#' @name dbplyr_edition
#' @return
#' Integer for which version of `dbplyr` is going to be used.
#' @export
dbplyr_edition.AthenaConnection <- function(con) 2L

#' S3 implementation of \code{db_connection_describe} for Athena (api version 2).
#'
#' This is a backend function for dplyr to retrieve meta data about Athena queries. Users won't be required to access and run this function.
#' @param con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
#' @name db_connection_describe
#' @return
#' Character variable containing Meta Data about query sent to Athena. The Meta Data is returned in the following format:
#'
#' \code{"Athena <paws version> [<profile_name>@region/database]"}
NULL

athena_conn_desc <- function(con) {
  info <- dbGetInfo(con)
  profile <- if (!is.null(info$profile_name)) paste0(info$profile_name, "@")
  paste0("Athena ", info$paws, " [", profile, info$region_name, "/", info$dbms.name, "]")
}

#' @rdname db_connection_describe
db_connection_describe.AthenaConnection <- function(con) {
  athena_conn_desc(con)
}

#' Athena S3 implementation of dbplyr backend functions (api version 2).
#'
#' These functions are used to build the different types of SQL queries.
#' The AWS Athena implementation give extra parameters to allow access the to standard DBI Athena methods. They also
#' utilise AWS Glue to speed up sql query execution.
#' @param con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
#' @param sql SQL code to be sent to AWS Athena
#' @param x R object to be transformed into athena equivalent
#' @param format returning format for explain queries, default set to `"text"`. Other formats can be found: \url{https://docs.aws.amazon.com/athena/latest/ug/athena-explain-statement.html}
#' @param type return plan for explain queries, default set to `NULL`. Other type can be found: \url{https://docs.aws.amazon.com/athena/latest/ug/athena-explain-statement.html}
#' @param ... other parameters, currently not implemented
#' @name backend_dbplyr_v2
#' @return
#' \describe{
#' \item{sql_query_explain}{Returns sql query for \href{https://docs.aws.amazon.com/athena/latest/ug/athena-explain-statement.html}{AWS Athena explain statement}}
#' \item{sql_query_fields}{Returns sql query column names}
#' \item{sql_escape_date}{Returns sql escaping from dates}
#' \item{sql_escape_datetime}{Returns sql escaping from date times}
#' }
NULL

athena_explain <- function(con, sql, format = "text", type = NULL, ...) {
  if (athena_unload()) {
    stop(
      "Unable to explain query plan when `noctua_options(unload = TRUE)`. Please run `noctua_options(unload = FALSE)` and try again.",
      call. = FALSE
    )
  }
  # AWS Athena now supports explain: https://docs.aws.amazon.com/athena/latest/ug/athena-explain-statement.html
  format <- match.arg(format, c("text", "json"))
  if (!is.null(type)) {
    type <- match.arg(type, c("LOGICAL", "DISTRIBUTED", "VALIDATE", "IO"))
    format <- NULL
  }
  build_sql <- pkg_method("build_sql", "dbplyr")
  dplyr_sql <- pkg_method("sql", "dbplyr")

  build_sql(
    "EXPLAIN ",
    if (!is.null(format)) dplyr_sql(paste0("(FORMAT ", format, ") ")),
    if (!is.null(type)) dplyr_sql(paste0("(TYPE ", type, ") ")),
    dplyr_sql(sql),
    con = con
  )
}

#' @rdname backend_dbplyr_v2
sql_query_explain.AthenaConnection <- athena_explain

#' @rdname backend_dbplyr_v2
sql_query_fields.AthenaConnection <- function(con, sql, ...) {
  # pass ident class to dbGetQuery to continue same functionality as dbplyr v1 api.
  if (inherits(sql, "ident")) {
    return(sql)
  } else {
    # None ident class uses dbplyr:::sql_query_fields.DBIConnection method
    dbplyr_query_select <- pkg_method("dbplyr_query_select", "dbplyr")
    sql_subquery <- pkg_method("sql_subquery", "dplyr")
    dplyr_sql <- pkg_method("sql", "dplyr")

    return(dbplyr_query_select(con, dplyr_sql("*"), sql_subquery(con, sql), where = dplyr_sql("0 = 1")))
  }
}

#' @rdname backend_dbplyr_v2
#' @export
sql_escape_date.AthenaConnection <- function(con, x) {
  dbQuoteString(con, x)
}

#' @rdname backend_dbplyr_v2
#' @export
sql_escape_datetime.AthenaConnection <- function(con, x) {
  str <- dbQuoteString(con, x)
  return(gsub("^date ", "timestamp ", str))
}

######################################################################
# dplyr v1 api support
######################################################################

#' S3 implementation of \code{db_desc} for Athena (api version 1).
#'
#' This is a backend function for dplyr to retrieve meta data about Athena queries. Users won't be required to access and run this function.
#' @param x A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
#' @name db_desc
#' @return
#' Character variable containing Meta Data about query sent to Athena. The Meta Data is returned in the following format:
#'
#' \code{"Athena <paws version> [<profile_name>@region/database]"}
db_desc.AthenaConnection <- function(x) {
  return(athena_conn_desc(x))
}

#' Athena S3 implementation of dbplyr backend functions (api version 1).
#'
#' These functions are used to build the different types of SQL queries.
#' The AWS Athena implementation give extra parameters to allow access the to standard DBI Athena methods. They also
#' utilise AWS Glue to speed up sql query execution.
#' @param con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
#' @param sql SQL code to be sent to AWS Athena
#' @param ... other parameters, currently not implemented
#' @name backend_dbplyr_v1
#' @return
#' \describe{
#' \item{db_explain}{Returns \href{https://docs.aws.amazon.com/athena/latest/ug/athena-explain-statement.html}{AWS Athena explain statement}}
#' \item{db_query_fields}{Returns sql query column names}
#' }

#' @rdname backend_dbplyr_v1
db_explain.AthenaConnection <- function(con, sql, ...) {
  sql <- athena_explain(con, sql, ...)
  expl <- dbGetQuery(con, sql, unload = FALSE)
  out <- utils::capture.output(print(expl))
  paste(out, collapse = "\n")
}

# NOTE: dbplyr v2 integration has to use this in dbGetQuery
athena_query_fields_ident <- function(con, sql) {
  if (str_count(sql, "\\.") < 2) {
    ll <- db_detect(con, gsub('"', "", sql))

    # If dbplyr schema, get the fields from Glue
    tryCatch(
      output <- con@ptr$Athena$get_table_metadata(
        CatalogName = ll[["db.catalog"]],
        DatabaseName = ll[["dbms.name"]],
        TableName = ll[["table"]]
      )$TableMetadata
    )
    col_names <- vapply(output$Columns, function(y) y$Name, FUN.VALUE = character(1))
    partitions <- vapply(output$PartitionKeys, function(y) y$Name, FUN.VALUE = character(1))

    return(c(col_names, partitions))
  } else {
    # If a subquery, query Athena for the fields
    # return dplyr methods
    sql_select <- pkg_method("sql_select", "dplyr")
    sql_subquery <- pkg_method("sql_subquery", "dplyr")
    dplyr_sql <- pkg_method("sql", "dplyr")

    sql <- sql_select(con, dplyr_sql("*"), sql_subquery(con, sql), where = dplyr_sql("0 = 1"))
    qry <- dbSendQuery(con, sql)
    on.exit(dbClearResult(qry))

    res <- dbFetch(qry, 0)
    return(names(res))
  }
}

#' @rdname backend_dbplyr_v1
db_query_fields.AthenaConnection <- function(con, sql, ...) {
  # check if sql is dbplyr schema
  if (inherits(sql, "ident")) {
    return(athena_query_fields_ident(con, sql))
  } else {
    # If a subquery, query Athena for the fields
    # return dplyr methods
    sql_select <- pkg_method("sql_select", "dplyr")
    sql_subquery <- pkg_method("sql_subquery", "dplyr")
    dplyr_sql <- pkg_method("sql", "dplyr")

    sql <- sql_select(con, dplyr_sql("*"), sql_subquery(con, sql), where = dplyr_sql("0 = 1"))
    qry <- dbSendQuery(con, sql)
    on.exit(dbClearResult(qry))

    res <- dbFetch(qry, 0)
    return(names(res))
  }
}
