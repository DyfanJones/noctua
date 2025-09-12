#' @include Driver.R
#' @include dplyr_integration.R
NULL

#' @title Athena Connection Methods
#'
#' @description Implementations of pure virtual functions defined in the `DBI` package
#' for AthenaConnection objects.
#' @slot ptr a list of connecting objects from the SDK paws package.
#' @slot info a list of metadata objects
#' @slot quote syntax to quote sql query when creating Athena ddl
#' @name AthenaConnection
#' @inheritParams methods::show
#' @importFrom utils modifyList
NULL

class_cache <- new.env(parent = emptyenv())

AthenaConnection <- function(
  aws_access_key_id = NULL,
  aws_secret_access_key = NULL,
  aws_session_token = NULL,
  catalog_name = NULL,
  schema_name = NULL,
  work_group = NULL,
  poll_interval = NULL,
  encryption_option = NULL,
  kms_key = NULL,
  s3_staging_dir = NULL,
  region_name = NULL,
  profile_name = NULL,
  aws_expiration = NULL,
  keyboard_interrupt = NULL,
  endpoint_override = NULL,
  ...
) {
  kwargs <- list(...)
  # get lower level paws methods
  get_region <- pkg_method("get_region", "paws.common")
  get_profile_name <- pkg_method("get_profile_name", "paws.common")

  # get region name
  RegionName <- (region_name %||% get_region(profile_name)) %||%
    get_aws_env("AWS_DEFAULT_REGION")

  # get profile_name
  prof_name <- if (
    !(is.null(aws_access_key_id) ||
      is.null(aws_secret_access_key) ||
      is.null(aws_session_token))
  ) {
    NULL
  } else {
    get_profile_name(profile_name)
  }

  # format credentials to pass to paws sdk
  Config <- cred_set(
    aws_access_key_id,
    aws_secret_access_key,
    aws_session_token,
    prof_name,
    RegionName
  )
  if (!is.null(kwargs$endpoint)) {
    stop(
      "Please use `endpoint_override` to override AWS service endpoints.",
      call. = F
    )
  }
  # set up any endpoint url for each aws service: athena, s3, glue
  endpoints <- set_endpoints(endpoint_override)

  tryCatch({
    Athena <- paws::athena(
      config = modifyList(Config, c(kwargs, list(endpoint = endpoints$athena)))
    )
    S3 <- paws::s3(
      config = modifyList(Config, c(kwargs, list(endpoint = endpoints$s3)))
    )
    glue <- paws::glue(
      config = modifyList(Config, c(kwargs, list(endpoint = endpoints$glue)))
    )
  })

  if (is.null(s3_staging_dir) && !is.null(work_group)) {
    tryCatch(
      s3_staging_dir <- Athena$get_work_group(
        WorkGroup = work_group
      )$WorkGroup$Configuration$ResultConfiguration$OutputLocation
    )
  }
  # return a subset of api function to reduce object size
  ptr_ll <- list(
    Athena = Athena[c(
      ".internal",
      "start_query_execution",
      "get_query_execution",
      "stop_query_execution",
      "get_query_results",
      "get_work_group",
      "list_work_groups",
      "list_data_catalogs",
      "list_databases",
      "get_table_metadata",
      "update_work_group",
      "create_work_group",
      "delete_work_group"
    )],
    S3 = S3[c(
      ".internal",
      "put_object",
      "get_object",
      "download_file",
      "delete_object",
      "delete_objects",
      "list_objects_v2"
    )]
  )
  s3_staging_dir <- s3_staging_dir %||% get_aws_env("AWS_ATHENA_S3_STAGING_DIR")

  if (is.null(s3_staging_dir)) {
    stop(
      "Please set `s3_staging_dir` either in parameter `s3_staging_dir`, environmental varaible `AWS_ATHENA_S3_STAGING_DIR`",
      "or when work_group is defined in `create_work_group()`",
      call. = F
    )
  }
  info <- list(
    profile_name = prof_name,
    s3_staging = s3_staging_dir,
    db.catalog = catalog_name,
    dbms.name = schema_name,
    work_group = work_group %||% "primary",
    poll_interval = poll_interval,
    encryption_option = encryption_option,
    kms_key = kms_key,
    expiration = aws_expiration,
    timezone = character(),
    keyboard_interrupt = keyboard_interrupt,
    region_name = RegionName,
    endpoint_override = endpoints
  )
  res <- new(
    "AthenaConnection",
    ptr = list2env(ptr_ll, parent = emptyenv()),
    info = list2env(info, parent = emptyenv()),
    quote = "`"
  )
}

#' @rdname AthenaConnection
#' @export
setClass(
  "AthenaConnection",
  contains = "DBIConnection",
  slots = list(
    ptr = "environment",
    info = "environment",
    quote = "character"
  )
)

#' @rdname AthenaConnection
#' @export
setMethod(
  "show",
  "AthenaConnection",
  function(object) {
    cat("<AthenaConnection>\n")
    if (!dbIsValid(object)) {
      cat("  DISCONNECTED\n")
    }
  }
)

#' @rdname AthenaConnection
#' @inheritParams DBI::dbDisconnect
#' @param conn A [DBI::DBIConnection][DBI::DBIConnection-class] object,
#' as returned by [dbConnect()][DBI::dbConnect].
#' @export
setMethod(
  "dbDisconnect",
  "AthenaConnection",
  function(conn, ...) {
    if (!dbIsValid(conn)) {
      warning("Connection already closed.", call. = FALSE)
    } else {
      on_connection_closed(conn)
      rm(list = ls(all.names = TRUE, envir = conn@ptr), envir = conn@ptr)
    }
    invisible(NULL)
  }
)

#' @rdname AthenaConnection
#' @param dbObj An object inheriting from `DBIObject`, i.e. `DBIDriver`,
#' `DBIConnection`, or a `DBIResult`.
#' @inheritParams DBI::dbIsValid
#' @export
setMethod(
  "dbIsValid",
  "AthenaConnection",
  function(dbObj, ...) {
    resource_active(dbObj)
  }
)

#' @rdname AthenaConnection
#' @inheritParams DBI::dbSendQuery
#' @param unload boolean input to modify `statement` to align with \href{https://docs.aws.amazon.com/athena/latest/ug/unload.html}{AWS Athena UNLOAD},
#'              default is set to \code{FALSE}.
#' @export
setMethod(
  "dbSendQuery",
  c("AthenaConnection", "character"),
  function(conn, statement, unload = athena_unload(), ...) {
    con_error_msg(conn, msg = "Connection already closed.")
    stopifnot(is.logical(unload))
    res <- AthenaResult(
      conn = conn,
      statement = statement,
      s3_staging_dir = conn@info$s3_staging,
      unload = unload
    )
    return(res)
  }
)

#' @rdname AthenaConnection
#' @inheritParams DBI::dbSendStatement
#' @export
setMethod(
  "dbSendStatement",
  c("AthenaConnection", "character"),
  function(conn, statement, unload = athena_unload(), ...) {
    con_error_msg(conn, msg = "Connection already closed.")
    stopifnot(is.logical(unload))
    res <- AthenaResult(
      conn = conn,
      statement = statement,
      s3_staging_dir = conn@info$s3_staging,
      unload = unload
    )
    return(res)
  }
)

#' @rdname AthenaConnection
#' @inheritParams DBI::dbExecute
#' @export
setMethod(
  "dbExecute",
  c("AthenaConnection", "character"),
  function(conn, statement, unload = athena_unload(), ...) {
    con_error_msg(conn, msg = "Connection already closed.")
    stopifnot(is.logical(unload))
    res <- AthenaResult(
      conn = conn,
      statement = statement,
      s3_staging_dir = conn@info$s3_staging,
      unload = unload
    )
    poll(res)

    # if query failed stop
    if (res@info$Status == "FAILED") {
      stop(res@info$StateChangeReason, call. = FALSE)
    }

    # cache query metadata if caching is enabled
    if (athena_option_env$cache_size > 0) {
      cache_query(res)
    }

    return(res)
  }
)

#' @rdname AthenaConnection
#' @inheritParams DBI::dbDataType
#' @export
setMethod("dbDataType", "AthenaConnection", function(dbObj, obj, ...) {
  dbDataType(athena(), obj, ...)
})


#' @rdname AthenaConnection
#' @inheritParams DBI::dbDataType
#' @export
setMethod(
  "dbDataType",
  c("AthenaConnection", "data.frame"),
  function(dbObj, obj, ...) {
    vapply(obj, AthenaDataType, FUN.VALUE = character(1), USE.NAMES = TRUE)
  }
)


# import DBI quote_string method
dbi_quote <- methods::getMethod(
  "dbQuoteString",
  c("DBIConnection", "character"),
  asNamespace("DBI")
)

detect_date <- function(x, try_format = c("%Y-%m-%d", "%Y/%m/%d")) {
  return(
    all_dates = all(try(as.Date(x, tryFormats = try_format), silent = T) == x) &
      all(nchar(x) == 10)
  )
}

detect_date_time <- function(x) {
  timestamp_fmt <- c(
    "%Y-%m-%d %H:%M:%OS",
    "%Y/%m/%d %H:%M:%OS",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M"
  )
  return(all(try(as.POSIXct(x, tryFormats = timestamp_fmt), silent = T) == x))
}

#' @rdname AthenaConnection
#' @inheritParams DBI::dbQuoteString
#' @export
setMethod(
  "dbQuoteString",
  c("AthenaConnection", "character"),
  function(conn, x, ...) {
    if (identical(dbplyr_env$major, 2L)) {
      all_ts <- detect_date_time(x)
      all_dates <- detect_date(x)
      if (all_dates & !is.na(all_dates)) {
        return(paste0("date ", dbi_quote(conn, strftime(x, "%Y-%m-%d"), ...)))
      } else if (all_ts & !is.na(all_ts)) {
        return(paste0(
          "timestamp ",
          dbi_quote(conn, strftime(x, "%Y-%m-%d %H:%M:%OS3"), ...)
        ))
      }
    }
    return(dbi_quote(conn, x, ...))
  }
)

#' @rdname AthenaConnection
#' @inheritParams DBI::dbQuoteString
#' @export
setMethod(
  "dbQuoteString",
  c("AthenaConnection", "POSIXct"),
  function(conn, x, ...) {
    x <- strftime(x, "%Y-%m-%d %H:%M:%OS3")
    paste0("timestamp ", dbi_quote(conn, x, ...))
  }
)

#' @rdname AthenaConnection
#' @inheritParams DBI::dbQuoteString
#' @export
setMethod(
  "dbQuoteString",
  c("AthenaConnection", "Date"),
  function(conn, x, ...) {
    paste0("date ", dbi_quote(conn, strftime(x, "%Y-%m-%d"), ...))
  }
)

#' @rdname AthenaConnection
#' @inheritParams DBI::dbQuoteString
#' @export
setMethod(
  "dbQuoteIdentifier",
  c("AthenaConnection", "SQL"),
  getMethod("dbQuoteIdentifier", c("DBIConnection", "SQL"), asNamespace("DBI"))
)

#' List Athena Tables
#'
#' Returns the unquoted names of Athena tables accessible through this connection.
#' @name dbListTables
#' @inheritParams DBI::dbListTables
#' @param conn A [DBI::DBIConnection][DBI::DBIConnection-class] object,
#' as returned by [dbConnect()][DBI::dbConnect].
#' @param catalog Athena catalog, default set to NULL to return all tables from all Athena catalogs
#' @param schema Athena schema, default set to NULL to return all tables from all Athena schemas.
#'               Note: The use of DATABASE and SCHEMA is interchangeable within Athena.
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
  "dbListTables",
  "AthenaConnection",
  function(conn, catalog = NULL, schema = NULL, ...) {
    con_error_msg(conn, msg = "Connection already closed.")
    cat_filter <- ""
    db_filter <- ""
    if (!is.null(catalog)) {
      cat_filter <- sprintf(
        "and lower(table_catalog) in ('%s')",
        paste(tolower(catalog), collapse = "', '")
      )
    }
    if (!is.null(schema)) {
      db_filter <- sprintf(
        "and lower(table_schema) in ('%s')",
        paste(tolower(schema), collapse = "', '")
      )
    }

    query <- "select
      table_name
    from information_schema.tables
    where table_schema != 'information_schema' %s %s
    order by table_catalog, table_schema
    "
    output <- suppressMessages(
      dbGetQuery(conn, sprintf(query, cat_filter, db_filter))[["table_name"]]
    )
  }
)

#' @title Get Athena Tables
#' @description Method to get Athena schema, tables and table types return as a data.frame
#' @param conn A [DBI::DBIConnection][DBI::DBIConnection-class] object,
#' as returned by [dbConnect()][DBI::dbConnect].
#' @param catalog Athena catalog, default set to NULL to return all tables from all Athena catalogs
#' @param schema Athena schema, default set to NULL to return all tables from all Athena schemas.
#'               Note: The use of DATABASE and SCHEMA is interchangeable within Athena.
#' @return `dbGetTables()` returns a data.frame.
#' @rdname AthenaConnection
#' @export
setGeneric("dbGetTables", function(conn, ...) standardGeneric("dbGetTables"))

#' @rdname AthenaConnection
#' @export
setMethod(
  "dbGetTables",
  "AthenaConnection",
  function(conn, catalog = NULL, schema = NULL, ...) {
    con_error_msg(conn, msg = "Connection already closed.")

    cat_filter <- ""
    db_filter <- ""
    if (!is.null(catalog)) {
      cat_filter <- sprintf(
        "and lower(table_catalog) in ('%s')",
        paste(tolower(catalog), collapse = "', '")
      )
    }
    if (!is.null(schema)) {
      db_filter <- sprintf(
        "and lower(table_schema) in ('%s')",
        paste(tolower(schema), collapse = "', '")
      )
    }

    query <- "select
      table_catalog as Catalog,
      table_schema as Schema,
      table_name as TableName,
      table_type as TableType
    from information_schema.tables
    where table_schema != 'information_schema' %s %s
    order by table_catalog, table_schema
    "
    output <- suppressMessages(
      dbGetQuery(conn, sprintf(query, cat_filter, db_filter))
    )
    return(output)
  }
)

#' @inheritParams DBI::dbListFields
#' @rdname AthenaConnection
#' @export
setMethod(
  "dbListFields",
  c("AthenaConnection", "character"),
  function(conn, name, ...) {
    con_error_msg(conn, msg = "Connection already closed.")
    ll <- db_detect(conn, name)
    retry_api_call(
      output <- conn@ptr$Athena$get_table_metadata(
        CatalogName = ll[["db.catalog"]],
        DatabaseName = ll[["dbms.name"]],
        TableName = ll[["table"]]
      )$TableMetadata
    )

    col_names <- vapply(
      output$Columns,
      function(y) y$Name,
      FUN.VALUE = character(1)
    )
    partitions <- vapply(
      output$PartitionKeys,
      function(y) y$Name,
      FUN.VALUE = character(1)
    )
    c(col_names, partitions)
  }
)

#' @inheritParams DBI::dbExistsTable
#' @rdname AthenaConnection
#' @export
setMethod(
  "dbExistsTable",
  c("AthenaConnection", "character"),
  function(conn, name, ...) {
    con_error_msg(conn, msg = "Connection already closed.")
    ll <- db_detect(conn, name)
    for (i in seq_len(athena_option_env$retry + 1)) {
      resp <- tryCatch(
        {
          conn@ptr$Athena$get_table_metadata(
            CatalogName = ll[["db.catalog"]],
            DatabaseName = ll[["dbms.name"]],
            TableName = ll[["table"]]
          )
        },
        error = function(err) {
          err_msg = err$message
          if (i == (athena_option_env$retry + 1)) {
            stop(err_msg, call. = F)
          }
          if (
            !grepl("EntityNotFoundException|Cannot.*find.*catalog", err_msg)
          ) {
            backoff <- 2**i * 0.5
            info_msg(err_msg)
            info_msg(
              paste("Request failed. Retrying in", backoff, "seconds...")
            )
            Sys.sleep(backoff)
            return(err)
          }
          return(FALSE)
        }
      )
      if (inherits(resp, "error")) {
        next
      }
      break
    }
    return(is.list(resp))
  }
)

#' @inheritParams DBI::dbExistsTable
#' @rdname AthenaConnection
#' @export
setMethod(
  "dbExistsTable",
  c("AthenaConnection", "Id"),
  function(conn, name, ...) {
    dbExistsTable(conn, dbQuoteIdentifier(conn, name), ...)
  }
)

#' @rdname AthenaConnection
#' @inheritParams DBI::dbRemoveTable
#' @param delete_data Deletes S3 files linking to AWS Athena table
#' @param confirm Allows for S3 files to be deleted without the prompt check. It is recommend to leave this set to \code{FALSE}
#'                   to avoid deleting other S3 files when the table's definition points to the root of S3 bucket.
#' @export
setMethod(
  "dbRemoveTable",
  c("AthenaConnection", "character"),
  function(conn, name, delete_data = TRUE, confirm = FALSE, ...) {
    con_error_msg(conn, msg = "Connection already closed.")
    stopifnot(
      is.logical(delete_data),
      is.logical(confirm)
    )
    ll <- db_detect(conn, name)

    TableMeta <- conn@ptr$Athena$get_table_metadata(
      CatalogName = ll[["db.catalog"]],
      DatabaseName = ll[["dbms.name"]],
      TableName = ll[["table"]]
    )[["TableMetadata"]]

    if (delete_data && TableMeta$TableType == "EXTERNAL_TABLE") {
      s3_path <- split_s3_uri(TableMeta$Parameters$location)
      # Detect if key ends with "/" or if it has ".": https://github.com/DyfanJones/noctua/issues/125
      if (!grepl("\\.|/$", s3_path$key)) {
        s3_path[["key"]] <- sprintf("%s/", s3_path[["key"]])
      }
      all_keys <- list()
      i <- 1
      token <- NULL
      # Get all s3 objects linked to table
      while (is.null(token) || length(token) != 0) {
        objects <- conn@ptr$S3$list_objects_v2(
          Bucket = s3_path[["bucket"]],
          Prefix = s3_path[["key"]],
          ContinuationToken = token
        )
        token <- objects[["NextContinuationToken"]]
        all_keys[[i]] <- lapply(objects[["Contents"]], function(x) {
          list(Key = x[["Key"]])
        })
        i <- i + 1
      }
      info_msg(
        "The S3 objects in prefix will be deleted:\n",
        paste0("s3://", s3_path$bucket, "/", s3_path$key)
      )
      if (!confirm) {
        confirm <- readline(prompt = "Delete files (y/n)?: ")
        if (trimws(tolower(confirm)) != "y") {
          info_msg("Table deletion aborted.")
          return(invisible(NULL))
        }
      }
      all_keys <- unlist(all_keys, recursive = FALSE, use.names = FALSE)
      # Only remove if files are found
      if (length(all_keys) > 0) {
        # Delete S3 files in batch size 1000
        key_parts <- split_vec(all_keys, 1000)
        for (i in seq_along(key_parts)) {
          conn@ptr$S3$delete_objects(
            Bucket = s3_path$bucket,
            Delete = list(Objects = key_parts[[i]])
          )
        }
      } else {
        warning(
          sprintf(
            'Failed to remove AWS S3 files from: "s3://%s/%s". Please check if AWS S3 files exist.',
            s3_path$bucket,
            s3_path$key
          ),
          call. = F
        )
      }
    }

    # Drop Athena table
    dbExecute(
      conn,
      sprintf('DROP TABLE IF EXISTS `%s`', paste(ll, collapse = '`.`'))
    )

    if (!delete_data) {
      info_msg("Only Athena table has been removed.")
    }
    on_connection_updated(conn, ll[["table"]])
    invisible(TRUE)
  }
)

#' @rdname AthenaConnection
#' @export
setMethod(
  "dbRemoveTable",
  c("AthenaConnection", "Id"),
  function(conn, name, delete_data = TRUE, confirm = FALSE, ...) {
    dbRemoveTable(conn, dbQuoteIdentifier(conn, name), ...)
  }
)


#' @rdname AthenaConnection
#' @inheritParams DBI::dbGetQuery
#' @inheritParams DBI::dbFetch
#' @param statistics If set to \code{TRUE} will print out AWS Athena statistics of query.
#' @param unload boolean input to modify `statement` to align with \href{https://docs.aws.amazon.com/athena/latest/ug/unload.html}{AWS Athena UNLOAD},
#'              default is set to \code{FALSE}.
#' @export
setMethod(
  "dbGetQuery",
  c("AthenaConnection", "character"),
  function(conn, statement, statistics = FALSE, unload = athena_unload(), ...) {
    con_error_msg(conn, msg = "Connection already closed.")
    stopifnot(is.logical(statistics), is.logical(unload))

    # dbplyr v2 support: dbplyr class ident
    if (!inherits(statement, "ident")) {
      rs <- dbSendQuery(conn, statement = statement, unload = unload)
      if (statistics) {
        print(dbStatistics(rs))
      }
      out <- dbFetch(res = rs, n = -1, ...)
      dbClearResult(rs)
    } else {
      # Create an empty table using AWS GLUE to retrieve column names
      field_names <- athena_query_fields_ident(conn, statement)
      empty_shell <- rep(list(character()), length(field_names))
      names(empty_shell) <- field_names

      if (inherits(athena_option_env[["file_parser"]], "athena_data.table")) {
        out <- as.data.table(empty_shell)
      } else {
        as_tibble <- pkg_method("as_tibble", "tibble")
        out <- as_tibble(empty_shell)
      }
    }
    return(out)
  }
)

#' @rdname AthenaConnection
#' @inheritParams DBI::dbGetInfo
#' @export
setMethod(
  "dbGetInfo",
  "AthenaConnection",
  function(dbObj, ...) {
    con_error_msg(dbObj, msg = "Connection already closed.")
    info <- as.list(dbObj@info)
    paws <- as.character(packageVersion("paws"))
    noctua <- as.character(packageVersion("noctua"))
    info <- c(info, paws = paws, noctua = noctua)
    return(info)
  }
)

#' @description This method returns all partitions from Athena table.
#' @inheritParams DBI::dbExistsTable
#' @param .format re-formats AWS Athena partitions format. So that each column represents a partition
#'         from the AWS Athena table. Default set to \code{FALSE} to prevent breaking previous package behaviour.
#' @return data.frame that returns all partitions in table, if no partitions in Athena table then
#'         function will return error from Athena.
#' @rdname AthenaConnection
#' @export
setGeneric(
  "dbGetPartition",
  def = function(conn, name, ..., .format = FALSE) {
    standardGeneric("dbGetPartition")
  },
  valueClass = "data.frame"
)

#' @inheritParams DBI::dbExistsTable
#' @rdname AthenaConnection
#' @export
setMethod(
  "dbGetPartition",
  "AthenaConnection",
  function(conn, name, ..., .format = FALSE) {
    con_error_msg(conn, msg = "Connection already closed.")
    stopifnot(is.logical(.format))
    ll <- db_detect(conn, name)
    dt <- dbGetQuery(
      conn,
      sprintf(
        "SHOW PARTITIONS %s",
        paste(ll, collapse = ".")
      )
    )

    if (.format) {
      # ensure returning format is data.table
      dt <- as.data.table(dt)
      dt <- dt[, tstrsplit(dt[[1]], split = "/")]
      partitions <- sapply(names(dt), function(x) {
        strsplit(dt[[x]][1], split = "=")[[1]][1]
      })
      for (col in names(dt)) {
        set(dt, j = col, value = tstrsplit(dt[[col]], split = "=")[2])
      }
      setnames(dt, old = names(dt), new = partitions)

      # convert data.table to tibble if using vroom as backend
      if (inherits(athena_option_env$file_parser, "athena_vroom")) {
        as_tibble <- pkg_method("as_tibble", "tibble")
        dt <- as_tibble(dt)
      }
    }
    return(dt)
  }
)

#' @description Executes a statement to return the data description language (DDL) of the Athena table.
#' @inheritParams DBI::dbExistsTable
#' @return \code{dbShow()} returns \code{\link[DBI]{SQL}} characters of the Athena table DDL.
#' @rdname AthenaConnection
#' @export
setGeneric(
  "dbShow",
  def = function(conn, name, ...) standardGeneric("dbShow"),
  valueClass = "character"
)

#' @inheritParams DBI::dbExistsTable
#' @rdname AthenaConnection
#' @export
setMethod(
  "dbShow",
  "AthenaConnection",
  function(conn, name, ...) {
    con_error_msg(conn, msg = "Connection already closed.")
    ll <- db_detect(conn, name)
    SQL(paste0(
      dbGetQuery(
        conn,
        sprintf("SHOW CREATE TABLE `%s`", paste0(ll, collapse = "`.`")),
        unload = FALSE
      )[[1]],
      collapse = "\n"
    ))
  }
)

#' @title dbConvertTable aws s3 backend file types.
#' @description Utilises AWS Athena to convert AWS S3 backend file types. It also also to create more efficient file types i.e. "parquet" and "orc" from SQL queries.
#' @param conn A [DBI::DBIConnection][DBI::DBIConnection-class] object,
#' @param obj Athena table or \code{SQL} DML query to be converted. For \code{SQL}, the query need to be wrapped with \code{DBI::SQL()} and
#'            follow AWS Athena DML format \href{https://docs.aws.amazon.com/athena/latest/ug/select.html}{link}
#' @param name Name of destination table
#' @param partition Partition Athena table
#' @param s3.location location to store output file, must be in s3 uri format for example ("s3://mybucket/data/").
#' @param file.type File type for \code{name}, currently support \code{c("NULL","csv", "tsv", "parquet", "json", "orc")}.
#'                  \code{"NULL"} will let Athena set the file type for you.
#' @param compress Compress \code{name}, currently can only compress \code{c("parquet", "orc")} (\href{https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html}{AWS Athena CTAS})
#' @param data If \code{name} should be created with data or not.
#' @param ... Extra parameters, currently not used
#' @name dbConvertTable
#' @return \code{dbConvertTable()} returns \code{TRUE} but invisible.
#' @export
setGeneric(
  "dbConvertTable",
  def = function(conn, obj, name, ...) standardGeneric("dbConvertTable")
)

#' @rdname dbConvertTable
#' @export
setMethod(
  "dbConvertTable",
  "AthenaConnection",
  function(
    conn,
    obj,
    name,
    partition = NULL,
    s3.location = NULL,
    file.type = c("NULL", "csv", "tsv", "parquet", "json", "orc"),
    compress = TRUE,
    data = TRUE,
    ...
  ) {
    con_error_msg(conn, msg = "Connection already closed.")
    stopifnot(
      is.character(obj),
      is.character(name),
      is.null(partition) || is.character(partition),
      is.null(s3.location) || is.s3_uri(s3.location),
      is.logical(compress),
      is.logical(data)
    )
    file.type <- match.arg(file.type)

    with_data <- if (data) " " else " NO "

    ins <- if (inherits(obj, "SQL")) {
      obj
    } else {
      sprintf(
        'SELECT * FROM "%s"',
        paste(db_detect(conn, obj), collapse = '"."')
      )
    }

    tt_sql <- paste0(
      'CREATE TABLE "',
      paste(db_detect(conn, name), collapse = '"."'),
      '" ',
      ctas_sql_with(partition, s3.location, file.type, compress),
      "AS ",
      ins,
      "\nWITH",
      with_data,
      "DATA",
      ";"
    )
    res <- dbExecute(conn, tt_sql, unload = FALSE)
    on.exit(dbClearResult(res))
    return(invisible(TRUE))
  }
)

# Unable to get Athena equivalents, for the time being will pass these methods.

#' @rdname AthenaConnection
#' @inheritParams DBI::dbBegin
#' @export
setMethod(
  "dbBegin",
  "AthenaConnection",
  function(conn, ...) {
    con_error_msg(conn, msg = "Connection already closed.")
    invisible(TRUE)
  }
)

# https://github.com/laughingman7743/PyAthena/blob/f4b21a0b0f501f5c3504698e25081f491a541d4e/pyathena/connection.py#L281C1-L282

#' @rdname AthenaConnection
#' @inheritParams DBI::dbCommit
#' @export
setMethod(
  "dbCommit",
  "AthenaConnection",
  function(conn, ...) {
    con_error_msg(conn, msg = "Connection already closed.")
    invisible(TRUE)
  }
)


# https://github.com/laughingman7743/PyAthena/blob/f4b21a0b0f501f5c3504698e25081f491a541d4e/pyathena/connection.py#L284-L285
#' @rdname AthenaConnection
#' @inheritParams DBI::dbRollback
#' @export
setMethod(
  "dbRollback",
  "AthenaConnection",
  function(conn, ...) {
    con_error_msg(conn, msg = "Connection already closed.")
    invisible(TRUE)
  }
)
