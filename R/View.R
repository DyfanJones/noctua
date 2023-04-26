# adapted from R package odbc

# Return the object hierarchy supported by a connection.
#
# Lists the object types and metadata known by the connection, and how those
# object types relate to each other.
#
# The returned hierarchy takes the form of a nested list, in which each object
# type supported by the connection is a named list with the following
# attributes:
#
# \describe{
#   \item{contains}{A list of other object types contained by the object, or
#       "data" if the object contains data}
#   \item{icon}{An optional path to an icon representing the type}
# }
#
# For instance, a connection in which the top-level object is a schema that
# contains tables and views, the function will return a list like the
# following:
#
# \preformatted{list(schema = list(contains = list(
#                    list(name = "table", contains = "data")
#                    list(name = "view", contains = "data"))))
#
# }
# @param connection A connection object, as returned by `dbConnect()`.
# @return The hierarchy of object types supported by the connection.

# nocov start
AthenaListObjectTypes <- function(connection) {
  UseMethod("AthenaListObjectTypes")
}

#' @export
AthenaListObjectTypes.default <- function(connection) {
  # slurp all the objects in the schema so we can determine the correct
  # object hierarchy
  
  # all schema contain tables, at a minimum
  obj_types <- list(table = list(contains = "data"))
  
  # see if we have views too
  table_types <- AthenaTableTypes(connection, connection@info[["db.catalog"]])
  if (any(grepl("VIEW", table_types))) {
    obj_types <- c(obj_types, list(view = list(contains = "data")))
  }

  # check for multiple schema or a named schema
  schemas <- AthenaDatabase(connection, connection@info[["db.catalog"]])
  if (length(schemas) > 0) {
    obj_types <- list(schema = list(contains = obj_types))
  }
  
  catalogs <- list_catalogs(connection@ptr$Athena)
  # check for multiple catalogs
  if (length(catalogs) > 0) {
    obj_types <- list(catalog = list(contains = obj_types))
  }
  
  obj_types
}

# List objects in a connection.
#
# Lists all of the objects in the connection, or all the objects which have
# specific attributes.
#
# When used without parameters, this function returns all of the objects known
# by the connection. Any parameters passed will filter the list to only objects
# which have the given attributes; for instance, passing \code{schema = "foo"}
# will return only objects matching the schema \code{foo}.
#
# @param connection A connection object, as returned by `dbConnect()`.
# @param ... Attributes to filter by.
# @return A data frame with \code{name} and \code{type} columns, listing the
#   objects.

AthenaListObjects <- function(connection, ...) {UseMethod("AthenaListObjects")}

AthenaListObjects.AthenaConnection <- function(connection, catalog = NULL, schema = NULL, name = NULL, ...) {
  
  # if no catalog was supplied but this database has catalogs, return a list of
  # catalogs
  if (is.null(catalog)) {
    catalogs <- list_catalogs(connection@ptr$Athena)
    if (length(catalogs) > 0) {
      return(
        data.frame(
          name = catalogs,
          type = rep("catalog", times = length(catalogs)),
          stringsAsFactors = FALSE
        ))
    }
  }
  
  # if no schema was supplied but this catalog has schema, return a list of
  # schema
  if (is.null(schema)) {
    schema <- AthenaDatabase(connection, catalog)
    if (length(schema) > 0) {
      return(
        data.frame(
          name = schema,
          type = rep("schema", times = length(schema)),
          stringsAsFactors = FALSE
        ))
    }
  }
  
  objs <- AthenaTableTypes(connection, catalog = catalog, schema = schema, name = name)
  # just return a list of the objects and their types, possibly filtered by the
  # options above
  data.frame(
    name = names(objs),
    type = gsub(".*_", "", unname(tolower(objs))),
    stringsAsFactors = FALSE
  )
}

# List columns in an object.
#
# Lists the names and types of each column (field) of a specified object.
#
# The object to inspect must be specified as one of the arguments
# (e.g. \code{table = "employees"}); depending on the driver and underlying
# data store, additional specification arguments may be required.
#
# @param connection A connection object, as returned by `dbConnect()`.
# @param ... Parameters specifying the object.
# @return A data frame with \code{name} and \code{type} columns, listing the
#   object's fields.

# given a connection, returns its "host name" (a unique string which identifies it)
computeHostName <- function(connection) {
  paste(collapse = "_",c(
    connection@info$profile_name,
    "Athena",
    connection@info$region_name
  ))
}

computeDisplayName <- function(connection) {
  paste0("AWS Region: ", connection@info$region_name)
}

# selects the table or view from arguments
validateObjectName <- function(table, view) {
  
  # Error if both table and view are passed
  if (!is.null(table) && !is.null(view)) {
    stop("`table` and `view` can not both be used", call. = FALSE)
  }
  
  # Error if neither table and view are passed
  if (is.null(table) && is.null(view)) {
    stop("`table` and `view` can not both be `NULL`", call. = FALSE)
  }
  
  table %||% view
}

AthenaListColumns <- function(connection, ...) UseMethod("AthenaListColumns")

AthenaListColumns.AthenaConnection <- function(connection,
                                               table = NULL,
                                               view = NULL,
                                               catalog = NULL,
                                               schema = NULL,
                                               ...) {
  if (dbIsValid(connection)) {
    athena <- connection@ptr$Athena
    
    output <- athena$get_table_metadata(
      CatalogName = catalog,
      DatabaseName = schema,
      TableName = table %||% view
    )$TableMetadata
    
    col_names <- sapply(output$Columns, ColMeta)
    partition <- unlist(sapply(output$PartitionKeys, ColMeta))

    tbl_meta <- c(col_names, partition)
    data.frame(
      name = names(tbl_meta),
      type = unname(tbl_meta),
      stringsAsFactors = FALSE
    )
  } else {
    NULL
  }
}

AthenaTableTypes <- function(connection, catalog = NULL, schema = NULL, name = NULL, ...) {
  con_error_msg(connection, "Connection already closed.")
  athena <- connection@ptr$Athena
  
  if (is.null(catalog)) {
    catalog <- list_catalogs(athena)
  }
  
  if (is.null(schema)) {
    schema <- sapply(catalog, function(ct) list_schemas(athena, ct), simplify = FALSE)
  } else {
    names(schema) <- if(length(catalog) == 1) catalog else connection@info$db.catalog
  }
  
  if (is.null(name)) {
    output <- tryCatch(
      {
        unlist(
          lapply(names(schema), function(n) {
            lapply(schema[[n]], function(s) list_tables(athena, n, s))
          }),
          recursive = F
        )
      },
      error = function(cond) list(list())
    )
    tbl_meta <- sapply(unlist(output, recursive = F), function(x) TblMeta(x))
  } else {
    output <- athena$get_table_metadata(
      CatalogName = catalog,
      DatabaseName = schema,
      TableName = name
    )$TableMetadata
    tbl_meta <- output$TableType
    names(tbl_meta) <- output$Name}
  return(tbl_meta)
}

AthenaDatabase <- function(connection, catalog, ...) {
  con_error_msg(connection, "Connection already closed.")
  athena <- connection@ptr$Athena
  return(unlist(list_schemas(athena, catalog)))
}

# Preview the data in an object.
#
# Return the data inside an object as a data frame.
#
# The object to previewed must be specified as one of the arguments
# (e.g. \code{table = "employees"}); depending on the driver and underlying
# data store, additional specification arguments may be required.
#
# @param connection A connection object, as returned by `dbConnect()`.
# @param rowLimit The maximum number of rows to display.
# @param ... Parameters specifying the object.
# @return A data frame containing the data in the object.

AthenaPreviewObject <- function(connection, rowLimit, ...) UseMethod("AthenaPreviewObject")

AthenaPreviewObject.AthenaConnection <- function(connection, rowLimit, table = NULL, 
                                                 view = NULL, catalog = NULL, schema = NULL, ...) {
    # extract object name from arguments
    name <- validateObjectName(table, view)
    
    # prepend schema if specified
    if (!is.null(schema)) {
      name <- paste(dbQuoteIdentifier(connection, schema),
                    dbQuoteIdentifier(connection, name), sep = ".")
    }
    
    dbGetQuery(connection, paste("SELECT * FROM", name, "LIMIT", rowLimit))
  }

# Get an icon representing a connection.
#
# Return the path on disk to an icon representing a connection.
#
# The icon returned should be a 32x32 square image file.
#
# @param connection A connection object, as returned by `dbConnect()`.
# @return The path to an icon file on disk.

AthenaConnectionIcon <- function(connection) {
  # no icon is returned by default
  icons <- system.file(file.path("icons"), package = "noctua")
  file.path(icons, "athena-logo.png")
}

# List the actions supported for the connection
#
# Return a list of actions that can be performed on the connection.
#
# The list returned is a named list of actions, where each action has the
# following properties:
#
# \describe{
#   \item{callback}{A function to be invoked to perform the action}
#   \item{icon}{An optional path to an icon representing the action}
# }
#
# @param connection A connection object, as returned by `dbConnect()`.
# @return A named list of actions that can be performed on the connection.

AthenaConnectionActions <- function(connection) {
  icons <- system.file(file.path("icons"), package = "noctua")
  
  actions <- list(
    "Athena" = list(
      icon = file.path(icons, "athena-logo.png"),
      callback = function() {
        utils::browseURL("https://aws.amazon.com/athena/")
      }
    )
  )
  
  if (exists(".rs.api.documentNew")) {
    documentNew <- get(".rs.api.documentNew")
    actions <- c(
      actions,
      list(
        SQL = list(
          icon = file.path(icons, "edit-sql.png"),
          callback = function() {
            varname <- Filter(
              function(e) identical(get(e, envir = .GlobalEnv), connection),
              ls(envir = .GlobalEnv))
            
            tables <- dbListTables(connection)
            
            contents <- paste(
              paste("-- !preview conn=", varname, sep = ""),
              "",
              if (length(tables) > 0)
                paste("SELECT * FROM \"", tables[[1]], "\"\nLIMIT 100", 
                      "\n\n-- Note: Please utilise LIMIT to restrict Data Scanned by AWS Athena",
                      sep = "")
              else
                "SELECT 1",
              "",
              sep = "\n"
            )
            
            documentNew("sql", contents, row = 2, column = 15, execute = FALSE)
          }
        )
      )
    )
  }
  
  actions <- c(
    actions,
    list(
      Help = list(
        icon = file.path(icons, "help.png"),
        callback = function() {
          utils::browseURL("https://dyfanjones.github.io/noctua/")
        }
      )
    )
  )
  
  actions
}

on_connection_closed <- function(con) {
  # make sure we have an observer
  observer <- getOption("connectionObserver")
  if (is.null(observer))
    return(invisible(NULL))
  
  if(!athena_option_env$rstudio_conn_tab)
    return(invisible(NULL))
  
  type <- "Athena"
  host <- computeHostName(con)
  observer$connectionClosed(type, host)
}

on_connection_updated <- function(con, hint) {
  # make sure we have an observer
  observer <- getOption("connectionObserver")
  if (is.null(observer))
    return(invisible(NULL))
  
  if(!athena_option_env$rstudio_conn_tab)
    return(invisible(NULL))
  
  type <- "Athena"
  host <- computeHostName(con)
  observer$connectionUpdated(type, host, hint = hint)
}

on_connection_opened <- function(connection) {
  # make sure we have an observer
  observer <- getOption("connectionObserver")
  if (is.null(observer))
    return(invisible(NULL))
  
  if(!athena_option_env$rstudio_conn_tab)
    return(invisible(NULL))
  
  # find an icon for this DBMS
  icon <- AthenaConnectionIcon(connection)
  
  # let observer know that connection has opened
  observer$connectionOpened(
    type = "Athena",
    
    # name displayed in connection pane
    displayName = computeDisplayName(connection),
    
    # host key
    host = computeHostName(connection),
    
    # icon for connection
    icon = icon,
    
    # connection code
    connectCode = paste(c("library(DBI)", "con <- dbConnect(noctua::athena())"),collapse = "\n"),
    
    # disconnection code
    disconnect = function() {
      dbDisconnect(connection)
    },
    
    listObjectTypes = function () {
      AthenaListObjectTypes(connection)
    },
    
    # table enumeration code
    listObjects = function(...) {
      AthenaListObjects(connection, ...)
    },
    
    # column enumeration code
    listColumns = function(...) {
      AthenaListColumns(connection, ...)
    },
    
    # table preview code
    previewObject = function(rowLimit, ...) {
      AthenaPreviewObject(connection, rowLimit, ...)
    },
    
    # other actions that can be executed on this connection
    actions = AthenaConnectionActions(connection),
    
    # raw connection object
    connectionObject = connection
  )
}
# nocov end 

TblMeta <- function(x) {
  tbl_type <- if(length(x$TableType)) x$TableType else ""
  names(tbl_type) <- x$Name
  tbl_type
}

ColMeta <- function(x){
  col_type <- if(length(x$Type)) x$Type else ""
  names(col_type) <- x$Name
  col_type
}
