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
# For instance, a connection in which the top-level object is a database that
# contains tables and views, the function will return a list like the
# following:
#
# \preformatted{list(database = list(contains = list(
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

AthenaListObjectTypes.default <- function(connection) {
  # slurp all the objects in the database so we can determine the correct
  # object hierarchy
  
  # all databases contain tables, at a minimum
  obj_types <- list(table = list(contains = "data"))
  
  # see if we have views too
  table_types <- AthenaTableTypes(connection)
  if (any(grepl("VIEW", table_types))) {
    obj_types <- c(obj_types, list(view = list(contains = "data")))
  }
  
  # check for multiple database or a named database
  databases <- AthenaDatabase(connection)
  if (length(databases) > 1) {
    obj_types <- list(database = list(contains = obj_types))
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
# which have the given attributes; for instance, passing \code{database = "foo"}
# will return only objects matching the database \code{foo}.
#
# @param connection A connection object, as returned by `dbConnect()`.
# @param ... Attributes to filter by.
# @return A data frame with \code{name} and \code{type} columns, listing the
#   objects.

AthenaListObjects <- function(connection, ...) {UseMethod("AthenaListObjects")}

AthenaListObjects.default <- function(connection, database = NULL, name = NULL, ...) {
  
  # if no database was supplied but this database has database, return a list of
  # database
  if (is.null(database)) {
    database <- AthenaDatabase(connection)
    if (length(database) > 1) {
      return(
        data.frame(
          name = database,
          type = rep("database", times = length(database)),
          stringsAsFactors = FALSE
        ))
    }
  }
  
  objs <- AthenaTableTypes(connection, database= database, name = name)
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
  get_region <- pkg_method("get_region", "paws.common")
  paste(collapse = "_",c(
    connection@info$profile_name,
    "Athena",
    get_region(connection@info$profile_name)
  ))
}

computeDisplayName <- function(connection) {
  get_region <- pkg_method("get_region", "paws.common")
  paste0("AWS Region: ", get_region(connection@info$profile_name))
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

AthenaListColumns.default <- function(connection, table = NULL, view = NULL, database = NULL, ...) {
  if (dbIsValid(connection)) {
    glue <- connection@ptr$glue
    
    output <- glue$get_table(DatabaseName = database, Name = table %||% view)$Table
    
    col_names <- sapply(output$StorageDescriptor$Columns, function(x){names(x$Type) = x$Name; x$Type})
    partition <- unlist(sapply(output$PartitionKeys, function(x){names(x$Type) = x$Name; x$Type}))
    

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

AthenaTableTypes <- function(connection, database = NULL, name = NULL, ...) {
  if (!dbIsValid(connection)) {stop("Connection already closed.", call. = FALSE)}
  glue <- connection@ptr$glue
  
  if(is.null(database)) database <- sapply(glue$get_databases()$DatabaseList,function(x) x$Name)
  if(is.null(name)){
    output <- lapply(database, function (x) glue$get_tables(DatabaseName = x)$TableList)
    tbl_meta <- unlist(lapply(output, function(x) sapply(x, function(y) {names(y$TableType) = y$Name; y$TableType})))}
  else{
    output <- glue$get_table(DatabaseName = database, Name = name)$Table
    tbl_meta <- output$TableType
    names(tbl_meta) <- output$Name}
  tbl_meta
}

AthenaDatabase <- function(connection, ...) {
  if (!dbIsValid(connection)) {stop("Connection already closed.", call. = FALSE)}
  glue <- connection@ptr$glue
  sapply(glue$get_databases()$DatabaseList,function(x) x$Name)
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

AthenaPreviewObject <- 
  function(connection, rowLimit, table = NULL, 
           view = NULL, database = NULL, ...) {
    # extract object name from arguments
    name <- validateObjectName(table, view)
    
    # prepend database if specified
    if (!is.null(database)) {
      name <- paste(dbQuoteIdentifier(connection, database),
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
  
  type <- "Athena"
  host <- computeHostName(con)
  observer$connectionClosed(type, host)
}

on_connection_updated <- function(con, hint) {
  # make sure we have an observer
  observer <- getOption("connectionObserver")
  if (is.null(observer))
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
  
  # find an icon for this DBMS
  icon <- AthenaConnectionIcon(connection)
  
  # let observer know that connection has opened
  observer$connectionOpened(
    # name displayed in connection pane
    displayName = computeDisplayName(connection),
    
    type = "Athena",
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