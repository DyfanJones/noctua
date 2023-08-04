.onLoad <- function(libname, pkgname) {
  dbplyr_version()
  if(identical(dbplyr_env$major, 1L)){
    register_s3_method("dplyr", "db_desc", "AthenaConnection")
    register_s3_method("dbplyr", "db_explain", "AthenaConnection")
    register_s3_method("dbplyr", "db_query_fields", "AthenaConnection")
    register_s3_method("dbplyr", "sql_translate_env", "AthenaConnection")
    register_s3_method("dbplyr", "sql_escape_string", "AthenaConnection")
  } else if(identical(dbplyr_env$major, 2L)){
    register_s3_method("dbplyr", "dbplyr_edition", "AthenaConnection")
    register_s3_method("dbplyr", "db_connection_describe", "AthenaConnection")
    register_s3_method("dbplyr", "sql_query_explain", "AthenaConnection")
    register_s3_method("dbplyr", "sql_query_fields", "AthenaConnection")
    register_s3_method("dbplyr", "sql_translation", "AthenaConnection")
    register_s3_method("dbplyr", "sql_escape_date", "AthenaConnection")
    register_s3_method("dbplyr", "sql_escape_datetime", "AthenaConnection")
  }
  register_s3_method("dbplyr", "db_compute", "AthenaConnection")
  register_s3_method("dbplyr", "db_copy_to", "AthenaConnection")
  register_s3_method("dbplyr", "sql_table_analyze", "AthenaConnection")
  register_s3_method("dbplyr", "sql_query_save", "AthenaConnection")
}

register_s3_method <- function(pkg, generic, class, fun = NULL) {
  stopifnot(is.character(pkg), length(pkg) == 1)
  stopifnot(is.character(generic), length(generic) == 1)
  stopifnot(is.character(class), length(class) == 1)
  
  if (is.null(fun)) {
    fun <- get(paste0(generic, ".", class), envir = parent.frame())
  } else {
    stopifnot(is.function(fun))
  }
  
  if (pkg %in% loadedNamespaces()) {
    registerS3method(generic, class, fun, envir = asNamespace(pkg))
  }
  
  # Always register hook in case package is later unloaded & reloaded
  setHook(
    packageEvent(pkg, "onLoad"),
    function(...) {
      registerS3method(generic, class, fun, envir = asNamespace(pkg))
    }
  )
}

dbplyr_version <- function(){
  if (nzchar(system.file(package = "dbplyr"))){
    dbplyr_env$version <- packageVersion("dbplyr")
    dbplyr_env$major <- dbplyr_env$version$major
    dbplyr_env$minor <- dbplyr_env$version$minor
  } else {
    # default to minimum supported dbplyr version
    dbplyr_env$major = 1L
    dbplyr_env$minor = 4L
  }
}

dbplyr_env <- new.env(parent=emptyenv())
