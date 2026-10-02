.onLoad <- function(libname, pkgname) {
  dbplyr_version()
  register_s3_method("dbplyr", "dbplyr_edition", "AthenaConnection")
  register_s3_method("dbplyr", "db_connection_describe", "AthenaConnection")
  register_s3_method("dbplyr", "sql_query_explain", "AthenaConnection")
  register_s3_method("dbplyr", "sql_query_fields", "AthenaConnection")
  register_s3_method("dbplyr", "sql_translation", "AthenaConnection")
  register_s3_method("dbplyr", "sql_escape_date", "AthenaConnection")
  register_s3_method("dbplyr", "sql_escape_datetime", "AthenaConnection")
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

dbplyr_version <- function() {
  dbplyr_env$available <- requireNamespace("dbplyr", quietly = TRUE)
}

dbplyr_env <- new.env(parent = emptyenv())
