.onLoad <- function(libname, pkgname) {
  readr_check()
  register_s3_method("dplyr", "db_desc", "AthenaConnection")
  register_s3_method("dbplyr", "db_compute", "AthenaConnection")
  register_s3_method("dplyr", "db_save_query", "AthenaConnection")
  register_s3_method("dbplyr", "db_copy_to", "AthenaConnection")
  register_s3_method("dbplyr", "sql_translate_env", "AthenaConnection")
  register_s3_method("dbplyr", "sql_escape_string", "AthenaConnection")
  register_s3_method("dbplyr", "db_explain", "AthenaConnection")
  register_s3_method("dbplyr", "db_query_fields", "AthenaConnection")
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

readr_check <- function() if (!requireNamespace("readr", quietly = TRUE)) packageStartupMessage("Info: For extra speed please install `readr`.")