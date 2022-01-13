# Set environmental variable 
athena_option_env <- new.env(parent=emptyenv())
athena_option_env$file_parser <- "file_method"
athena_option_env$cache_size <- 0
class(athena_option_env$file_parser) <- "athena_data.table"

cache_dt = data.table("QueryId" = character(), "Query" = character(), "State"= character(),
                      "StatementType"= character(),"WorkGroup" = character(), "UnloadDir" = character())
athena_option_env$cache_dt <-  cache_dt
athena_option_env$retry <- 5
athena_option_env$bigint <- "integer64"
athena_option_env$binary <- "raw"
athena_option_env$json <- "auto"
athena_option_env$rstudio_conn_tab <- TRUE
athena_option_env$athena_unload <- FALSE
athena_option_env$verbose <- TRUE

# ==========================================================================
# helper function to handle big integers
big_int <- function(bigint){
  fp <- class(athena_option_env$file_parser)
  
  if(fp == "athena_data.table")
    return(switch(bigint,
                  "I" = bit64_check("integer64"),
                  "i" = "integer",
                  "d" = "double",
                  "c" = "character",
                  "numeric" = "double",
                  bigint)
    )
  if(fp == "athena_vroom")
    return(switch(bigint,
                  "integer64" = bit64_check("I"), 
                  "integer" = "i",
                  "numeric" = "d",
                  "double" = "d",
                  "character" = "c",
                  bigint)
    )
}

bit64_check <- function(value){
  if(!nzchar(system.file(package = "bit64")))
    stop('integer64 is supported by `bit64`. Please install `bit64` package and try again', call. = F)
  return(value)
}

# ==========================================================================
# Setting file parser method

#' A method to configure noctua backend options.
#'
#' \code{noctua_options()} provides a method to change the backend. This includes changing the file parser,
#'  whether \code{noctua} should cache query ids locally and number of retries on a failed api call.
#' @param file_parser Method to read and write tables to Athena, currently default to \code{"data.table"}. The file_parser also
#'                    determines the data format returned for example \code{"data.table"} will return \code{data.table} and \code{"vroom"} will return \code{tibble}.
#' @param bigint The R type that 64-bit integer types should be mapped to (default: \code{"integer64"}).
#'    Inbuilt \code{bigint} conversion types ["integer64", "integer", "numeric", "character"].
#' @param binary The R type that [binary/varbinary] types should be mapped to (default \code{"raw"}).
#'    Inbuilt binary conversion types ["raw", "character"].
#' @param json Attempt to converts AWS Athena data types [arrays, json] using \code{jsonlite:parse_json} (default: \code{"auto"}).
#'    Inbuilt json conversion types ["auto", "character"].
#'    Custom Json parsers can be provide by using a function with data frame parameter.
#' @param cache_size Number of queries to be cached. Currently only support caching up to 100 distinct queries (default: \code{0}).
#' @param clear_cache Clears all previous cached query metadata
#' @param retry Maximum number of requests to attempt (default: \code{5}).
#' @param retry_quiet This method is deprecated please use verbose instead.
#' @param unload set AWS Athena unload functionality globally (default: \code{FALSE})
#' @param verbose print package info messages (default: \code{TRUE})
#' @return \code{noctua_options()} returns \code{NULL}, invisibly.
#' @examples
#' library(noctua)
#' 
#' # change file parser from default data.table to vroom
#' noctua_options("vroom")
#' 
#' # cache queries locally
#' noctua_options(cache_size = 5)
#' @export
noctua_options <- function(file_parser,
                           bigint,
                           binary,
                           json,
                           cache_size,
                           clear_cache,
                           retry,
                           retry_quiet,
                           unload,
                           verbose) {
  # Reset to defaults
  if(missing(file_parser) & missing(bigint)
     & missing(binary) & missing(json)
     & missing(cache_size) & missing(clear_cache)
     & missing(retry) & missing(retry_quiet)
     & missing(unload) & missing(verbose)){
    file_parser <- "data.table"
    bigint <- "integer64"
    binary <- "raw"
    json <- "auto"
    cache_size <- 0
    retry <- 5
    unload <- FALSE
    verbose <- TRUE
  }
  if(!missing(file_parser)){
    file_parser = match.arg(file_parser, choices = c("data.table", "vroom"))
    
    if (!nzchar(system.file(package = file_parser))) 
      stop('Please install ', file_parser, ' package and try again', call. = F)
    
    switch(file_parser,
           "vroom" = if(packageVersion(file_parser) < '1.2.0')  
             stop("Please update `vroom` to  `1.2.0` or later", call. = FALSE))
    
    class(athena_option_env$file_parser) <- paste("athena", file_parser, sep = "_")
  }
  
  missing_match(bigint, c("integer64", "integer", "numeric", "character"), athena_option_env, "big_int")
  missing_match(binary, c("raw", "character"), athena_option_env, "binary")
  athena_option_env$bigint <- big_int(athena_option_env$bigint)
  
  # only change json when not null
  if(!missing(json)){
    if(is.character(json)) {
      athena_option_env[["json"]] <- match.arg(json, c("auto", "character"))
    } else if(is.function(json)) {
      athena_option_env$json <- json
    } else{
      stop(
        'Unknown json parser. Please use defaults ["auto", "character"] or a custom function.',
        call. = F)
    }
  }
  missing_expr(cache_size, is.numeric, sprintf("`cache_size` is class `%s`. Please set `cache_size` to numeric", class(cache_size)), {
    if(cache_size < 0 | cache_size > 100)
      stop("noctua currently only supports up to 100 queries being cached", call. = F)
    athena_option_env$cache_size <- as.integer(cache_size)
  })
  missing_expr(retry, is.numeric, sprintf("`retry` is class `%s`. Please set `retry` to numeric", class(retry)), {
    if(retry < 0) stop("Number of retries is required to be greater than 0.", call. = F)
    athena_option_env[["retry"]] <- as.integer(retry)
  })
  if(!missing(retry_quiet)) {
    warning("`retry_quiet` has been deprecated in favour of `verbose`. Please use `verbose` in the future.", call. = F)
  }
  missing_expr(clear_cache, is.logical, sprintf("`clear_cache` is class `%s`. Please set `clear_cache` to logical", class(clear_cache)), {
    if(clear_cache)
      athena_option_env$cache_dt <- athena_option_env$cache_dt[0]  
  })
  missing_expr(unload, is.logical, sprintf("`unload` is class `%s`. Please set `unload` to logical", class(unload)), {
    athena_option_env$athena_unload <- unload
  })
  missing_expr(verbose, is.logical, sprintf("`verbose` is class `%s`. Please set `verbose` to logical", class(verbose)), {
    athena_option_env$verbose <- verbose
  })
  invisible(NULL)
}

missing_match = function(x, choices, env, var){
  if(!missing(x))
    env[[var]] <- match.arg(x, choices)
}

missing_expr = function(x, check, msg, expr){
  if(!missing(x)){
    if(!check(x)) stop(msg, call. = F)
    eval.parent(substitute(expr))
  }
}
