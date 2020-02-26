# Set environmental variable 
athena_option_env <- new.env(parent=emptyenv())
athena_option_env$file_parser <- "file_method"
athena_option_env$cache_size <- 0
class(athena_option_env$file_parser) <- "athena_data.table"

cache_dt = data.table(QueryId = character(), Query = character(), State= character(),
                      StatementType= character(),WorkGroup = character(), timestamp = integer())
athena_option_env$cache_dt <-  cache_dt

# ==========================================================================
# Setting file parser method

#' A method to configue noctua backend options.
#'
#' \code{noctua_options()} provides a method to change the backend. This includes changing the file parser
#' and whether \code{noctua} should cache query ids locally.
#' @param file_parser Method to read and write tables to Athena, currently defaults to data.table
#' @param cache_size Number of queries to be cached. Currently only support caching up to 50 queries.
#' @return \code{noctua_options()} returns \code{NULL}, invisibly.
#' @examples
#' library(noctua)
#' 
#' # change file parser from default data.table to vroom
#' noctua_options("vroom")
#' @export
noctua_options <- function(file_parser = c("data.table", "vroom"), cache_size = 0) {
  file_parser = match.arg(file_parser)
  
  if(cache_size < 0 && cache_size > 50) stop("noctua currently only caches up to 50 queries", call. = F)
  
  if (!requireNamespace(file_parser, quietly = TRUE)) 
    stop('Please install ', file_parser, ' package and try again', call. = F)
  
  switch(file_parser,
         "vroom" = if(packageVersion(file_parser) < '1.2.0')  
           stop("Please update `vroom` to  `1.2.0` or later", call. = FALSE))
  
  class(athena_option_env$file_parser) <- paste("athena", file_parser, sep = "_")
  
  athena_option_env$cache_size <- cache_size
  
  invisible(NULL)
} 