# Set environmental variable 
athena_option_env <- new.env(parent=emptyenv())
athena_option_env$file_parser <- "file_method"
class(athena_option_env$file_parser) <- "athena_data.table"

# ==========================================================================
# Setting file parser method

#' A method to change noctua backend file parser.
#'
#' @param file_parser Method to read and write tables to Athena, currently defaults to data.table
#' @return \code{noctua_options()} returns \code{NULL}, invisibly.
#' @examples
#' library(noctua)
#' 
#' # change file parser from default data.table to vroom
#' noctua_options("vroom")
#' @export
noctua_options <- function(file_parser = c("data.table", "vroom")) {
  file_parser = match.arg(file_parser)
  
  if (!requireNamespace(file_parser, quietly = TRUE)) 
    stop('Please install ', file_parser, ' package and try again', call. = F)
  
  switch(file_parser,
         "vroom" = if(packageVersion(file_parser) < '1.2.0')  
           stop("Please update `vroom` to  `1.2.0` or later", call. = FALSE))
  
  class(athena_option_env$file_parser) <- paste("athena", file_parser, sep = "_")
  invisible(NULL)
} 