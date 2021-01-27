# Helper functions to parse the more complex data types

.error_msg = "Column `%s` was unable to be converted."

# Takes a string and converts it to raw
hex2raw <- function(string){
  split_str = strsplit(string, " ")[[1]]
  as.raw(as.hexmode(split_str))
}

# applying string convertion across entire data frame
raw_parser <- function(output, columns){
  
  # only convert Athena data types `varbinary`
  for (col in names(columns[columns %in% c("varbinary")])) {
    tryCatch({
      set(output, j=col, value=lapply(output[[col]], hex2raw))
    },
    error = function(e){
      warning(sprintf(.error_msg, col), call. = F)
    })
  }
}

json_parser <- function(output, columns){
  # Get JSON conversion method
  if(identical(athena_option_env$json, "auto")){
    parse_json <- pkg_method("parse_json", "jsonlite")
  } else if(is.function(athena_option_env$json)) {
    parse_json <- athena_option_env$json
  } else if(is.character(athena_option_env$json) &&
            athena_option_env$json != "auto"){
    stop("Unknown Json conversion method.", call. = F)
  }
  
  # only convert Athena data types `array` and `json`
  for (col in names(columns[columns %in% c("array", "json")])) {
    tryCatch({
      set(output, j=col, value=lapply(output[[col]], parse_json))
    },
    error = function(e){
      warning(sprintf(.error_msg, col), call. = F)
    })
  }
}
