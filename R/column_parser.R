# Helper functions to parse the more complex data types

.error_msg = "Column `%s` was unable to be converted."

# Takes a string and converts it to raw
hex2raw <- function(string){
  split_str = strsplit(string, split = " ", fixed = TRUE)
  return(lapply(split_str, function(x) as.raw(as.hexmode(x))))
}

# applying string convertion across entire data frame
raw_parser <- function(output, columns){
  
  # only convert Athena data types `varbinary`
  for (col in names(columns[columns %in% c("varbinary")])) {
    tryCatch({
      set(output, j=col, value=hex2raw(output[[col]]))
    },
    error = function(e){
      warning(sprintf(.error_msg, col), call. = F)
    })
  }
}

split_vec <- function(vec, len, max_len = length(vec)){
  chunks <- seq(1, max_len, len)
  ll <- Map(function(i) list(), 1:length(chunks))
  for (i in seq_along(chunks))
    ll[[i]] <- vec[chunks[i]:min(chunks[i]+(len-1), max_len)]
  return(ll)
}

create_json_string <- function(string){paste0("[", paste(string, collapse = ","), "]")}

# chunk up json strings then collapse json chunks before parsing them 
json_chunks <- function(string, fun=jsonlite::parse_json, min_chunk = 10000L){
  if(length(string) < min_chunk){
    output <- fun(paste0("[", paste(string, collapse = ","), "]"))
  } else {
    len <- max(ceiling(length(string)/20), min_chunk)
    split_string <- split_vec(string, len)
    output <- unlist(
      lapply(split_string, function(i) fun(create_json_string(i))),
      recursive = FALSE
    )
  }
  return(output)
}

# parse json string
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
  # only convert Athena data types `array` and `json`
  for (col in names(columns[columns %in% c("array", "json")])) {
    tryCatch({
      set(output,
          j=col,
          value=json_chunks(output[[col]], parse_json))
    },
    error = function(e){
      message(e)
      warning(sprintf(.error_msg, col), call. = F)
    })
  }
}