#' @include utils.R

sql_quantile <- function(x, probs){
  build_sql <- pkg_method("build_sql", "dbplyr")
  check_probs(probs)
  build_sql("APPROX_PERCENTILE(",x,", ",probs,")")
}

sql_median <- function(){
  warned <- FALSE
  function(x, na.rm = FALSE){
    warned <<- check_na_rm(na.rm, warned)
    sql_quantile(x, 0.5)
  }
}

# mimic check_na_rm from dbplyr
# https://github.com/tidyverse/dbplyr/blob/master/R/translate-sql-helpers.R#L213-L225
check_na_rm <- function(na.rm, warned){
  if(warned || identical(na.rm, TRUE))
    return(warned)
  warning(
    "Missing values are always removed in SQL.\n", "Use `", 
    "median(x, na.rm = TRUE)` to silence this warning\n",
    "This warning is displayed only once per session.", 
    call. = FALSE)
  return(TRUE)
}

# re-create check_probs from dbplyr:
# https://github.com/tidyverse/dbplyr/blob/master/R/translate-sql-quantile.R#L40-L48
check_probs <- function(probs) {
  if (!is.numeric(probs)) {
    stop("`probs` must be numeric", call. = FALSE)
  }
  
  if (length(probs) > 1) {
    stop("SQL translation only supports single value for `probs`.", call. = FALSE)
  }
}

# helper function to support R function paste in sql_translation_env
athena_paste <- function(..., sep = " ", con) {
  escape <- pkg_method("escape", "dbplyr")
  sql <- pkg_method("sql", "dplyr")
  sep <- escape(sep, con = con)
  pieces <- vapply(list(...), escape, con = con, character(1))
  sql(paste(pieces, collapse = paste0('||', sep, '||')))
}

athena_regexpr <- function(pattern, text, ignore.case = FALSE, perl = FALSE, fixed = FALSE, 
                           useBytes = FALSE){
  if (any(c(perl, fixed, useBytes))) {
    stop("`perl`, `fixed` and `useBytes` parameters are unsupported", call. = F)
  }
  build_sql <- pkg_method('build_sql', "dbplyr")
  if(!ignore.case){
    build_sql('REGEXP_LIKE(', text,",", pattern, ')')
  } else {
    pattern <- paste0("(?i)", pattern)
    build_sql('REGEXP_LIKE(', text,",", pattern, ')')
  }
}
