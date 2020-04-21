#' @include Connection.R
NULL

#' AWS Athena backend dbplyr 
#' 
#' Create s3 implementation of \code{sql_translate_env} for AWS Athena sql translate environment based off
#' \href{https://docs.aws.amazon.com/athena/latest/ug/data-types.html}{Athena Data Types} and 
#' \href{https://docs.aws.amazon.com/athena/latest/ug/functions-operators-reference-section.html}{DML Queries, Functions, and Operators}
#' @param con An \code{\linkS4class{AthenaConnection}} object, produced by
#'   [DBI::dbConnect()]
#' @param x An object to escape. Existing sql vectors will be left as is,
#'   character vectors are escaped with single quotes, numeric vectors have
#'   trailing `.0` added if they're whole numbers, identifiers are
#'   escaped with double quotes.
#' @name sql_translate_env
NULL

athena_window_functions <- function() {
  # using pkg_method to retrieve external pkg methods
  base_win <- pkg_method('base_win', "dbplyr")
  sql_translator <- pkg_method('sql_translator', "dbplyr")
  win_absent <- pkg_method('win_absent', "dbplyr")
  win_recycled <- pkg_method('win_recycled', "dbplyr")
  return(sql_translator(
    .parent=base_win,
    all=win_recycled('bool_and'),
    any=win_recycled('bool_or'),
    n_distinct=win_absent('n_distinct'),
    sd=win_recycled("stddev_samp")
  ))
}


#' @rdname sql_translate_env
#' @export
sql_translate_env.AthenaConnection <- function(con) {
  sql_variant <- pkg_method("sql_variant", "dbplyr")
  sql_translator <- pkg_method("sql_translator", "dbplyr")
  sql_prefix <- pkg_method("sql_prefix", "dbplyr")
  sql_cast <- pkg_method('sql_cast', "dbplyr")
  sql <- pkg_method('sql', "dplyr")
  build_sql <- pkg_method('build_sql', "dbplyr")
  base_scalar <- pkg_method('base_scalar', "dbplyr")
  base_agg <- pkg_method('base_agg', "dbplyr")
  
  sql_variant(
    sql_translator(.parent = base_scalar,
                   ifelse = sql_prefix("IF"),
                   as = function(column, type) {
                     sql_type <- toupper(dbDataType(athena(), type)) # using toupper to keep dependencies low
                     build_sql('CAST(', column, ' AS ', sql(sql_type), ')')
                   },
                   as.character = sql_cast("VARCHAR"), # Varchar types are created with a length specifier (between 1 and 65535)
                   as.numeric = sql_cast("DOUBLE"),
                   as.double = sql_cast("DOUBLE"),
                   as.integer = sql_cast("INTEGER"), # https://docs.aws.amazon.com/athena/latest/ug/data-types.html#type-integer
                   as.integer64 = sql_cast("BIGINT"), # as.integer64 reflects bigint for AWS Athena
                   as.Date = sql_cast("DATE"),
                   as.logical = sql_cast("BOOLEAN"),
                   as.raw = sql_cast("VARBINARY"),
                   tolower = sql_prefix("LOWER"),
                   toupper = sql_prefix("UPPER"),
                   pmax = sql_prefix("GREATEST"),
                   pmin = sql_prefix("LEAST"),
                   is.finite = sql_prefix("IS_FINITE"),
                   is.infinite = sql_prefix("IS_FINITE"),
                   is.nan = sql_prefix("IS_NAN"),
                   paste0 = sql_prefix("CONCAT"),
                   paste = function(..., sep = " ") athena_paste(list(...), sep = sep, con = con),
                   `[[` = function(x, i) {
                     if (is.numeric(i) && all.equal(i, as.integer(i))) {
                       i <- as.integer(i)
                     }
                     build_sql(x, "[", i, "]")
                   }
    ),
    sql_translator(.parent = base_agg,
                   n = function() sql("COUNT(*)"),
                   sd =  sql_prefix("STDDEV_SAMP"),
                   var = sql_prefix("VAR_SAMP"),
                   all = sql_prefix("BOOL_AND"),
                   any = sql_prefix("BOOL_OR")
    ),
    athena_window_functions()
  )
}

# helper function to support R function paste in sql_translation_env
athena_paste <- function(..., sep = " ", con) {
  escape <- pkg_method("escape", "dbplyr")
  sql <- pkg_method("sql", "dplyr")
  sep <- paste0('||', escape(sep, con = con), '||')
  pieces <- vapply(list(...), escape, con = con,collapse = sep, character(1))
  sql(paste(pieces))
}

# Athena specifc S3 method for converting date variables and iso formateed date strings to date literals
#' @rdname sql_translate_env
#' @export
sql_escape_string.AthenaConnection <- function(con, x) {
  # Added string restiction to prevent timestamps wrongly added to date format
  all_dates <- all(try(as.Date(x, tryFormats = "%Y-%m-%d"), silent=T) == x) & all(nchar(x) == 10)
  if(all_dates & !is.na(all_dates)) {
    paste0('date ', DBI::dbQuoteString(con, x))
  } else {
    DBI::dbQuoteString(con, x)
  }
}