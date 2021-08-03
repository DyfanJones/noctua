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

#' @rdname sql_translate_env
#' @export
sql_translate_env.AthenaConnection <- function(con) {
  # base methods
  sql_variant <- pkg_method("sql_variant", "dbplyr")
  sql_translator <- pkg_method("sql_translator", "dbplyr")
  sql_prefix <- pkg_method("sql_prefix", "dbplyr")
  sql_cast <- pkg_method('sql_cast', "dbplyr")
  sql <- pkg_method('sql', "dplyr")
  build_sql <- pkg_method('build_sql', "dbplyr")
  
  # scalar methods
  base_scalar <- pkg_method('base_scalar', "dbplyr")
  
  # aggregate methods
  base_agg <- pkg_method('base_agg', "dbplyr")
  sql_aggregate <- pkg_method("sql_aggregate", "dbplyr")
  sql_aggregate_2 <- pkg_method("sql_aggregate_2", "dbplyr")
  
  # window methods
  base_win <- pkg_method('base_win', "dbplyr")
  win_aggregate <- pkg_method('win_aggregate', "dbplyr")
  win_aggregate_2 <- pkg_method('win_aggregate_2', "dbplyr")
  
  sql_variant(
    sql_translator(.parent = base_scalar,
      ifelse = sql_prefix("IF"),
      Sys.Date = function() build_sql("current_date"),
      Sys.time = function(tz=NULL){
        if(is.null(tz))
          build_sql("now()")
        else
          build_sql("now() at time zone ", tz)
      },
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
      as.POSIXct = function(x, tz=NULL){
        if(is.null(tz))
          build_sql("timestamp ", x)
        else
          build_sql("timestamp ", x, " at time zone ", tz)
      },
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
      paste = function(..., sep = " ") athena_paste(..., sep = sep, con = con),
      `[[` = function(x, i) {
        if (is.numeric(i) && all.equal(i, as.integer(i))) {
          i <- as.integer(i)
        }
        build_sql(x, "[", i, "]")
      },
      grepl = athena_regexpr,
      regexpr = athena_regexpr,
      
      # stringr functions:
      # https://prestodb.io/docs/current/functions/string.html
      # https://prestodb.io/docs/current/functions/regexp.html
      str_c = function(..., sep = "") athena_paste(..., sep = sep, con = con),
      str_locate = function(string, pattern) {
        build_sql("strpos(",string, ",", pattern, ")")
      },
      str_detect = function(string, pattern, negate = FALSE){
        if(isTRUE(negate)){
          build_sql('NOT REGEXP_LIKE(', string,",", pattern, ')')
        } else {
          build_sql('REGEXP_LIKE(', string,",", pattern, ')')
        }
      },
      # Currently `str_replace` replaces every instance 
      # of the substring matched by the regular expression pattern in string with replacement
      # https://prestodb.io/docs/current/functions/regexp.html#id2
      str_replace = function(string, pattern, replacement){
        build_sql("regexp_replace(",string, ",", pattern, ",", replacement, ")")
      },
      str_replace_all = function(string, pattern, replacement){
        build_sql("regexp_replace(",string, ",", pattern, ",", replacement, ")")
      },
      str_squish = function(string){
        build_sql("trim(", "regexp_replace(", string,", '\\s+', ' '))")
      },
      # Currently `str_remove` replaces every instance 
      # of the substring matched by the regular expression pattern in string with replacement
      # https://prestodb.io/docs/current/functions/regexp.html#id2
      str_remove =  function(string, pattern){
        build_sql("regexp_replace(",string, ",", pattern, ")")
      },
      str_remove_all =  function(string, pattern){
        build_sql("regexp_replace(",string, ",", pattern, ")")
      },
      str_split = function(string, pattern, n = Inf, simplify = FALSE){
        if(simplify)
          stop("`simplify` is not supported in Athena.")
        build_sql("regexp_split(", string, ",", pattern, ")")
      },
      str_extract = function(string, pattern){
        build_sql("regexp_extract(",string, ",", pattern, ")")
      },
      str_extract_all = function(string, pattern, simplify = FALSE){
        if(simplify)
          stop("`simplify` is not supported in Athena.")
        build_sql("regexp_extract_all(",string, ",", pattern, ")")
      },
      
      # lubridate functions
      month = function(x, label = FALSE, abbr = TRUE){
        if(!label){
          build_sql("date_format(", x, ", '%m')")
        } else {
          if(abbr)
            build_sql("date_format(", x, ", '%b')")
          else
            build_sql("date_format(", x, ", '%M')")
        }
      },
      quarter = function(x, with_year = FALSE, fiscal_start = 1) {
        if (fiscal_start != 1) {
          stop("`fiscal_start` is not supported in Athena translation. Must be 1.", call. = FALSE)
        }
        if (with_year) {
          build_sql("year(",x,")"," || '.' || ", "quarter(", x, ")")
        } else {
          build_sql("quarter(", x, ")")
        }
      },
      wday = function(x, label = FALSE, abbr = TRUE, week_start = NULL) {
        if(!label){
          week_start <- week_start %||% getOption("lubridate.week.start", 7)
          offset <- as.integer(7 - week_start)
          build_sql("(dow(", x, ") + ", offset, ") %7 + 1") # developed from data.table::wday
        } else if (label && !abbr){
          build_sql("format_datetime(", x, ", 'EEEE')")
        } else if (label && abbr){
          build_sql("format_datetime(", x, ", 'E')")
        } else {
          stop("Unrecognized arguments to `wday`", call. = FALSE)
        }
      },
      yday = function(x){
        build_sql("doy(", x, ")")
      },
      
      # https://prestodb.io/docs/current/functions/datetime.html#date-and-time-operators
      seconds = function(x){
        x <- as.character(as.integer(x))
        build_sql("INTERVAL ", x, " second")
      },
      minutes = function(x){
        x <- as.character(as.integer(x))
        build_sql("INTERVAL ", x, " minute")
      },
      hours = function(x){
        x <- as.character(as.integer(x))
        build_sql("INTERVAL ", x, " hour")
      },
      days = function(x){
        x <- as.character(as.integer(x))
        build_sql("INTERVAL ", x, " day")
      },
      weeks = function(x){
        x <- as.character(as.integer(x))
        build_sql("INTERVAL ", x, " week")
      },
      months = function(x){
        x <- as.character(as.integer(x))
        build_sql("INTERVAL ", x, " month")
      },
      years = function(x){
        x <- as.character(as.integer(x))
        build_sql("INTERVAL ", x, " year")
      },
      floor_date = function(x, unit = "second"){
        unit <- match.arg(unit,
          c("second", "minute", "hour", "day", "week", "month", "quarter", "year")
        )
        build_sql("date_trunc(", unit, ", ",  x, ")")
      },
      today = function() {build_sql("current_date")},
      as_date = sql_cast("DATE"),
      as_datetime = function(x, tz = NULL){
        if(is.null(tz))
          build_sql("timestamp ", x)
        else
          build_sql("timestamp ", x, " at time zone ", tz)
      },
      now = function(tz=NULL){
        if(is.null(tz))
          build_sql("now()")
        else
          build_sql("now() at time zone ", tz)
      }
    ),
    
    # Align aggregate functions to Postgres
    # https://github.com/tidyverse/dbplyr/blob/master/R/backend-postgres.R#L186-L191
    sql_translator(.parent = base_agg,
      cor = sql_aggregate_2("CORR"),
      cov = sql_aggregate_2("COVAR_SAMP"),
      sd = sql_aggregate("STDDEV_SAMP", "sd"),
      var = sql_aggregate("VAR_SAMP", "var"),
      all = sql_aggregate("BOOL_AND", "all"),
      any = sql_aggregate("BOOL_OR", "any"),
      quantile = function(x, probs){
        check_probs(probs)
        build_sql("APPROX_PERCENTILE(",x,", ",probs,")")
      },
      median = function(x){build_sql("APPROX_PERCENTILE(",x,", 0.5)")}
    ),
    
    # Align window functions to Postgres
    # https://github.com/tidyverse/dbplyr/blob/master/R/backend-postgres.R#L194-L200
    sql_translator(
      .parent=base_win,
      cor = win_aggregate_2("CORR"),
      cov = win_aggregate_2("COVAR_SAMP"),
      sd =  win_aggregate("STDDEV_SAMP"),
      var = win_aggregate("VAR_SAMP"),
      all = win_aggregate("BOOL_AND"),
      any = win_aggregate("BOOL_OR"),
      quantile = function(x, probs){
        check_probs(probs)
        build_sql("APPROX_PERCENTILE(",x,", ",probs,")")
      },
      median = function(x){build_sql("APPROX_PERCENTILE(",x,", 0.5)")}
    )
  )
}

# re-create check_probs from dbplyr:
# https://github.com/tidyverse/dbplyr/blob/47e53ce30402d41ae4b38c803de12e63d64a9b6c/R/translate-sql-quantile.R#L40-L48
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
