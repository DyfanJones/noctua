AthenaDataType <-
  function(fields, ...) {
    switch(
      class(fields)[1],
      logical =   "BOOLEAN",
      integer =   "INT",
      integer64 = "BIGINT",
      numeric =   "DOUBLE",
      double = "DOUBLE",
      factor =    "STRING",
      character = "STRING",
      list = "STRING",
      Date =      "DATE",
      POSIXct =   "TIMESTAMP",
      stop("Unknown class ", paste(class(fields), collapse = "/"), call. = FALSE)
    )
  }

# ==========================================================================
# convert Athena types to R classes
AthenaToRDataType <- function(method, data_type) UseMethod("AthenaToRDataType")

AthenaToRDataType.athena_data.table <- 
  function(method, data_type){
    Names <- sapply(data_type, function(x) x$Name)
    Types <- tolower(sapply(data_type, function(x) x$Type))
    athena_to_r <- function(x){
      switch(x,
             boolean = "logical",
             int ="integer",
             integer = "integer",
             tinyint = "integer",
             smallint = "integer",
             bigint = athena_option_env$bigint,
             float = "double",
             decimal = "double",
             string = "character",
             varchar = "character",
             date = "Date",
             timestamp = "POSIXct",
             x)}
    output <- vapply(Types, athena_to_r, FUN.VALUE = character(1))
    names(output) <- Names
    output
  }

AthenaToRDataType.athena_vroom <- 
  function(method, data_type){
    Names <- sapply(data_type, function(x) x$Name)
    Types <- tolower(sapply(data_type, function(x) x$Type))
    athena_to_r <- function(x){
      switch(x,
             boolean = "l",
             int ="i",
             integer = "i",
             tinyint = "i",
             smallint = "i",
             bigint = athena_option_env$bigint,
             double = "d",
             float = "d",
             decimal = "d",
             string = "c",
             varchar = "c",
             date = "D",
             timestamp = "T",
             x)}
    output <- vapply(Types, athena_to_r, FUN.VALUE = character(1))
    names(output) <- Names
    output
  }