AthenaDataType <-
  function(fields, ...) {
    switch(
      class(fields)[1],
      logical =   "BOOLEAN",
      integer =   "INT",
      integer64 = "BIGINT",
      numeric =   "DOUBLE",
      double =    "DOUBLE",
      factor =    "STRING",
      character = "STRING",
      list =      "STRING",
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
    athena_to_r <- function(x){
      switch(x,
             boolean = "logical",
             int ="integer",
             integer = "integer",
             tinyint = "integer",
             smallint = "integer",
             bigint = athena_option_env$bigint,
             float = "double",
             real = "double",
             decimal = "double",
             string = "character",
             varchar = "character",
             char = "character",
             date = "Date",
             timestamp = "POSIXct",
             array = "character",
             row = "character",
             map = "character",
             json = "character",
             ipaddress = "character",
             varbinary = "character",
             x)}
    output <- vapply(data_type, athena_to_r, FUN.VALUE = character(1))
    output
  }

AthenaToRDataType.athena_vroom <- 
  function(method, data_type){
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
             real = "d",
             decimal = "d",
             string = "c",
             varchar = "c",
             char = "c",
             date = "D",
             timestamp = "T",
             array = "c",
             row = "c",
             map = "c",
             json = "c",
             ipaddress = "c",
             varbinary = "c",
             x)}
    output <- vapply(data_type, athena_to_r, FUN.VALUE = character(1))
    output
  }