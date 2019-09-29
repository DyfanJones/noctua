AthenaDataType <-
  function(fields, ...) {
    switch(
      class(fields)[1],
      logical =   "BOOLEAN",
      integer =   "INT",
      integer64 = "BIGINT",
      numeric =   "FLOAT",
      factor =    "STRING",
      character = "STRING",
      list = "STRING",
      Date =      "DATE",
      POSIXct =   "TIMESTAMP",
      stop("Unknown class ", paste(class(fields), collapse = "/"), call. = FALSE)
    )
  }


AthenaToRDataType <- function(data_type){
  Types <- tolower(sapply(data_type, function(x) x$Type))
  athena_to_r <- function(x){
    switch(x,
           boolean = "logical",
           int ="integer",
           integer = "integer",
           tinyint = "integer",
           smallint = "integer",
           bigint = "integer",
           float = "double",
           decimal = "double",
           string = "character",
           varchar = "character",
           date = "Date",
           timestamp = "POSIXct",
           x)}
  vapply(Types, athena_to_r, FUN.VALUE = character(1))
}