format_athena_types <- function(athena_types){
  data_type <- tolower(vapply(athena_types, function(x) x$Type, FUN.VALUE = character(1)))
  names(data_type) <- vapply(athena_types, function(x) x$Name, FUN.VALUE = character(1))
  return(data_type)
}

# ==========================================================================
# read in method
athena_read <- function(method, File, athena_types, ...) {
  UseMethod("athena_read")
}

athena_read.athena_data.table <-
  function(method, File, athena_types , ...){
    data_type <- format_athena_types(athena_types)
    
    Type2 <- Type <- AthenaToRDataType(method, data_type)
    # Type2 is to handle issue with data.table fread 
    Type2[Type2 %in% "POSIXct"] <- "character"
    
    fill <- any(c("array", "row", "map", "json") %in% data_type)
    
    # currently parameter data.table is left as default. If users require data.frame to be returned then parameter will be updated
    output <- data.table::fread(File, col.names = names(Type2), colClasses = unname(Type2), sep = ",", showProgress = F, na.strings="", fill = fill)
    # formatting POSIXct: from string to POSIXct
    for (col in names(Type[Type %in% "POSIXct"])) set(output, j=col, value=as.POSIXct(output[[col]]))
    # AWS Athena returns " values as "". Due to this "" will be reformatted back to "
    for (col in names(Type[Type %in% "character"])) set(output, j=col, value=gsub('""' , '"', output[[col]]))
    
    # convert raw
    if(!identical(athena_option_env$binary, "character"))
      raw_parser(output, data_type)
    
    # convert json
    if(!identical(athena_option_env$json, "character"))
      json_parser(output, data_type)
    
    return(output)
  }

athena_read.athena_vroom <- 
  function(method, File, athena_types, ...){
    data_type <- format_athena_types(athena_types)
    
    vroom <- pkg_method("vroom", "vroom")
    Type <- AthenaToRDataType(method, data_type)

    output <- vroom(File, delim = ",", col_types = Type, progress = FALSE, trim_ws = FALSE, altrep = TRUE)
    
    # convert raw
    if(!identical(athena_option_env$binary, "character"))
      raw_parser(output, data_type)
    
    # convert json
    if(!identical(athena_option_env$json, "character"))
      json_parser(output, data_type)
    
    return(output)
  }

# Read in .txt files line by line and return them as a data.frame
athena_read_lines <- function(method, File, athena_types, ...) {
  UseMethod("athena_read_lines")
}

# Keep data.table formatting
athena_read_lines.athena_data.table <-
  function(method, File, athena_types, ...){
    data_type <- format_athena_types(athena_types)
    
    Type2 <- Type <- AthenaToRDataType(method, data_type)
    # Type2 is to handle issue with data.table fread 
    Type2[Type2 %in% "POSIXct"] <- "character"
    
    # currently parameter data.table is left as default. If users require data.frame to be returned then parameter will be updated
    output <- data.table::fread(File, col.names = names(Type2), colClasses = unname(Type2), sep = "\n", showProgress = F, na.strings="", header = F, strip.white= F)
    # formatting POSIXct: from string to POSIXct
    for (col in names(Type[Type %in% "POSIXct"])) set(output, j=col, value=as.POSIXct(output[[col]]))
    # AWS Athena returns " values as "". Due to this "" will be reformatted back to "
    for (col in names(Type[Type %in% "character"])) set(output, j=col, value=gsub('""' , '"', output[[col]]))
    
    # convert raw
    if(!identical(athena_option_env$binary, "character"))
      raw_parser(output, data_type)
    
    # convert json
    if(!identical(athena_option_env$json, "character"))
      json_parser(output, data_type)
    
    return(output)
  }

athena_read_lines.athena_vroom <- 
  function(method, File, athena_types, ...){
    data_type <- format_athena_types(athena_types)
    
    vroom <- pkg_method("vroom", "vroom")
    
    Type <- AthenaToRDataType(method, data_type)
    
    output <- vroom(File, col_names = names(Type), col_types = unname(Type), progress = FALSE, altrep = TRUE, trim_ws = FALSE, delim = "\n")
    
    # convert raw
    if(!identical(athena_option_env$binary, "character"))
      raw_parser(output, data_type)
    
    # convert json
    if(!identical(athena_option_env$json, "character"))
      json_parser(output, data_type)
    
    return(output)
  }

# ==========================================================================
# write method
write_batch <- function(value, split_vec, fun, max.batch, max_row, path, file.type, compress, ...){
  
  # create temp file
  File <- paste(uuid::UUIDgenerate(), Compress(file.type, compress), sep = ".")
  path <- file.path(path, File)
  file_con <- if(file.type != "json") path else file(path)
  
  # split data.frame into chunks
  chunk <- value[split_vec:min(max_row,(split_vec+max.batch-1)),]
  
  # write 
  fun(chunk, file_con, ...)
  
  path
}

dt_split <- function(value, max.batch, file.type, compress){
  
  if((file.type %in% c("parquet", "json") & is.infinite(max.batch)) || 
     (file.type %in% c("csv", "tsv") & !compress & is.infinite(max.batch))) 
    max.batch <- nrow(value)
  
  # set up split vec
  max_row <- nrow(value)
  split_20 <- .05 * max_row # default currently set to 20 split: https://github.com/DyfanJones/RAthena/issues/36
  min.batch = 1000000 # min.batch sized at 1M
  
  # if batch is set to default
  if(is.infinite(max.batch)){
    max.batch <- max(split_20, min.batch)
    split_vec <- seq(1, max_row, max.batch)
  }
  
  # if max.batch is set by user
  if(!is.infinite(max.batch)) split_vec <- seq(1, max_row, as.integer(max.batch))
  return(list(SplitVec = split_vec, MaxBatch = max.batch, MaxRow = max_row))
}

update_args <- function(file.type = "tsv", init_args = list(), compress = FALSE){
  if(file.type ==  "parquet"){
    write_parquet <- pkg_method("write_parquet", "arrow")
    cp <- if(compress) "snappy" else NULL
    init_args <- c(init_args,
                   fun = write_parquet,
                   use_deprecated_int96_timestamps = TRUE, # align POSIXct to athena timestamp: https://docs.aws.amazon.com/athena/latest/ug/data-types.html
                   list(compression = cp))
  } else if(file.type == "json") {
    stream_out <- pkg_method("stream_out", "jsonlite")
    init_args <- c(init_args,
                   fun = stream_out,
                   verbose = FALSE)
  } else if(class(athena_option_env$file_parser) == "athena_data.table") {
    init_args <- c(init_args,
                   fun = data.table::fwrite,
                   quote = FALSE,
                   showProgress = FALSE)
    if(file.type == "csv")
      init_args <- c(init_args,
                     sep = ",")
    if(file.type == "tsv")
      init_args <- c(init_args,
                     sep = "\t")
  } else if(class(athena_option_env$file_parser) == "athena_vroom"){
    vroom_write <- pkg_method("vroom_write", "vroom")
    init_args <- c(init_args,
                   fun = vroom_write,
                   quote = "none",
                   progress = FALSE,
                   escape = "none")
    if(file.type == "csv")
      init_args <- c(init_args,
                     delim = ",")
    if(file.type == "tsv")
      init_args <- c(init_args,
                     delim = "\t")
  }
  return(init_args)
}

# To enable vroom to compress files in parrallel then pigz is required: https://zlib.net/pigz/. 
# pipe(sprintf("pigz > %s", path))