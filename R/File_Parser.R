# ==========================================================================
# read in method
athena_read <- function(method, File, ...) {
  UseMethod("athena_read")
}

athena_read.athena_data.table <-
  function(method, File, athena_types , ...){
    Type2 <- Type <- AthenaToRDataType(method, athena_types)
    # Type2 is to handle issue with data.table fread 
    Type2[Type2 %in% "POSIXct"] <- "character"
    
    # currently parameter data.table is left as default. If users require data.frame to be returned then parameter will be updated
    output <- data.table::fread(File, col.names = names(Type2), colClasses = unname(Type2), sep = ",", showProgress = F, na.strings="")
    # formatting POSIXct: from string to POSIXct
    for (col in names(Type[Type %in% "POSIXct"])) set(output, j=col, value=as.POSIXct(output[[col]]))
    # AWS Athena returns " values as "". Due to this "" will be reformatted back to "
    for (col in names(Type[Type %in% "character"])) set(output, j=col, value=gsub('""' , '"', output[[col]]))
    
    output
  }

athena_read.athena_vroom <- 
  function(method, File, athena_types, ...){
    vroom <- pkg_method("vroom", "vroom")
    as.integer64 <- pkg_method("as.integer64", "bit64")
    Type2 <- Type <- AthenaToRDataType(method, athena_types)
    
    # Type2 is to bigint
    Type2[Type2 %in% "i64"] <- "n"
    
    output <- vroom(File, delim = ",", col_types = Type2, progress = FALSE,trim_ws = FALSE, altrep_opts = TRUE)
    
    # convert numeric class to integer64 to match athena's bigint
    for (var in names(Type[Type %in% "i64"])) output[[var]] <- as.integer64(output[[var]])
    
    output
  }

# ==========================================================================
# write method
split_data <- function(method, x, ...){
  UseMethod("split_data")
}

split_data.athena_data.table <- function(method, x, max.batch = Inf, path = tempdir(), 
                                         sep = ",", compress = T, file.type = "csv"){
  # Bypass splitter if not compressed
  if(!compress){
    file <- paste(paste(sample(letters, 10, replace = TRUE), collapse = ""), Compress(file.type, compress), sep = ".")
    path <- file.path(path, file)
    fwrite(x, path, sep = sep, quote = FALSE, showProgress = FALSE)
    return(path)}
  
  # set up split vec
  max_row <- nrow(x)
  split_10 <- .05 * max_row # default currently set to 20 split: https://github.com/DyfanJones/RAthena/issues/36
  min.batch = 1000000 # min.batch sized at 1M
  
  # if batch is set to default
  if(is.infinite(max.batch)){
    max.batch <- max(split_10, min.batch)
    split_vec <- seq(1, max_row, max.batch)
  }
  
  # if max.batch is set by user
  if(!is.infinite(max.batch)) split_vec <- seq(1, max_row, as.integer(max.batch))
  
  sapply(split_vec, write_batch_DT, dt = x, max.batch = max.batch,
         max_row= max_row, path = path, sep = sep, file.type= file.type)
}

# write data.frame by batch data.table
write_batch_DT <- function(split_vec, dt, max.batch, max_row, path, sep, file.type){
  sample <- dt[split_vec:min(max_row,(split_vec+max.batch-1)),]
  file <- paste(paste(sample(letters, 10, replace = TRUE), collapse = ""), Compress(file.type, TRUE), sep = ".")
  path <- file.path(path, file)
  fwrite(sample, path, sep = sep, quote = FALSE, showProgress = FALSE)
  path
}

split_data.athena_vroom <- function(method, x, max.batch = Inf, path = tempdir(), 
                                    sep = ",", compress = T, file.type = "csv"){
  vroom_write <- pkg_method("vroom_write", "vroom")
  # Bypass splitter if not compressed
  if(!compress){
    file <- paste(paste(sample(letters, 10, replace = TRUE), collapse = ""), Compress(file.type, compress), sep = ".")
    path <- file.path(path, file)
    vroom_write(x, path, delim = sep, quote = "none", progress = FALSE, escape = "none")
    return(path)}
  
  # set up split vec
  max_row <- nrow(x)
  split_10 <- .05 * max_row # default currently set to 20 split: https://github.com/DyfanJones/RAthena/issues/36
  min.batch = 1000000 # min.batch sized at 1M
  
  # if batch is set to default
  if(is.infinite(max.batch)){
    max.batch <- max(split_10, min.batch)
    split_vec <- seq(1, max_row, max.batch)
  }
  
  # if max.batch is set by user
  if(!is.infinite(max.batch)) split_vec <- seq(1, max_row, as.integer(max.batch))
  
  sapply(split_vec, write_batch_vroom, dt = x, fun = vroom_write, max.batch = max.batch,
         max_row= max_row, path = path, sep = sep, file.type= file.type)
}

# write data.frame by batch vroom
write_batch_vroom <- function(split_vec, dt, fun, max.batch, max_row, path, sep, file.type){
  sample <- dt[split_vec:min(max_row,(split_vec+max.batch-1)),]
  file <- paste(paste(sample(letters, 10, replace = TRUE), collapse = ""), Compress(file.type, TRUE), sep = ".")
  path <- file.path(path, file)
  fun(sample,  path, delim = sep, quote = "none", progress = FALSE, escape = "none")
  path
}

# To enable vroom to compress files in parrallel then pigz is required: https://zlib.net/pigz/. 
# pipe(sprintf("pigz > %s", path))