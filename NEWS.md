# noctua 2.0.1.999
## Bug Fix:
* `dbIsValid` wrongly stated connection is valid for result class when connection class was disconnected. 

# noctua 2.0.1
## Bug Fix:
* Fix issue of keyboard interrupt failing to raise interrupt error.

# noctua 2.0.0
## API Change
* `AthenaConnection` class: `ptr` and `info` slots changed from `list` to `environment` with in `AthenaConnect` class. Allows class to be updated by reference. Simplifies notation when viewing class from RStudio environment tab.
* `AthenaResult` class: `info` slot changed from `list` to `environment`. Allows class to be updated by reference. 

By utilising environments for `AthenaConnection` and `AthenaResult`, all `AthenaResult` classes created from `AthenaConnection` will point to the same `ptr` and `info` environments for it's connection. Previously `ptr` and `info` would make a copy. This means if it was modified it would not affect the child or parent class for example:

```r
# Old Method
library(DBI)
con <- dbConnect(noctua::athena(),
                 rstudio_conn_tab = F)

res <- dbExecute(con, "select 'helloworld'")

# modifying parent class to influence child
con@info$made_up <- "helloworld"

# nothing happened
res@connection@info$made_up
# > NULL

# modifying child class to influence parent
res@connection@info$made_up <- "oh no!"

# nothing happened
con@info$made_up
# > "helloworld"

# New Method
library(DBI)
con <- dbConnect(noctua::athena(),
                 rstudio_conn_tab = F)

res <- dbExecute(con, "select 'helloworld'")

# modifying parent class to influence child
con@info$made_up <- "helloworld"

# picked up change
res@connection@info$made_up
# > "helloworld"

# modifying child class to influence parent
res@connection@info$made_up <- "oh no!"

# picked up change
con@info$made_up
# > "oh no!"
```

## New Feature
* Added support to `AWS Athena` data types `[array, row, map, json, binary, ipaddress]` (#135). Conversion types can be changed through `dbConnect` and `noctua_options`.
```r
library(DBI)
library(noctua)

# default conversion methods
con <- dbConnect(noctua::athena())

# change json conversion method
noctua_options(json = "character")
noctua:::athena_option_env$json
# [1] "character"

# change json conversion to custom method
noctua_options(json = jsonify::from_json)
noctua:::athena_option_env$json
# function (json, simplify = TRUE, fill_na = FALSE, buffer_size = 1024) 
# {
#   json_to_r(json, simplify, fill_na, buffer_size)
# }
# <bytecode: 0x7f823b9f6830>
#   <environment: namespace:jsonify>

# change bigint conversion without affecting custom json conversion methods
noctua_options(bigint = "numeric")
noctua:::athena_option_env$json
# function (json, simplify = TRUE, fill_na = FALSE, buffer_size = 1024) 
# {
#   json_to_r(json, simplify, fill_na, buffer_size)
# }
# <bytecode: 0x7f823b9f6830>
#   <environment: namespace:jsonify>

noctua:::athena_option_env$bigint
# [1] "numeric"

# change binary conversion without affect, bigint or json methods
noctua_options(binary = "character")
noctua:::athena_option_env$json
# function (json, simplify = TRUE, fill_na = FALSE, buffer_size = 1024) 
# {
#   json_to_r(json, simplify, fill_na, buffer_size)
# }
# <bytecode: 0x7f823b9f6830>
#   <environment: namespace:jsonify>

noctua:::athena_option_env$bigint
# [1] "numeric"

noctua:::athena_option_env$binary
# [1] "character"

# no conversion for json objects
con2 <- dbConnect(noctua::athena(), json = "character")

# use custom json parser
con <- dbConnect(noctua::athena(), json = jsonify::from_json)
```
* Allow users to turn off RStudio Connection Tab when working in RStudio (#136). This can be done through parameter `rstudio_conn_tab` within `dbConnect`.

## Bug Fix
* `AWS Athena` uses `float` data type for the DDL only, `noctua` was wrongly parsing `float` data type back to R. Instead `AWS Athena` uses data type `real` in SQL functions like `select cast` https://docs.aws.amazon.com/athena/latest/ug/data-types.html. `noctua` now correctly parses `real` to R's data type `double` (#133)
* Iterate through each token `AWS` returns to get all results from `AWS Glue` catalogue (#137)

# noctua 1.10.0
## New Feature
* Added optional formatting to `dbGetPartition`. This simply tidies up the default AWS Athena partition format.
```r
library(DBI)
library(noctua)
con <- dbConnect(athena())
dbGetPartition(con, "test_df2", .format = T)
# Info: (Data scanned: 0 Bytes)
#    year month day
# 1: 2020    11  17
dbGetPartition(con, "test_df2")
# Info: (Data scanned: 0 Bytes)
#                    partition
# 1: year=2020/month=11/day=17
```
* Support different formats for returning `bigint`, this is to align with other DBI interfaces i.e. `RPostgres`. Now `bigint` can be return in the possible formats: ["integer64", "integer", "numeric", "character"]
```
library(DBI)
con <- dbConnect(noctua::athena(), bigint = "numeric")
```
When switching between the different file parsers the `bigint` to be represented according to the file parser i.e. `data.table`: "integer64" -> `vroom`: "I".

## Bug Fix
* `dbRemoveTable`: Check if key has "." or ends with "/" before adding "/" to the end (#125)
* Added `uuid` minimum version to fix issue (#128)

## Documentation
* Added note to dbRemoveTable doc string around aws athena table Location in Amazon S3.

# noctua 1.9.1
## Note
* Added package checks to unit tests when testing a suggested dependency. This is to fix "CRAN Package Check Results for Package noctua" for operating system "r-patched-solaris-x86". Error message:
```
Error: write_parquet requires the arrow package, please install it first and try again
```

## Bug Fix
* `dbRemoveTable` would error if AWS S3 files for Athena table have been removed:
```
Error in seq.default(1, length(l), 1000) : wrong sign in 'by' argument
```
Now a warning message will be returned:
```
Warning message:
Failed to remove AWS S3 files from: "s3://{bucket}/{prefix}/". Please check if AWS S3 files exist.
```

# noctua 1.9.0
## Minor Change
* `dbRemoveTable` now removes AWS S3 objects using `delete_objects` instead of `delete_object`. This allows `noctua` to delete AWS S3 files in batches. This will reduce the number of api calls to AWS and comes with a performance improvement.
```r
library(DBI)
library(data.table)

X <- 1010
value <- data.table(x = 1:X,
                    y = sample(letters, X, replace = T), 
                    z = sample(c(TRUE, FALSE), X, replace = T))

con <- dbConnect(noctua::athena())

# create a removable table with 1010 parquet files in AWS S3.
dbWriteTable(con, "rm_tbl", value, file.type = "parquet", overwrite = T, max.batch = 1)

# old method: delete_object
system.time({dbRemoveTable(con, "rm_tbl", confirm = T)})
# user  system elapsed 
# 31.004   8.152 115.906 

# new method: delete_objects
system.time({dbRemoveTable(con, "rm_tbl", confirm = T)})
# user  system elapsed 
# 17.319   0.370  22.709 
```

## New Feature
* Move `sql_escape_date` into `dplyr_integration.R` backend ([RAthena: # 121](https://github.com/DyfanJones/RAthena/issues/121)).
* Allow noctua to append to a static AWS s3 location using uuid

## Bug Fix
* parquet file.types now use parameter `use_deprecated_int96_timestamps` set to `TRUE`. This puts POSIXct data type in to `java.sql.Timestamp` compatible format, such as `yyyy-MM-dd HH:mm:ss[.f...]`. Thanks to Christian N Wolz for highlight this issue.
* When more than 1000 files exist in the back of an Athena table. `dbRemoveTable` will ask the user twice to confirm if they wish to remove the backend files:
```
Info: The S3 objects in prefix will be deleted:
  s3://bucket/path/schema/table
Info: The S3 objects in prefix will be deleted:
  s3://bucket/path/schema/table
```
To overcome this `dbRemoveTable` will opt for `paws::s3()$list_objects_v2` instead of `paws::s3()$list_objects` when listing s3 objects to be deleted. This allows `noctua` to iterate over AWS s3 prefix using tokens, instead of deleting objects in chunks.
* `s3_upload_location` simplified how s3 location is built. Now s3.location parameter isn't affected and instead only additional components e.g. name, schema and partition.
* `dbplyr v-2.0.0` function `in_schema` now wraps strings in quotes, this breaks `db_query_fields.AthenaConnection`. Now `db_query_fields.AthenaConnection` removes any quotation from the string so that it can search `AWS GLUE` for table metadata. (#117)

# noctua 1.8.1
## Bug Fix
* Pass a uuid string to `start_query_execution` parameter `ClientRequestToken`. This so that the `ClientRequestToken` is "A unique case-sensitive string used to ensure the request to create the query is idempotent (executes only once)." (#104)
* Allowed cache_size to equal 100
* Do not abort if a glue::get_tables api call fails (e.g., due to missing permissions to a specific database or an orphaned Lake Formation resource link) when retrieving a list of database tables with dbListTables, dbGetTables or in Rstudio's Connections pane. 

# noctua 1.8.0
## New Feature
* noctua now supports Keyboard Interrupt and will stop AWS Athena running the query when the query has been interrupted. To keep the functionality of AWS Athena running when `R` has been interrupt a new parameter has been added to `dbConnect`, `keyboard_interrupt`. Example:

```r
# Stop AWS Athena when R has been interrupted:

con <- dbConnect(noctua::athena())

# Let AWS Athena keep running when R has been interrupted:

con <- dbConnect(noctua::athena(),
                 keyboard_interrupt = F)
```

* Added "AWS_DEFAULT_REGION" to supported environmental variables: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-environment-variables

# noctua 1.7.1
## Minor Change
* Fixed issue where `noctua` would return a `data.frame` for utility `SQL` queries regardless of backend file parser. This is due to `AWS Athena` outputting `SQL UTILITY` queries as a text file that required to be read in line by line. Now `noctua` will return the correct data format based on file parser set in `noctua_options` for example: `noctua_options("vroom")` will return `tibbles`.

## Documentation:
* Added documentation to highlight behaviour `dbClearResult` when user doesn't have permission to delete AWS S3 objects (#96)

# noctua 1.7.0
## New Feature
* functions that collect or push to AWS S3 now have a retry capability. Meaning if API call fails then the call is retried (#79)
* `noctua_options` contains 2 new parameters to control how `noctua` handles retries.
* `dbFetch` is able to return data from AWS Athena in chunk. This has been achieved by passing `NextToken` to `AthenaResult` s4 class. This method won't be as fast `n = -1` as each chunk will have to be process into data frame format.

```r
library(DBI)
con <- dbConnect(noctua::athena())
res <- dbExecute(con, "select * from some_big_table limit 10000")
dbFetch(res, 5000)
```

* When creating/appending partitions to a table, `dbWriteTable` opts to use `alter table` instead of standard `msck repair table`. This is to improve performance when appending to tables with high number of existing partitions.
* `dbWriteTable` now allows json to be appended to json ddls created with the Openx-JsonSerDe library.
* `dbConvertTable` brings `dplyr::compute` functionality to base package, allowing `noctua` to use the power of AWS Athena to convert tables and queries to more efficient file formats in AWS S3 ([RAthena: # 37](https://github.com/DyfanJones/RAthena/issues/37)).
* Extended `dplyr::compute` to give same functionality of `dbConvertTable`
* Added `region_name` check before making a connection to AWS Athena ([RAthena: # 110](https://github.com/DyfanJones/RAthena/issues/110))

## Bug Fix
* `dbWriteTable` would throw `throttling error` every now and again, `retry_api_call` as been built to handle the parsing of data between R and AWS S3.
* `dbWriteTable` did not clear down all metadata when uploading to `AWS Athena`

## Documentation
* `dbWriteTable` added support ddl structures for user who have created ddl's outside of `noctua`
* added vignette around how to use `noctua` retry functionality
* Moved all examples requiring credentials to `\dontrun` (#91)

# noctua 1.6.0
## New Feature
* Inspired by `pyathena`, `noctua_options` now has a new parameter `cache_size`. This implements local caching in R environments instead of using AWS `list_query_executions`. This is down to `dbClearResult` clearing S3's Athena output when caching isn't disabled
* `noctua_options` now has `clear_cache` parameter to clear down all cached data.
* `dbRemoveTable` now utilise `AWS Glue` to remove tables from `AWS Glue` catalog. This has a performance enhancement:

```r
library(DBI)

con = dbConnect(noctua::athena())

# upload iris dataframe for removal test
dbWriteTable(con, "iris2", iris)

# Athena method
system.time(dbRemoveTable(con, "iris2", confirm = T))
# user  system elapsed 
# 0.247   0.091   2.243 

# upload iris dataframe for removal test
dbWriteTable(con, "iris2", iris)

# Glue method
system.time(dbRemoveTable(con, "iris2", confirm = T))
# user  system elapsed 
# 0.110   0.045   1.094 
```

* `dbWriteTable` now supports uploading json lines (http://jsonlines.org/) format up to `AWS Athena` (#88).

```r
library(DBI)
con = dbConnect(noctua::athena())
dbWriteTable(con, "iris2", iris, file.type = "json")
dbGetQuery(con, "select * from iris2")
```

## Bug Fix
* `dbConnect` didn't correct pass `.internal` metadata for paws objects.
* RStudio connection tab functions:`computeHostName` & `computeDisplayName` now get region name from `info` object from `dbConnect` S4 class.
* `dbWriteTable` appending to existing table compress file type was incorrectly return.
* `Rstudio connection tab` comes into an issue when Glue Table isn't stored correctly ([RAthena: # 92](https://github.com/DyfanJones/RAthena/issues/92))

## Documentation
* Added supported environmental variable `AWS_REGION` into `dbConnect`
* Vignettes added:
  * AWS Athena Query Cache
  * AWS S3 backend
  * Changing Backend File Parser
  * Getting Started

## Unit tests:
* Increase coverage to + 80%

# noctua 1.5.1
## Bug Fix
* `writeBin`: Only 2^31 - 1 bytes can be written in a single call (and that is the maximum capacity of a raw vector on 32-bit platforms). This means that it will error out with large raw connections. To over come this `writeBin` can be called in chunks. If `readr` is available on system then `readr::write_file` is used for extra speed.

```r
library(readr)
library(microbenchmark)

# creating some dummy data for testing
X <- 1e8
df <- 
data.frame(
    w = runif(X),
    x = 1:X,
    y = sample(letters, X, replace = T), 
    z = sample(c(TRUE, FALSE), X, replace = T))
write_csv(df, "test.csv")

# read in text file into raw format
obj <- readBin("test.csv", what = "raw", n = file.size("test.csv"))

format(object.size(obj), units = "auto")
# 3.3 Gb

# writeBin in a loop
write_bin <- function(
  value,
  filename,
  chunk_size = 2L ^ 20L) {
  
  total_size <- length(value)
  split_vec <- seq(1, total_size, chunk_size)
  
  con <- file(filename, "a+b")
  on.exit(close(con))
  
  sapply(split_vec, function(x){writeBin(value[x:min(total_size,(x+chunk_size-1))],con)})
  invisible(TRUE)
}


microbenchmark(writeBin_loop = write_bin(obj, tempfile()),
               readr = write_file(obj, tempfile()),
               times = 5)

# Unit: seconds
# expr       min       lq      mean    median        uq       max neval
# R_loop 41.463273 41.62077 42.265778 41.908908 42.022042 44.313893     5
# readr  2.291571  2.40495  2.496871  2.542544  2.558367  2.686921     5
```

* Thanks to @OssiLehtinen for fixing date variables being incorrectly translated by `sql_translate_env` ([RAthena: # 44](https://github.com/DyfanJones/RAthena/issues/44))

```r
# Before
translate_sql("2019-01-01", con = con)
# '2019-01-01'

# Now
translate_sql("2019-01-01", con = con)
# DATE '2019-01-01'
```

* Dependency data.table now restricted to (>=1.12.4) due to file compression being added to `fwrite` (>=1.12.4) https://github.com/Rdatatable/data.table/blob/master/NEWS.md
* R functions `paste`/`paste0` would use default `dplyr:sql-translate-env` (`concat_ws`). `paste0` now uses Presto's `concat` function and `paste` now uses pipes to get extra flexibility for custom separating values.

```r
# R code:
paste("hi", "bye", sep = "-")

# SQL translation:
('hi'||'-'||'bye')
```

* If table exists and parameter `append` set to `TRUE` then existing s3.location will be utilised ([RAthena: # 73](https://github.com/DyfanJones/RAthena/issues/73))
* `db_compute` returned table name, however when a user wished to write table to another location ([RAthena: # 74](https://github.com/DyfanJones/RAthena/issues/74)). An error would be raised: `Error: SYNTAX_ERROR: line 2:6: Table awsdatacatalog.default.temp.iris does not exist` This has now been fixed with db_compute returning `dbplyr::in_schema`.

```r
library(DBI)
library(dplyr)

con <- dbConnect(noctua::athena())

tbl(con, "iris") %>%
  compute(name = "temp.iris")
```

* `dbListFields` didn't display partitioned columns. This has now been fixed with the call to AWS Glue being altered to include more metadata allowing for column names and partitions to be returned.
* RStudio connections tab didn't display any partitioned columns, this has been fixed in the same manner as `dbListFields`

## New Feature
* `dbStatistics` is a wrapper around `paws` `get_query_execution` to return statistics for `noctua::dbSendQuery` results
* `dbGetQuery` has new parameter `statistics` to print out `dbStatistics` before returning Athena results.
* `noctua_options`
  * Now checks if desired file parser is installed before changed file_parser method
  * File parser `vroom` has been restricted to >= 1.2.0 due to integer64 support and changes to `vroom` api
* Thanks to @OssiLehtinen for improving the speed of `dplyr::tbl` when calling Athena when using the ident method (#64): 

```r
library(DBI)
library(dplyr)

con <- dbConnect(noctua::athena())

# ident method:
t1 <- system.time(tbl(con, "iris"))

# sub query method:
t2 <- system.time(tbl(con, sql("select * from iris")))

# ident method
# user  system elapsed 
# 0.082   0.012   0.288 

# sub query method
# user  system elapsed 
# 0.993   0.138   3.660 
```
  
## Unit test
* `dplyr` sql_translate_env: expected results have now been updated to take into account bug fix with date fields
* S3 upload location: Test if the created s3 location is in the correct location

# noctua 1.5.0
## New Feature
* Added integration into Rstudio connections tab
* Added information message of amount of data scanned by AWS Athena
* Added method to change backend file parser so user can change file parser from `data.table` to `vroom`. From now on it is possible to change file parser using `noctua_options` for example:

```r
library(noctua)

noctua_options("vroom")
```

* new function `dbGetTables` that returns Athena hierarchy as a data.frame

## Unit tests
* Added data transfer unit test for backend file parser `vroom`

## Documentation
* Updated R documentation to `roxygen2` 7.0.2

# noctua 1.4.0
## Major Change
* Default delimited file uploaded to AWS Athena changed from "csv" to "tsv" this is due to separating value "," in character variables. By using "tsv" file type JSON/Array objects can be passed to Athena through character types. To prevent this becoming a breaking change `dbWriteTable` `append` parameter checks and uses existing AWS Athena DDL file type. If `file.type` doesn't match Athena DDL file type then user will receive a warning message:

```r
warning('Appended `file.type` is not compatible with the existing Athena DDL file type and has been converted to "', File.Type,'".', call. = FALSE)
```

## Bug fix
* Due to issue highlighted by @OssiLehtinen in ([RAthena: # 50](https://github.com/DyfanJones/RAthena/issues/50)), special characters have issue being processed when using flat file in the backend.
* Fixed issue where row.names not being correctly catered and returning NA in column names ([RAthena: # 41](https://github.com/DyfanJones/RAthena/issues/41))
* Fixed issue with `INTEGER` being incorrectly translated in `sql_translate_env.R`
* Fixed issue where `as.character` was getting wrongly translated ([RAthena: # 45](https://github.com/DyfanJones/RAthena/issues/45))

## Unit Tests
* Special characters have been added to unit test `data-transfer`
* `dbRemoveTable` new parameters are added in unit test
* Added row.names to unit test data transfer
* Updated dplyr `sql_translate_env` until test to cater bug fix

## New Feature
* Due to help from @OssiLehtinen, `dbRemoveTable` can now remove S3 files for AWS Athena table being removed.

## Minor Change
* Added AWS_ATHENA_WORK_GROUP environmental variable support
* Removed `tolower` conversion due to request ([RAthena: # 41](https://github.com/DyfanJones/RAthena/issues/41))

# noctua 1.3.0
## Major Change
* `dbWriteTable` now will split `gzip` compressed files to improve AWS Athena performance. By default `gzip` compressed files will be split into 20.

Performance results

```r
library(DBI)
X <- 1e8
df <- data.frame(w =runif(X),
                 x = 1:X,
                 y = sample(letters, X, replace = T), 
                 z = sample(c(TRUE, FALSE), X, replace = T))
con <- dbConnect(noctua::athena())
# upload dataframe with different splits
dbWriteTable(con, "test_split1", df, compress = T, max.batch = nrow(df), overwrite = T) # no splits
dbWriteTable(con, "test_split2", df, compress = T, max.batch = 0.05 * nrow(df), overwrite = T) # 20 splits
dbWriteTable(con, "test_split3", df, compress = T, max.batch = 0.1 * nrow(df), overwrite = T) # 10 splits
```
AWS Athena performance results from AWS console (query executed: `select count(*) from ....` ):

* test_split1: (Run time: 38.4 seconds, Data scanned: 1.16 GB)
* test_split2: (Run time: 3.73 seconds, Data scanned: 1.16 GB)
* test_split3: (Run time: 5.47 seconds, Data scanned: 1.16 GB)

```r
library(DBI)
X <- 1e8
df <- data.frame(w =runif(X),
                 x = 1:X,
                 y = sample(letters, X, replace = T), 
                 z = sample(c(TRUE, FALSE), X, replace = T))
con <- dbConnect(noctua::athena())
dbWriteTable(con, "test_split1", df, compress = T, overwrite = T) # default will now split compressed file into 20 equal size files.
```

Added information message to inform user about what files have been added to S3 location if user is overwriting an Athena table.

## Minor Change
* `copy_to` method now supports compress and max_batch, to align with `dbWriteTable`

## Bug Fix
* Fixed bug in regards to Athena DDL being created incorrectly when passed from `dbWriteTable`
* Thanks to @OssiLehtinen for identifying issue around uploading class `POSIXct` to Athena. This class was convert incorrectly and AWS Athena would return NA instead. `noctua` will now correctly convert `POSIXct` to timestamp but will also correct read in timestamp into `POSIXct`
* Thanks to @OssiLehtinen for discovering an issue with `NA` in string format. Before `noctua` would return `NA` in string class as `""` this has now been fixed.
* When returning a single column data.frame from Athena, `noctua` would translate output into a vector with current the method `dbFetch` n = 0.
* Thanks to @OssiLehtinen for identifying issue around `sql_translate_env`. Previously `noctua` would take the default `dplyr::sql_translate_env`, now `noctua` has a custom method that uses Data types from: https://docs.aws.amazon.com/athena/latest/ug/data-types.html and window functions from: https://docs.aws.amazon.com/athena/latest/ug/functions-operators-reference-section.html

### Unit tests
* `POSIXct` class has now been added to data transfer unit test
* `dplyr sql_translate_env` tests if R functions are correct translated in to Athena `sql` syntax.

# noctua 1.2.1
## New Features:
* Parquet file type can now be compress using snappy compression when writing data to S3.

## Bug fixed
* Older versions of R are returning errors when function `dbWriteTable` is called. The bug is due to function `sqlCreateTable` which `dbWriteTable` calls. Parameters `table` and `fields` were set to `NULL`. This has now been fixed.

# noctua 1.2.0
## Minor Change
* `s3.location` parameter is `dbWriteTable` can now be made nullable

## Backend Change
* helper function `upload_data` has been rebuilt and removed the old "horrible" if statement with `paste` now the function relies on `sprintf` to construct the s3 location path. This method now is a lot clearer in how the s3 location is created plus it enables a `dbWriteTable` to be simplified. `dbWriteTable` can now upload data to the default s3_staging directory created in `dbConnect` this simplifies `dbWriteTable` to :

```r
library(DBI)

con <- dbConnect(noctua::athena())

dbWriteTable(con, "iris", iris)
```

## Bug Fix
* Info message wasn't being return when colnames needed changing for Athena DDL

## Unit Tests
* `data transfer` test now tests compress, and default s3.location when transferring data

## New Feature
* GZIP compression is now supported for "csv" and "tsv" file format in `dbWriteTable`

## Minor Change
* `sqlCreateTable` info message will now only inform user if colnames have changed and display the column name that have changed

# noctua 1.1.0
## New Features
* credentials are now passed through the new `config = list()` parameter is `paws` objects
* `BigInt` are now passed correctly into `integer64`

### Bug fix
* `AthenaResult` returned: `Error in call[[2]] : object of type 'closure' is not subsettable`. The function `do.call` was causing the issue, to address this `do.call` has been removed and the helper function `request` has been broken down into `ResultConfiguration` to return a single component of `start_query_execution`
* All functions that utilise `do.call` have been broken down due to error: `Error in call[[2]] : object of type 'closure' is not subsettable`

## Unit Tests
* Added `bigint` to `integer64` in data.transfer unit test

## Minor Change
* dependency `paws` version has been set to a minimum of `0.1.5` due to latest change.

## Major Change
* `data.table` is now used as the default file parser `data.table::fread` / `data.table::fwrite`. This isn't a breaking change as `data.table` was used before however this change makes `data.table` to default file parser.


# noctua 1.0.0
## New Features
### DBI
* `dbConnect` method can use the following methods:
  * assume role
  * aws profile name
  * hard coded aws credentials
  * set credentials in system variables
* Enabled method to upload parquet file format into AWS S3 using `arrow` package
  
### Athena lower level api
* `assume_role` developed method for user to assume role when connecting to AWS Athena
* developed methods to create, list, delete and get AWS Athena work groups
