# noctua 1.5.0.9001
### New Feature
* `dbStatistics` is a wrapper around `paws` `get_query_execution` to return statistics for `noctua::dbSendQuery` results
* `dbGetQuery` has new parameter `statistics` to print out `dbStatistics` before returning Athena results.

# noctua 1.5.0.9000
### Bug Fix
* R functions `paste`/`paste0` would use default `dplyr:sql-translate-env` (`concat_ws`). `paste0` now uses Presto's `concat` function and `paste` now uses pipes to get extra flexible for custom separating values.

```
# R code:
paste("hi", "bye", sep = "-")
# SQL translation:
('hi'||'-'||'bye')
```

# noctua 1.5.0
Updated package version for cran release

# noctua 1.4.0.9001
### New Feature
* Added integration into Rstudio connections tab
* Added information message of amount of data scanned by aws athena
* Added method to change backend file parser so user can change file parser from `data.table` to `vroom`. From now on it is possible to change file parser using `noctua_options` for example:

```
library(noctua)

noctua_options("vroom")

```

* new function `dbGetTables` that returns Athena hierarchy as a data.frame

### Unit tests
* Added datatransfer unit test for backend file parser vroom

# noctua 1.4.0.9000
### Documentation
* Updated rdocumentation to roxygen2 7.0.2

# noctua 1.4.0
Updated package version for cran release

# noctua 1.3.0.9003
### Major Change
* Default delimited file uploaded to AWS Athena changed from "csv" to "tsv" this is due to separating value "," in character variables. By using "tsv" file type JSON/Array objects can be passed to Athena through character types. To prevent this becoming a breaking change `dbWriteTable` `append` parameter checks and uses existing AWS Athena DDL file type. If `file.type` doesn't match Athena DDL file type then user will receive a warning message:

```
warning('Appended `file.type` is not compatible with the existing Athena DDL file type and has been converted to "', File.Type,'".', call. = FALSE)
```

### Bug fix
* Due to issue highlighted by @OssiLehtinen in https://github.com/DyfanJones/RAthena/issues/50, special characters have issue being processed when using flat file in the backend.

### Unit Tests
* Special characters have been added to unit test `data-transfer`

# noctua 1.3.0.9002
### New Feature
* Due to help from @OssiLehtinen, `dbRemoveTable` can now remove S3 files for AWS Athena table being removed.

### Unit Tests
* `dbRemoveTable` new parameters are added in unit test

# noctua 1.3.0.9001
### Minor Change
* Added AWS_ATHENA_WORK_GROUP environmental variable support

# noctua 1.3.0.9000
### Minor Change
* Removed tolower conversion due to request https://github.com/DyfanJones/RAthena/issues/41

### Bug fixed
* Fixed issue where row.names not being correctly catered and returning NA in column names https://github.com/DyfanJones/RAthena/issues/41
* Fixed issue with `INTEGER` being incorrectly translated in `sql_translate_env.R`
* Fixed issue where `as.character` was getting wrongly translated https://github.com/DyfanJones/RAthena/issues/45

### Unit Tests
* Added row.names to unit test data transfer
* Updated dplyr sql_translate_env until test to cater bug fix


# noctua 1.3.0
Updated package version for cran release

# noctua 1.2.1.9004
### Major Change
* `dbWriteTable` now will split `gzip` compressed files to improve AWS Athena performance. By default `gzip` compressed files will be split into 20.

Performance results
```
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

```
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

### Minor Change
* `copy_to` method now supports compress and max_batch, to align with `dbWriteTable`

# noctua 1.2.1.9003
### Bug Fixed
* Fixed bug in regards to Athena DDL being created incorrectly when passed from `dbWriteTable`

# noctua 1.2.1.9002
### Bug Fixed
* Thanks to @OssiLehtinen for identifying issue around uploading class `POSIXct` to Athena. This class was convert incorrectly and AWS Athena would return NA instead. `noctua` will now correctly convert `POSIXct` to timestamp but will also correct read in timestamp into `POSIXct`

* Thanks to @OssiLehtinen for discovering an issue with `NA` in string format. Before `noctua` would return `NA` in string class as `""` this has now been fixed.

### Unit tests
* `POSIXct` class has now been added to data transfer unit test

# noctua 1.2.1.9001
### Bug Fixed
When returning a single column data.frame from Athena, `noctua` would translate output into a vector with current the method `dbFetch` n = 0.

# noctua 1.2.1.9000
### Bug Fixed
Thanks to @OssiLehtinen for identifying issue around `sql_translate_env`. Previously `noctua` would take the default `dplyr::sql_translate_env`, now `noctua` has a custom method that uses Data types from: https://docs.aws.amazon.com/athena/latest/ug/data-types.html and window functions from: https://docs.aws.amazon.com/athena/latest/ug/functions-operators-reference-section.html

### Unit tests
* `dplyr sql_translate_env` tests if R functions are correct translated in to Athena sql syntax.

# noctua 1.2.1
### New Features:
* Parquet file type can now be compress using snappy compression when writing data to S3.

### Bug fixed
* Older versions of R are returning errors when function `dbWriteTable` is called. The bug is due to function `sqlCreateTable` which `dbWriteTable` calls. Parameters `table` and `fields` were set to `NULL`. This has now been fixed.

# noctua 1.2.0
Updated package version for cran release

# noctua 1.1.0.9001
### Minor Change
* `s3.location` parameter is `dbWriteTable` can now be made nullable

### Backend Change
* helper function `upload_data` has been rebuilt and removed the old "horrible" if statement with `paste` now the function relies on `sprintf` to construct the s3 location path. This method now is a lot clearer in how the s3 location is created plus it enables a `dbWriteTable` to be simplified. `dbWriteTable` can now upload data to the default s3_staging directory created in `dbConnect` this simplifies `dbWriteTable` to :
```
library(DBI)

con <- dbConnect(noctua::athena())

dbWriteTable(con, "iris", iris)
```
### Bug Fix
* Info message wasn't being return when colnames needed changing for Athena DDL

### Unit Tests
* `data transfer` test now tests compress, and default s3.location when transferring data

# noctua 1.1.0.9000
### New Feature
* GZIP compression is now supported for "csv" and "tsv" file format in `dbWriteTable`

### Minor Change
* `sqlCreateTable` info message will now only inform user if colnames have changed and display the colname that have changed

# noctua 1.1.0
* Increment package version from dev version to cran

# noctua 1.0.9000

### New Features
* credentials are now passed through the new `config = list()` parameter is `paws` objects
* `BigInt` are now passed correctly into `integer64`

#### Bug
* `AthenaResult` returned: `Error in call[[2]] : object of type 'closure' is not subsettable`. The function `do.call` was causing the issue, to address this `do.call` has been removed and the helper function `request` has been broken down into `ResultConfiguration` to return a single component of `start_query_execution`
* All functions that utilise `do.call` have been broken down due to error: `Error in call[[2]] : object of type 'closure' is not subsettable`

### Unit Tests
* Added `bigint` to `integer64` in data.transfer unit test

### Minor Change
* dependency `paws` version has been set to a minimum of `0.1.5` due to latest change.

### Major Change
* `data.table` is now used as the default file parser `data.table::fread` / `data.table::fwrite`. This isn't a breaking change as `data.table` was used before however this change makes `data.table` to default file parser.


# noctua 1.0.0
* **Initial RAthena release**

### New Features
#### DBI
* `dbConnect` method can use the following methods:
  * assume role
  * aws profile name
  * hard coded aws credentials
  * set credentials in system variables
* Enabled method to upload parquet file format into AWS S3 using `arrow` package
  
#### Athena lower level api
* `assume_role` developed method for user to assume role when connecting to AWS Athena
* developed methods to create, list, delete and get AWS Athena work groups
