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

dbWrite(con, "iris", iris)
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

## New Features
* credentials are now passed through the new `config = list()` parameter is `paws` objects
* `BigInt` are now passed correctly into `integer64`

## Bug
* `AthenaResult` returned: `Error in call[[2]] : object of type 'closure' is not subsettable`. The function `do.call` was causing the issue, to address this `do.call` has been removed and the helper function `request` has been broken down into `ResultConfiguration` to return a single component of `start_query_execution`
* All functions that utilise `do.call` have been broken down due to error: `Error in call[[2]] : object of type 'closure' is not subsettable`

## Unit Tests
* Added `bigint` to `integer64` in data.transfer unit test

## Minor Change
* dependency `paws` version has been set to a minimum of `0.1.5` due to latest change.

## Major Change
* `data.table` is now used as the default file parser `data.table::fread` / `data.table::fwrite`. This isn't a breaking change as `data.table` was used before however this change makes `data.table` to default file parser.


# noctua 1.0.0
* **Initial RAthena release**

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
