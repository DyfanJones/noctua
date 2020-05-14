## Release Summary
This release brings increase reliability when working with `AWS Athena`, several new features and a few bug fixes.

**New Features**
* When working with `AWS Athena`, `AWS` APIs can become overwhelmed and return unnecassary exceptions. To over come this `noctua` has now been given a retry capability with exponential backoff.
* Previously `dbFetch` was restricted to only return the entire data.frame or a chunk limited to 1000 from `AWS Athena`. This was due to the restriction in the call to `AWS Athena`. Now `noctua` uses tokens from `AWS` to iterate over. This allows `dbFetch` to back larger chunks and work similar to other DBI backend packages:

```
library(DBI)
con <- dbConnect(noctua::athena())
res <- dbExecute(con, "select * from some_big_table limit 10000")
dbFetch(res, 5000)
```

* When appending to existing tables `dbWriteTable` now opts to use `ALTER TABLE` instead of `MSCK REPAIR TABLE` this gives an performance increase when appending onto highly partitioned tables.
* `dbWriteTable` is not compatible with `SerDes` and Data Formats
* New function `dbConvertTable` allows `noctua` to convert backend `AWS S3` files of existing `AWS Athena` tables

**Bug Fixes**
* `dbWriteTable` would throw `throttling error` every now and again, `retry_api_call` as been built to handle the parsing of data between R and `AWS S3`.
* `dbWriteTable` did not clear down all metadata when uploading to `AWS Athena`

## Examples Note:
* All R examples with `\dontrun` have been given a note warning users that `AWS credentials` are required to run

## Test environments
* local OS X install, R 4.0.0
* rhub: windows-x86_64-devel, ubuntu-gcc-release, fedora-clang-devel

## R CMD check results (local)
0 errors ✓ | 0 warnings ✓ | 0 notes ✓

## R devtools::check_rhub() results
0 errors ✓ | 0 warnings ✓ | 0 notes ✓

## unit tests (using testthat) results
* OK:       127
* Failed:   0
* Warnings: 0
* Skipped:  0
