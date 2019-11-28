## Release Summary
This is update focuses on fixing a bug with `dbWriteTable`.

**Bug Fix**
The helper function `sqlCreateTable` had it's generic parameters `table` and `fields` set to NULL. For more recent versions of R this is not an issue however for older versions of R this raises an error.

**New Feature**
parquet file format now can be compressed using snappy compression when uploading to amazon s3

## Examples Note:
* All R examples with `\dontrun` & `\donttest` have been given a note warning users that `AWS credentials` are required to run
* All R examples with `\dontrun` have a dummy `AWS S3 Bucket uri` example and won't run until user replace the `AWS S3 bucket uri`.

## Test environments
* local OS X install, R 3.6.1
* rhub: windows-x86_64-devel, ubuntu-gcc-release, fedora-clang-devel

## R CMD check results (local)
0 errors ✔ | 0 warnings ✔ | 0 notes ✔

## R devtools::check_rhub() results
0 errors ✔ | 0 warnings ✔ | 0 notes ✔

## unit tests (using testthat) results
* OK:       37
* Failed:   0
* Warnings: 0
* Skipped:  0