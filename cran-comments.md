## Release Summary
This release is a feature release, focusing on:

**Major Change**
* Convert default delimited file from `csv` to `tsv`. This is to enable array and json to be pushed to `AWS Athena` from `R`. To prevent this from becoming a breaking change, `dbWriteTable` now checks existing file type in `Athena` Data Definition Language (`DDL`) and utilises that file type when appending to existing tables. User will be returned with a warning message:

```
warning('Appended `file.type` is not compatible with the existing Athena DDL file type and has been converted to "', File.Type,'".', call. = FALSE)
```

**New Features**
*  Support environment variable `AWS_ATHENA_WORK_GROUP`
* Added append checker to `dbWriteTable`. This checks what file type is currently being used, utilities file type when pushing new data to existing `AWS Athena Table`.
* `dbRemoveTable` to be able to delete Athena table s3 files

**Bug Fix**
* Special character incorrectly passed to `AWS Athena`
* `translate_sql_env` wrongly translated `integer`

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
* OK:       56
* Failed:   0
* Warnings: 0
* Skipped:  0