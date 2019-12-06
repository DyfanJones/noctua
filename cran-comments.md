## Release Summary
This release is a feature relase, focusing on:

**New Features**
* `dplyr::sql_translate_env` method for noctua
* Rebuilt backend of `dbWriteTable` when uploading compressed `gzip` files to Amazon Web Service (AWS) S3. Now `gzip` files are split, to increase AWS Athena performance (initial tests show an improvement of x10). This may cause issues when overwriting existing tables in AWS Athena, an information message has been created to inform users to check s3 the files have be replaced correctly.

**Bug Fix**
* Issue with creating DDL, tables created with special characters would fail due to different quotation syntax needed.

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
* OK:       52
* Failed:   0
* Warnings: 0
* Skipped:  0