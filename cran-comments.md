## Release Summary
This the initial submission of the noctua package

In this version I have:

* Added Explanation to all aliases in package description
* dplyr integration methods
* Unit test:
  * **NOTE:** *System variable format returned for Unit tests:*
  * Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
  * Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
  * Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"
* All R examples with `\dontrun` & `\donttest` have been given a note warning users that `AWS credentials` are required to run
* All R examples with `\dontrun` have a dummy `AWS S3 Bucket uri` example and won't run until user replace the `AWS S3 bucket uri`.

## Test environments
* local OS X install, R 3.6.1
* rhub: windows-x86_64-devel, ubuntu-gcc-release, fedora-clang-devel

## R CMD check results (local)
0 errors ✔ | 0 warnings ✔ | 0 notes ✔

## R devtools::check_rhub() results
  Maintainer: 'Dyfan Jones <dyfan.r.jones@gmail.com>'
  New submission
0 errors ✔ | 0 warnings ✔ | 1 note ✖

## unit tests (using testthat) results
* OK:       32
* Failed:   0
* Warnings: 0
* Skipped:  0