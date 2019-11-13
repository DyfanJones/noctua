## Release Summary
This is a feature updated, focusing on methods to compress flat files before submitting them to `Amazon Web Service S3`. 

In this version I have:
* created a new parameter in `dbWriteTable` called `compress` that enables compression method `gzip` to be utilised when sending data to AWS S3.

**Bug Fix**
* Fixed minor bug of s3_uri being incorrectly built in helper function `upload_data`

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