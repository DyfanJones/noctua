## Release Summary
This is a quick release due to the changes in the dependent package `paws`. `paws` has enabled connection credentials to be passed through to `paws` objects, causing an error in several wrapping functions for example: `dbSendQuery`. In this release the bugs are addressed plus it contains new feature updates: 

* Setting `data.table` as the default file parser
* Handling of 'AWS Athena' `bigint` classes

In this version I have:

* Enabled and fixed connection parameters to pass through the new `config = list()` in `paws` objects
* Correctly pass Amazon Web Service ('AWS') Athena `bigint` to R `integer64` class.
* data.table has been made a dependency as `fread` and `fwrite` have been made the default file parsers to transfer data to and from 'AWS Athena'

## Examples Note:
* All R examples with `\dontrun` & `\donttest` have been given a note warning users that `AWS credentials` are required to run
* All R examples with `\dontrun` have a dummy `AWS S3 Bucket uri` example and won't run until user replace the `AWS S3 bucket uri`.

## Test environments
* local OS X install, R 3.6.1
* rhub: windows-x86_64-devel, ubuntu-gcc-release, fedora-clang-devel

## R CMD check results (local)
0 errors ✔ | 0 warnings ✔ | 0 notes ✔

## R devtools::check_rhub() results
checking CRAN incoming feasibility ... NOTE
  Maintainer: 'Dyfan Jones <dyfan.r.jones@gmail.com>'
  
  Days since last update: 3
  
### Author notes:
Due to the update in the dependency `paws` a bug has developed in several wrapper functions. This release addresses these bugs.

## unit tests (using testthat) results
* OK:       36
* Failed:   0
* Warnings: 0
* Skipped:  0