## Release Summary
This is a release being several new features and bug fixes highlighted by the community

**New Features**
* Query caching: `RAthena_options` now has a new parameter `cache_size` which allows to cache query meta data for improved performance over repeat queries
* `dbRemoveTable` now doesn't query `AWS Athena` to remove tables, instead it goes directly to `AWS Glue` and removes the table from `AWS Glue`'s catalogue. This improves performance when deleting tables.
* `dbWriteTable` now supports uploading `data.frames` in json lines format

**Bug Fixes**
* `dbWriteTable` appending to existing table compress file type was incorrectly return.
* `dbConnect` didn't correctly pass AWS region. This caused issue when running package on systems like travis.ci
* Rstudio Connection tab would fail if `AWS Glue` table information was uploaded incorrectly.

## Examples Note:
* All R examples with `\dontrun` & `\donttest` have been given a note warning users that `AWS credentials` are required to run
* All R examples with `\dontrun` have a dummy `AWS S3 Bucket uri` example and won't run until user replace the `AWS S3 bucket uri`.

## Test environments
* local OS X install, R 3.6.1
* rhub: windows-x86_64-devel, ubuntu-gcc-release, fedora-clang-devel

## R CMD check results (local)
0 errors ✓ | 0 warnings ✓ | 0 notes ✓

## R devtools::check_rhub() results
  Maintainer: 'Dyfan Jones <dyfan.r.jones@gmail.com>'
  
  Number of updates in past 6 months: 8

0 errors ✓ | 0 warnings ✓ | 1 note x

**Author's Notes**
* Apologies for the fast re-submission of this package. This release contains several cost benefits for using AWS Athena. Plus a couple of bug fixes. Unit tests now have increase coverage +80%.

## unit tests (using testthat) results
* OK:       115
* Failed:   0
* Warnings: 0
* Skipped:  0
