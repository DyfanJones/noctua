## Submission
This release contains a couple of new features

### New Features:
* `dbGetPartition` has an optional formatting option to return tidy output instead of default `AWS Athena` output
* `dbConnect` now supports different options for `bigint`, this is to align with other `DBI` backend packages, that already support this feature.

### Bug Fix:
* `dbRemoveTable` now checks the `AWS S3` path returned from `AWS Glue` before modifying to remove `AWS S3` objects.

## Examples Note:
* All R examples with `\dontrun` have been given a note warning users that `AWS credentials` are required to run

## Test environments
* local OS X install, R 4.0.2
* rhub: windows-x86_64-devel, ubuntu-gcc-release, fedora-clang-devel

## R CMD check results (local)
0 errors ✓ | 0 warnings ✓ | 0 notes ✓

## R devtools::check_rhub() results
0 errors ✓ | 0 warnings ✓ | 0 notes ✓

**Side note:** ran devtools::check_rhub with following environment variables:
`devtools::check_rhub(env_vars=c(R_COMPILE_AND_INSTALL_PACKAGES = "always", LIBARROW_BINARY="true"))`

## unit tests (using testthat) results
* OK:       127
* Failed:   0
* Warnings: 0
* Skipped:  0
