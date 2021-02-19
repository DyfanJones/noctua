## Submission
This release brings in new features, update api change to connection classes and some bug fixes.

### API Change:
* Classes switched from utilising list to environmnents, this allows to update classes by reference. This allows the connection class to update result class by reference.

### New Features:
* Added support to `AWS Athena` data types `[array, row, map, json, binary, ipaddress]`
* Allow users to turn off RStudio Connection Tab when working in RStudio

### Bug Fix:
* Iterate through each`AWS` token to get all results from `AWS Glue` catalogue.

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
* OK:       221
* Failed:   0
* Warnings: 0
* Skipped:  0
