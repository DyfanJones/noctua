## Release Summary
This release brings in new features and some bug fixes.

**New Features**
* Improve `dbRemoveTable` performance by utilising `delete_objects` instead of `delete_object` from the `paws` software development kit package.
* Incoraprate `dbplyr` helper function `sql_escape_date` into `RAthena`.
* Allow `noctua` append to a static `AWS S3` location.

**Bug Fix**
* Parquet file types now use parameter `use_deprecated_int96_timestamps` to align with AWS Athena `timestamp`.
* `dbplyr v-2.0.0` function `in_schema` broken `RAthena` function `db_query_fields.AthenaConnection`. This fix removes any quotations added but `in_schema`.

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
