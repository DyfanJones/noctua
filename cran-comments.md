## Release Summary
This release addresses several bugs identified by the community:

** Bug Fix**
* `sql_translate_env` now correctly translates `paste`/`paste0` into `AWS Athena` sql
* `dplyr::sql_escape_string` now has a custom s3 method to take into account `date` classes when translating R code to sql
* `dbFetch` would fail due to large raw vectors exceeding `2^31 bytes`
* `noctua_options` will now check if file parser is installed, and if it is a compatible version
* Dependency `data.table` has been restricted to version (>=1.12.4), due `data.table::fwrite` adding file compression in version (>=1.12.4)

**New Features**
* New function `dbStatistics` returns `AWS Athena` query information 

## Examples Note:
* All R examples with `\dontrun` & `\donttest` have been given a note warning users that `AWS credentials` are required to run
* All R examples with `\dontrun` have a dummy `AWS S3 Bucket uri` example and won't run until user replace the `AWS S3 bucket uri`.

## Test environments
* local OS X install, R 3.6.1
* rhub: windows-x86_64-devel, ubuntu-gcc-release, fedora-clang-devel

## R CMD check results (local)
0 errors ✓ | 0 warnings ✓ | 0 notes ✓

## R devtools::check_rhub() results
0 errors ✔ | 0 warnings ✔ | 0 notes ✔

## unit tests (using testthat) results
* OK:       65
* Failed:   0
* Warnings: 0
* Skipped:  0