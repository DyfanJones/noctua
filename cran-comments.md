## Submission
This release brings in new features, and some bug fixes.

### Bug Fix:
* `sql_translate_env` `paste` method broke due to latest `dbplyr` release. `sql_translate_env` `paste` method now works intended from version `1.4.3` + 

### Feature:
  * `sql_translate_env` add support for `lubridate` / `stringr` functions
  * `write_bin` now doesn't chunk `writeBin` when `R` is greater than version `4.0.0`
  * `sql_translate_env` add support to base `R` `grepl`.
  * `dbConnect` add `timezone` parameter so that time zone between `R` and `AWS Athena` is consistent.

## Examples Note:
* All R examples with `\dontrun` have been given a note warning users that `AWS credentials` are required to run

## Test environments
* local OS X install, 4.1.0
* rhub: windows-x86_64-devel, ubuntu-gcc-release, fedora-clang-devel

## R CMD check results (local)
0 errors ✓ | 0 warnings ✓ | 0 notes ✓

## R devtools::check_rhub() results
0 errors ✓ | 0 warnings ✓ | 0 notes ✓

**Side note:** ran devtools::check_rhub with following environment variables:
`devtools::check_rhub(env_vars=c(R_COMPILE_AND_INSTALL_PACKAGES = "always", LIBARROW_BINARY="true"))`

## unit tests (using testthat) results
[ FAIL 0 | WARN 0 | SKIP 0 | PASS 276 ]
