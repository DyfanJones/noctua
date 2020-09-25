## Release Summary
This release brings in bug fixes.

**Bug Fix**
* Provided uuid string to `ClientRequestToken`. This fixes the following errors:
  * `Error: InvalidRequestException (HTTP 400). Idempotent parameters do not match`
  * `Error: InvalidRequestException (HTTP 400). Could not find results`
* `noctua_options` parameter `cache_size` now correctly uses the range [0,100]
*  Do not abort if a `AWS Glue` `get_tables` api call fails

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
