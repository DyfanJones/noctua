## Submission
This release contains a few new features.

### Feature:
  *  Support `AWS Athena UNLOAD` within `DBI` methods.

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
[ FAIL 0 | WARN 0 | SKIP 0 | PASS 295 ]
