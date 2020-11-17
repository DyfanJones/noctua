## Resubmission
This is a resubmission. In this version I have:
* Ensure all Packages in Suggests should be used conditionally within unit tests, this is to fix: https://cran.r-project.org/web/checks/check_results_noctua.html

## Examples Note:
* All R examples with `\dontrun` have been given a note warning users that `AWS credentials` are required to run

## Test environments
* local OS X install, R 4.0.2
* rhub: windows-x86_64-devel, ubuntu-gcc-release, fedora-clang-devel

## R CMD check results (local)
0 errors ✓ | 0 warnings ✓ | 0 notes ✓

## R devtools::check_rhub() results
*  Days since last update: 6
0 errors ✓ | 0 warnings ✓ | 1 note x

**Notes**
Apologises for the fast resubmission. This is to fix "CRAN Package Check Results for Package noctua"

**Side note:** ran devtools::check_rhub with following environment variables:
`devtools::check_rhub(env_vars=c(R_COMPILE_AND_INSTALL_PACKAGES = "always", LIBARROW_BINARY="true"))`

## unit tests (using testthat) results
* OK:       127
* Failed:   0
* Warnings: 0
* Skipped:  0
