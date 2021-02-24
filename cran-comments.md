## Submission
This is hot fix to fix issue of keyboard interrupt failing to raise interrupt error

### Bug Fix:
* fix issue were keyboard interrupt didn't raise interrupt error due to 2.0.0 release

### Unit test:
* check if interrupt function successfully interrupts Athena when user manually triggers a keyboard interrupt

## Examples Note:
* All R examples with `\dontrun` have been given a note warning users that `AWS credentials` are required to run

## Test environments
* local OS X install, R 4.0.2
* rhub: windows-x86_64-devel, ubuntu-gcc-release, fedora-clang-devel

## R CMD check results (local)
0 errors ✓ | 0 warnings ✓ | 0 notes ✓

## R devtools::check_rhub() results
0 errors ✓ | 0 warnings ✓ | 1 note x

**Note:**
Maintainer: 'Dyfan Jones <dyfan.r.jones@gmail.com>'
Days since last update: 2

**Author notes:** 
Apologises for the quick re-release, this is a hot fix to fix "keyboard interrupt" failing to raise errors due to v-2.0.0 release

**Side note:** ran devtools::check_rhub with following environment variables:
`devtools::check_rhub(env_vars=c(R_COMPILE_AND_INSTALL_PACKAGES = "always", LIBARROW_BINARY="true"))`

## unit tests (using testthat) results
[ FAIL 0 | WARN 0 | SKIP 0 | PASS 227 ]
