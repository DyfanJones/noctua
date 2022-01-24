## Submission
This release is a patch release addressing cran build.

### Bug Fix:
  *  Unit test helper function `test_data` to use explicitly calls `size` parameter.

## Examples Note:
* All R examples with `\dontrun` have been given a note warning users that `AWS credentials` are required to run

## Test environments
* local OS X install, 4.1.1
* rhub: windows-x86_64-devel, ubuntu-gcc-release, fedora-clang-devel

## R CMD check results (local)
0 errors ✓ | 0 warnings ✓ | 0 notes ✓

## R devtools::check_rhub() results
0 errors ✓ | 0 warnings ✓ | 1 notes ✓

> checking CRAN incoming feasibility ... NOTE
  Maintainer: 'Dyfan Jones <dyfan.r.jones@gmail.com>'
  
  Days since last update: 4
  
## Maintainers Comments:
Apologises for this fast turn around. This release is to address issues regarding cran build. To prevent this from happening in the future,

> setenv _R_CHECK_LENGTH_1_CONDITION_ abort,verbose
> setenv _R_CHECK_LENGTH_1_LOGIC2_ abort,verbose
 
Have been set for all unit test runs.

**Side note:** ran devtools::check_rhub with following environment variables:
```
devtools::check_rhub(
  env_vars=c(
    "R_COMPILE_AND_INSTALL_PACKAGES" = "always",
    "LIBARROW_BINARY"="true",
    "_R_CHECK_LENGTH_1_CONDITION_"="abort,verbose",
    "_R_CHECK_LENGTH_1_LOGIC2_"="abort,verbose"
  )
)
```

## unit tests (using testthat) results
[ FAIL 0 | WARN 0 | SKIP 0 | PASS 331 ]
