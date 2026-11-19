## Submission
This is a resubmission of noctua 2.7.0. The package was previously archived on CRAN because installation timed out. Installing the `paws` 0.10.0 umbrella package pulled in the entire AWS SDK and took too long. This is fixed in this release: noctua now depends on the modular `paws` category packages instead of the umbrella package: `paws.analytics`, `paws.security.identity` and `paws.storage` (each `>= 0.11.0`, where the installation time was fixed), plus the low-level `paws.common` (`>= 0.9.0`).

This release:

* Raises the minimum supported versions to `dbplyr (>= 2.5.0)` and `dplyr (>= 1.1.0)`, and drops the dbplyr 1st-edition backend methods (`db_desc`, `db_explain`, `db_query_fields`), which were never part of noctua's public API.
* Moves from the monolithic `paws` dependency to the modular `paws.common`, `paws.analytics`, `paws.security.identity` and `paws.storage` packages.
* Fixes `create_work_group()` / `update_work_group()` with latest `paws.common`, `compute()` with latest `dbplyr`, and adds a `sql_escape_string` method so date/timestamp literals are detected again.

## Test environments
* local Ubuntu 26.04, R 4.6.0 (`R CMD check --as-cran`)
* R-hub (R-devel): linux, windows, macos (x86_64), m1-san (arm64) and macos-arm64 all report `Status: OK` with 0 errors, 0 warnings and 0 notes.

## R CMD check results
0 errors | 0 warnings | 0 notes (`devtools::check(cran = TRUE)`, with CRAN incoming checks disabled)

* CRAN's incoming checks will add a "New submission" note and may add a "Package was archived on CRAN" note. Both are expected; the archive reason is in the submission section above.

## Notes on examples and tests
* Most of the package needs an AWS account, so examples that connect to AWS are wrapped in `\dontrun{}`. The unit tests that need AWS credentials are skipped unless the `noctua_arn`, `noctua_s3_query` and `noctua_s3_tbl` environment variables are set, so they are skipped on CRAN.
* Tests that use the suggested packages `vroom`, `arrow` and `jsonify` are skipped when those packages are not installed.
