#' @include paws.athena.R
NULL

#' Athena Driver Methods
#'
#' Implementations of pure virtual functions defined in the `DBI` package
#' for AthenaDriver objects.
#' @keywords internal
#' @name AthenaDriver
NULL

#' Athena Driver
#'
#' Driver for an Athena Boto3 connection.
#'
#' @export
#' @import methods DBI
#' @return \code{athena()} returns a s4 class. This class is used active Athena method for \code{\link[DBI]{dbConnect}}
#' @seealso \code{\link{dbConnect}}

athena <- function() {
  new("AthenaDriver")
}

#' @rdname AthenaDriver
#' @export
setClass("AthenaDriver", contains = "DBIDriver")

#' @rdname AthenaDriver
#' @inheritParams methods::show
#' @export
setMethod(
  "show", "AthenaDriver",
  function(object) {
    cat("<AthenaDriver>\n")
  })

#' @rdname dbDataType
#' @export
setMethod("dbDataType", "AthenaDriver", function(dbObj, obj,...) {
  AthenaDataType(obj)
})

#' @rdname dbDataType
#' @export
setMethod(
  "dbDataType", c("AthenaDriver", "list"),
  function(dbObj, obj, ...) {
    AthenaDataType(obj)
  })

#' Connect to Athena using python's sdk boto3
#' 
#' @aliases dbConnect
#' @return \code{dbConnect()} returns a s4 class. This object is used to communicate with AWS Athena.
#' @seealso \code{\link[DBI]{dbConnect}}
#' @export
setMethod(
  "dbConnect", "AthenaDriver",
  function(drv,
           aws_access_key_id = NULL,
           aws_secret_access_key = NULL ,
           aws_session_token = NULL,
           schema_name = "default",
           work_group = NULL,
           poll_interval = NULL,
           encryption_option = c("NULL", "SSE_S3", "SSE_KMS", "CSE_KMS"),
           kms_key = NULL,
           profile_name = NULL,
           role_arn= NULL,
           role_session_name= sprintf("paws-athena-session-%s", as.integer(Sys.time())),
           duration_seconds = 3600L,
           s3_staging_dir = NULL,
           region_name = NULL,
           botocore_session = NULL, ...) {
    
    # assert checks on parameters
    stopifnot(is.null(aws_access_key_id) || is.character(aws_access_key_id),
              is.null(aws_secret_access_key) || is.character(aws_secret_access_key),
              is.null(aws_session_token) || is.character(aws_session_token),
              is.character(schema_name),
              is.null(work_group) || is.character(work_group),
              is.null(poll_interval) || is.numeric(poll_interval),
              is.null(kms_key) || is.character(kms_key),
              is.null(s3_staging_dir) || is.s3_uri(s3_staging_dir),
              is.null(region_name) || is.character(region_name),
              is.null(profile_name) || is.character(profile_name),
              is.null(role_arn) || is.character(role_arn),
              is.character(role_session_name),
              is.numeric(duration_seconds))
    
    encryption_option <- switch(encryption_option[1],
                                "NULL" = NULL,
                                match.arg(encryption_option))
    
    aws_access_key_id <- aws_access_key_id %||% get_aws_env("AWS_ACCESS_KEY_ID")
    aws_secret_access_key <- aws_secret_access_key %||% get_aws_env("AWS_SECRET_ACCESS_KEY")
    aws_session_token <- aws_session_token %||% get_aws_env("AWS_SESSION_TOKEN")
    role_arn <- role_arn %||% get_aws_env("AWS_ROLE_ARN")
    
    aws_expiration <- NULL
    
    con <- AthenaConnection(aws_access_key_id = aws_access_key_id,
                            aws_secret_access_key = aws_secret_access_key ,
                            aws_session_token = aws_session_token,
                            schema_name = schema_name,
                            work_group = work_group,
                            poll_interval =poll_interval,
                            encryption_option = encryption_option,
                            kms_key = kms_key,
                            s3_staging_dir = s3_staging_dir,
                            region_name = region_name,
                            botocore_session = botocore_session,
                            profile_name = profile_name, 
                            aws_expiration = aws_expiration,...)
    con
  })
