#' @include noctua.R
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
#' Driver for an Athena paws connection.
#'
#' @import methods DBI
#' @return \code{athena()} returns a s4 class. This class is used active Athena method for \code{\link[DBI]{dbConnect}}
#' @seealso \code{\link{dbConnect}}
#' @export
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

#' Connect to Athena using R's sdk paws
#' 
#' @description 
#' It is never advised to hard-code credentials when making a connection to Athena (even though the option is there). Instead it is advised to use
#' \code{profile_name} (set up by \href{https://aws.amazon.com/cli/}{AWS Command Line Interface}), 
#' \href{https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html}{Amazon Resource Name roles} or environmental variables. Here is a list
#' of supported environment variables:
#' \itemize{
#' \item{\strong{AWS_ACCESS_KEY_ID:} is equivalent to the \code{dbConnect} parameter - \code{aws_access_key_id}}
#' \item{\strong{AWS_SECRET_ACCESS_KEY:} is equivalent to the \code{dbConnect} parameter - \code{aws_secret_access_key}}
#' \item{\strong{AWS_SESSION_TOKEN:} is equivalent to the \code{dbConnect} parameter - \code{aws_session_token}}
#' \item{\strong{AWS_ROLE_ARN:} is equivalent to the \code{dbConnect} parameter - \code{role_arn}}
#' \item{\strong{AWS_EXPIRATION:} is equivalent to the \code{dbConnect} parameter - \code{duration_seconds}}
#' \item{\strong{AWS_ATHENA_S3_STAGING_DIR:} is equivalent to the \code{dbConnect} parameter - \code{s3_staging_dir}}
#' \item{\strong{AWS_ATHENA_WORK_GROUP:} is equivalent to \code{dbConnect} parameter - \code{work_group}}
#' \item{\strong{AWS_REGION:} is equivalent to \code{dbConnect} parameter - \code{region_name}}
#' }
#' 
#' \strong{NOTE:} If you have set any environmental variables in \code{.Renviron} please restart your R in order for the changes to take affect.
#'
#' @inheritParams DBI::dbConnect
#' @param aws_access_key_id AWS access key ID
#' @param aws_secret_access_key AWS secret access key
#' @param aws_session_token AWS temporary session token
#' @param schema_name The schema_name to which the connection belongs
#' @param work_group The name of the \href{https://aws.amazon.com/about-aws/whats-new/2019/02/athena_workgroups/}{work group} to run Athena queries , Currently defaulted to \code{NULL}.
#' @param poll_interval Amount of time took when checking query execution status. Default set to a random interval between 0.5 - 1 seconds.
#' @param encryption_option Athena encryption at rest \href{https://docs.aws.amazon.com/athena/latest/ug/encryption.html}{link}. 
#'                          Supported Amazon S3 Encryption Options ["NULL", "SSE_S3", "SSE_KMS", "CSE_KMS"]. Connection will default to NULL,
#'                          usually changing this option is not required.
#' @param kms_key \href{https://docs.aws.amazon.com/kms/latest/developerguide/overview.html}{AWS Key Management Service}, 
#'                please refer to \href{https://docs.aws.amazon.com/kms/latest/developerguide/concepts.html}{link} for more information around the concept.
#' @param profile_name The name of a profile to use. If not given, then the default profile is used.
#'                     To set profile name, the \href{https://aws.amazon.com/cli/}{AWS Command Line Interface} (AWS CLI) will need to be configured.
#'                     To configure AWS CLI please refer to: \href{https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html}{Configuring the AWS CLI}.
#' @param role_arn The Amazon Resource Name (ARN) of the role to assume (such as \code{arn:aws:sts::123456789012:assumed-role/role_name/role_session_name})
#' @param role_session_name An identifier for the assumed role session. By default `RAthena` creates a session name \code{sprintf("RAthena-session-\%s", as.integer(Sys.time()))}
#' @param duration_seconds The duration, in seconds, of the role session. The value can range from 900 seconds (15 minutes) up to the maximum session duration setting for the role. 
#'                         This setting can have a value from 1 hour to 12 hours. By default duration is set to 3600 seconds (1 hour). 
#' @param s3_staging_dir The location in Amazon S3 where your query results are stored, such as \code{s3://path/to/query/bucket/}
#' @param region_name Default region when creating new connections. Please refer to \href{https://docs.aws.amazon.com/general/latest/gr/rande.html}{link} for 
#'                    AWS region codes (region code example: Region = EU (Ireland) 	\code{ region_name = "eu-west-1"})
#' @param bigint The R type that 64-bit integer types should be mapped to,
#'   default is [bit64::integer64], which allows the full range of 64 bit
#'   integers.
#' @param binary The R type that [binary/varbinary] types should be mapped to,
#'   default is [raw]. If the mapping fails R will resort to [character] type.
#'   To ignore data type conversion set to ["character"].
#' @param json Attempt to converts AWS Athena data types [arrays, json] using \code{jsonlite:parse_json}. If the mapping fails R will resort to [character] type.
#'   Custom Json parsers can be provide by using a function with data frame parameter.
#'   To ignore data type conversion set to ["character"].
#' @param keyboard_interrupt Stops AWS Athena process when R gets a keyboard interrupt, currently defaults to \code{TRUE}
#' @param rstudio_conn_tab Optional to get AWS Athena Schema and display it in RStudio's Connections Tab.
#'   Default set to \code{TRUE}.
#' @param ... other parameters for \code{paws} session
#' @aliases dbConnect
#' @return \code{dbConnect()} returns a s4 class. This object is used to communicate with AWS Athena.
#' @examples
#' \dontrun{
#' # Connect to Athena using your aws access keys
#'  library(DBI)
#'  con <- dbConnect(noctua::athena(),
#'                   aws_access_key_id='YOUR_ACCESS_KEY_ID', # 
#'                   aws_secret_access_key='YOUR_SECRET_ACCESS_KEY',
#'                   s3_staging_dir='s3://path/to/query/bucket/',
#'                   region_name='us-west-2')
#'  dbDisconnect(con)
#'  
#' # Connect to Athena using your profile name
#' # Profile name can be created by using AWS CLI
#'  con <- dbConnect(noctua::athena(),
#'                   profile_name = "YOUR_PROFILE_NAME",
#'                   s3_staging_dir = 's3://path/to/query/bucket/')
#'  dbDisconnect(con)
#'  
#' # Connect to Athena using ARN role
#'  con <- dbConnect(noctua::athena(),
#'                   profile_name = "YOUR_PROFILE_NAME",
#'                   role_arn = "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name",
#'                   s3_staging_dir = 's3://path/to/query/bucket/')
#'                  
#'  dbDisconnect(con)
#' }
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
           role_session_name= sprintf("noctua-session-%s", as.integer(Sys.time())),
           duration_seconds = 3600L,
           s3_staging_dir = NULL,
           region_name = NULL,
           bigint = c("integer64", "integer", "numeric", "character"),
           binary = c("raw", "character"),
           json = c("auto", "character"),
           keyboard_interrupt = TRUE,
           rstudio_conn_tab = TRUE,
           ...) {
    
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
              is.numeric(duration_seconds),
              is.logical(keyboard_interrupt),
              is.character(json) || is.function(json),
              is.logical(rstudio_conn_tab))
    
    athena_option_env$bigint <- big_int(match.arg(bigint))
    athena_option_env$binary <- match.arg(binary)
    athena_option_env$json <- if(is.character(json)) jsonlite_check(json[[1]]) else json
    athena_option_env$rstudio_conn_tab <- rstudio_conn_tab
    
    encryption_option <- switch(encryption_option[1],
                                "NULL" = NULL,
                                match.arg(encryption_option))
    
    # if aws session token then return duration
    aws_session_token <- aws_session_token %||% get_aws_env("AWS_SESSION_TOKEN")
    aws_expiration <- NULL
    if(!is.null(aws_session_token)) aws_expiration <- get_aws_env("AWS_EXPIRATION")
    if(!is.null(aws_expiration)) aws_expiration <- as.POSIXct(as.numeric(aws_expiration), origin='1970-01-01')
    
    if(!is.null(role_arn)) {
      creds <- assume_role(profile_name = profile_name,
                           region_name = region_name,
                           role_arn = role_arn,
                           role_session_name = role_session_name,
                           duration_seconds = duration_seconds)
      profile_name <- NULL
      aws_access_key_id <- creds$AccessKeyId
      aws_secret_access_key <- creds$SecretAccessKey
      aws_session_token <- creds$SessionToken
      aws_expiration <- creds$Expiration
    }
    
    aws_access_key_id <- aws_access_key_id %||% get_aws_env("AWS_ACCESS_KEY_ID")
    aws_secret_access_key <- aws_secret_access_key %||% get_aws_env("AWS_SECRET_ACCESS_KEY")
    role_arn <- role_arn %||% get_aws_env("AWS_ROLE_ARN")
    work_group <- work_group %||% get_aws_env("AWS_ATHENA_WORK_GROUP")
    
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
                            profile_name = profile_name, 
                            aws_expiration = aws_expiration,
                            keyboard_interrupt = keyboard_interrupt,
                            ...)
    # integrate with RStudio
    on_connection_opened(con)
    con
  })
