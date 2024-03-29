#' Athena Work Groups
#'
#' @description
#' Lower level API access, allows user to create and delete Athena Work Groups.
#' \describe{
#' \item{create_work_group}{Creates a workgroup with the specified name (\href{https://www.paws-r-sdk.com/docs/athena_create_work_group/}{link}).
#'                          The work group utilises parameters from the \code{dbConnect} object, to determine the encryption and output location of the work group.
#'                          The s3_staging_dir, encryption_option and kms_key parameters are gotten from \code{\link{dbConnect}}}
#' \item{tag_options}{Helper function to create tag options for function \code{create_work_group()}}
#' \item{delete_work_group}{Deletes the workgroup with the specified name (\href{https://www.paws-r-sdk.com/docs/athena_delete_work_group/}{link}).
#'                          The primary workgroup cannot be deleted.}
#' \item{list_work_groups}{Lists available workgroups for the account (\href{https://www.paws-r-sdk.com/docs/athena_list_work_groups/}{link}).}
#' \item{get_work_group}{Returns information about the workgroup with the specified name (\href{https://www.paws-r-sdk.com/docs/athena_get_work_group/}{link}).}
#' \item{update_work_group}{Updates the workgroup with the specified name (\href{https://www.paws-r-sdk.com/docs/athena_update_work_group/}{link}).
#'                          The workgroup's name cannot be changed. The work group utilises parameters from the \code{dbConnect} object, to determine the encryption and output location of the work group.
#'                          The s3_staging_dir, encryption_option and kms_key parameters are gotten from \code{\link{dbConnect}}}
#' }
#'
#' @param conn A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
#' @param work_group The Athena workgroup name.
#' @param enforce_work_group_config If set to \code{TRUE}, the settings for the workgroup override client-side settings.
#'           If set to \code{FALSE}, client-side settings are used. For more information, see
#'           \href{https://docs.aws.amazon.com/athena/latest/ug/workgroups-settings-override.html}{Workgroup Settings Override Client-Side Settings}.
#' @param publish_cloud_watch_metrics Indicates that the Amazon CloudWatch metrics are enabled for the workgroup.
#' @param bytes_scanned_cut_off The upper data usage limit (cutoff) for the amount of bytes a single query in a workgroup is allowed to scan.
#' @param description The workgroup description.
#' @param tags A tag that you can add to a resource. A tag is a label that you assign to an AWS Athena resource (a workgroup).
#'           Each tag consists of a key and an optional value, both of which you define. Tags enable you to categorize workgroups in Athena, for example,
#'           by purpose, owner, or environment. Use a consistent set of tag keys to make it easier to search and filter workgroups in your account.
#'           The maximum tag key length is 128 Unicode characters in UTF-8. The maximum tag value length is 256 Unicode characters in UTF-8.
#'           You can use letters and numbers representable in UTF-8, and the following characters: \code{"+ - = . _ : / @"}. Tag keys and values are case-sensitive.
#'           Tag keys must be unique per resource. Please use the helper function \code{tag_options()} to create tags for work group, if no tags are required please put \code{NULL} for this parameter.
#' @param key A tag key. The tag key length is from 1 to 128 Unicode characters in UTF-8. You can use letters and numbers representable in UTF-8, and the following characters: \code{"+ - = . _ : / @"}.
#'           Tag keys are case-sensitive and must be unique per resource.
#' @param value A tag value. The tag value length is from 0 to 256 Unicode characters in UTF-8. You can use letters and numbers representable in UTF-8, and the following characters: \code{"+ - = . _ : / @"}.
#'           Tag values are case-sensitive.
#' @param recursive_delete_option The option to delete the workgroup and its contents even if the workgroup contains any named queries
#' @param remove_output_location If set to \code{TRUE}, indicates that the previously-specified query results location (also known as a client-side setting) for queries in this workgroup should be ignored and set to null.
#'           If set to \code{FALSE} the out put location in the workgroup's result configuration will be updated with the new value.
#'           For more information, see \href{https://docs.aws.amazon.com/athena/latest/ug/workgroups-settings-override.html}{Workgroup Settings Override Client-Side Settings}.
#' @param state The workgroup state that will be updated for the given workgroup.
#'
#' @return
#' \describe{
#' \item{create_work_group}{Returns \code{NULL} but invisible}
#' \item{tag_options}{Returns \code{list} but invisible}
#' \item{delete_work_group}{Returns \code{NULL} but invisible}
#' \item{list_work_groups}{Returns list of available work groups}
#' \item{get_work_group}{Returns list of work group meta data}
#' \item{update_work_group}{Returns \code{NULL} but invisible}
#' }
#'
#' @examples
#' \dontrun{
#' # Note:
#' # - Require AWS Account to run below example.
#' # - Different connection methods can be used please see `noctua::dbConnect` documnentation
#'
#' library(noctua)
#'
#' # Demo connection to Athena using profile name
#' con <- dbConnect(noctua::athena())
#'
#' # List current work group available
#' list_work_groups(con)
#'
#' # Create a new work group
#' wg <- create_work_group(con,
#'   "demo_work_group",
#'   description = "This is a demo work group",
#'   tags = tag_options(key = "demo_work_group", value = "demo_01")
#' )
#'
#' # List work groups to see new work group
#' list_work_groups(con)
#'
#' # get meta data from work group
#' wg <- get_work_group(con, "demo_work_group")
#'
#' # Update work group
#' wg <- update_work_group(con, "demo_work_group",
#'   description = "This is a demo work group update"
#' )
#'
#'
#' # get updated meta data from work group
#' wg <- get_work_group(con, "demo_work_group")
#'
#' # Delete work group
#' delete_work_group(con, "demo_work_group")
#'
#' # Disconect from Athena
#' dbDisconnect(con)
#' }
#'
#' @name work_group
NULL

#' @rdname work_group
#' @export
create_work_group <- function(conn,
                              work_group = NULL,
                              enforce_work_group_config = FALSE,
                              publish_cloud_watch_metrics = FALSE,
                              bytes_scanned_cut_off = 10000000L,
                              description = NULL,
                              tags = tag_options(key = NULL, value = NULL)) {
  con_error_msg(conn, "Connection already closed.")
  stopifnot(
    is.character(work_group),
    is.logical(enforce_work_group_config),
    is.logical(publish_cloud_watch_metrics),
    is.integer(bytes_scanned_cut_off) || is.infinite(bytes_scanned_cut_off),
    is.character(description)
  )

  Configuration <- list(work_group_config(conn,
    EnforceWorkGroupConfiguration = enforce_work_group_config,
    PublishCloudWatchMetricsEnabled = publish_cloud_watch_metrics,
    BytesScannedCutoffPerQuery = bytes_scanned_cut_off
  ))

  tryCatch(conn@ptr$Athena$create_work_group(
    Name = work_group,
    Configuration = Configuration,
    Description = description,
    Tags = tags
  ))
  invisible(NULL)
}

#' @rdname work_group
#' @export
tag_options <- function(key = NULL,
                        value = NULL) {
  stopifnot(
    is.character(key),
    is.character(value)
  )
  invisible(list(list(list(Key = key, Value = value))))
}

#' @rdname work_group
#' @export
delete_work_group <- function(conn, work_group = NULL, recursive_delete_option = FALSE) {
  con_error_msg(conn, "Connection already closed.")
  stopifnot(
    is.character(work_group),
    is.logical(recursive_delete_option)
  )
  tryCatch(conn@ptr$Athena$delete_work_group(WorkGroup = work_group, RecursiveDeleteOption = recursive_delete_option))
  invisible(NULL)
}

#' @rdname work_group
#' @export
list_work_groups <- function(conn) {
  con_error_msg(conn, "Connection already closed.")
  tryCatch(response <- conn@ptr$Athena$list_work_groups())
  response[["WorkGroups"]]
}

#' @rdname work_group
#' @export
get_work_group <- function(conn, work_group = NULL) {
  con_error_msg(conn, "Connection already closed.")
  stopifnot(is.character(work_group))
  tryCatch(response <- conn@ptr$Athena$get_work_group(WorkGroup = work_group))
  response[["WorkGroup"]]
}

#' @rdname work_group
#' @export
update_work_group <- function(conn,
                              work_group = NULL,
                              remove_output_location = FALSE,
                              enforce_work_group_config = FALSE,
                              publish_cloud_watch_metrics = FALSE,
                              bytes_scanned_cut_off = 10000000L,
                              description = NULL,
                              state = c("ENABLED", "DISABLED")) {
  con_error_msg(conn, "Connection already closed.")
  stopifnot(
    is.character(work_group),
    is.logical(remove_output_location),
    is.logical(enforce_work_group_config),
    is.logical(publish_cloud_watch_metrics),
    is.integer(bytes_scanned_cut_off) || is.infinite(bytes_scanned_cut_off),
    is.character(description)
  )

  state <- match.arg(state)
  ConfigurationUpdates <- list(work_group_config_update(conn,
    RemoveOutputLocation = remove_output_location,
    EnforceWorkGroupConfiguration = enforce_work_group_config,
    PublishCloudWatchMetricsEnabled = publish_cloud_watch_metrics,
    BytesScannedCutoffPerQuery = bytes_scanned_cut_off
  ))

  tryCatch(conn@ptr$Athena$update_work_group(
    WorkGroup = work_group,
    Description = description,
    State = state,
    ConfigurationUpdates = ConfigurationUpdates
  ))
  invisible(NULL)
}

#' Get Session Tokens for PAWS Connection
#'
#' Returns a set of temporary credentials for an AWS account or IAM user (\href{https://www.paws-r-sdk.com/docs/sts_get_session_token/}{link}).
#'
#' @param profile_name The name of a profile to use. If not given, then the default profile is used.
#'                     To set profile name, the \href{https://aws.amazon.com/cli/}{AWS Command Line Interface} (AWS CLI) will need to be configured.
#'                     To configure AWS CLI please refer to: \href{https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html}{Configuring the AWS CLI}.
#' @param region_name Default region when creating new connections. Please refer to \href{https://docs.aws.amazon.com/general/latest/gr/rande.html}{link} for
#'                    AWS region codes (region code example: Region = EU (Ireland) 	\code{ region_name = "eu-west-1"})
#' @param serial_number The identification number of the MFA device that is associated with the IAM user who is making the GetSessionToken call.
#'                      Specify this value if the IAM user has a policy that requires MFA authentication. The value is either the serial number for a hardware device
#'                      (such as `GAHT12345678`) or an Amazon Resource Name (ARN) for a virtual device (such as arn:aws:iam::123456789012:mfa/user).
#' @param token_code The value provided by the MFA device, if MFA is required. If any policy requires the IAM user to submit an MFA code,
#'                   specify this value. If MFA authentication is required, the user must provide a code when requesting a set of temporary
#'                   security credentials. A user who fails to provide the code receives an "access denied" response when requesting resources
#'                   that require MFA authentication.
#' @param duration_seconds The duration, in seconds, that the credentials should remain valid. Acceptable duration for IAM user sessions range
#'                         from 900 seconds (15 minutes) to 129,600 seconds (36 hours), with 3,600 seconds (1 hour) as the default.
#' @param set_env If set to \code{TRUE} environmental variables \code{AWS_ACCESS_KEY_ID}, \code{AWS_SECRET_ACCESS_KEY} and \code{AWS_SESSION_TOKEN} will be set.
#'
#' @return \code{get_session_token()} returns a list containing: \code{"AccessKeyId"}, \code{"SecretAccessKey"}, \code{"SessionToken"} and \code{"Expiration"}
#'
#' @examples
#' \dontrun{
#' # Note:
#' # - Require AWS Account to run below example.
#'
#' library(noctua)
#' library(DBI)
#'
#' # Create Temporary Credentials duration 1 hour
#' get_session_token("YOUR_PROFILE_NAME",
#'   serial_number = "arn:aws:iam::123456789012:mfa/user",
#'   token_code = "531602",
#'   set_env = TRUE
#' )
#'
#' # Connect to Athena using temporary credentials
#' con <- dbConnect(athena())
#' }
#' @name session_token
#' @export
get_session_token <- function(profile_name = NULL,
                              region_name = NULL,
                              serial_number = NULL,
                              token_code = NULL,
                              duration_seconds = 3600L,
                              set_env = FALSE) {
  stopifnot(
    is.null(profile_name) || is.character(profile_name),
    is.character(serial_number),
    is.null(token_code) || is.character(token_code),
    is.numeric(duration_seconds),
    is.logical(set_env)
  )

  # get lower level paws methods
  get_region <- pkg_method("get_region", "paws.common")
  get_profile_name <- pkg_method("get_profile_name", "paws.common")

  region_name <- region_name %||% get_region(profile_name)
  profile_name <- profile_name %||% get_profile_name(profile_name)

  config <- cred_set(NULL, NULL, NULL, profile_name, region_name)

  STS <- paws::sts(config)

  tryCatch({
    response <- STS$get_session_token(
      SerialNumber = serial_number,
      TokenCode = token_code,
      DurationSeconds = as.integer(duration_seconds)
    )
  })

  if (set_env) {
    set_aws_env(response)
  }
  response$Credentials
}

#' Assume AWS ARN Role
#'
#' Returns a set of temporary security credentials that you can use to access AWS resources that you might not normally have access to (\href{https://www.paws-r-sdk.com/docs/sts_assume_role/}{link}).
#' These temporary credentials consist of an access key ID, a secret access key, and a security token. Typically, you use AssumeRole within
#' your account or for cross-account access.
#' @param profile_name The name of a profile to use. If not given, then the default profile is used.
#'                     To set profile name, the \href{https://aws.amazon.com/cli/}{AWS Command Line Interface} (AWS CLI) will need to be configured.
#'                     To configure AWS CLI please refer to: \href{https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html}{Configuring the AWS CLI}.
#' @param region_name Default region when creating new connections. Please refer to \href{https://docs.aws.amazon.com/general/latest/gr/rande.html}{link} for
#'                    AWS region codes (region code example: Region = EU (Ireland) 	\code{ region_name = "eu-west-1"})
#' @param role_arn The Amazon Resource Name (ARN) of the role to assume (such as \code{arn:aws:sts::123456789012:assumed-role/role_name/role_session_name})
#' @param role_session_name An identifier for the assumed role session. By default `noctua` creates a session name \code{sprintf("noctua-session-\%s", as.integer(Sys.time()))}
#' @param duration_seconds The duration, in seconds, of the role session. The value can range from 900 seconds (15 minutes) up to the maximum session duration setting for the role.
#'                         This setting can have a value from 1 hour to 12 hours. By default duration is set to 3600 seconds (1 hour).
#' @param set_env If set to \code{TRUE} environmental variables \code{AWS_ACCESS_KEY_ID}, \code{AWS_SECRET_ACCESS_KEY} and \code{AWS_SESSION_TOKEN} will be set.
#' @return \code{assume_role()} returns a list containing: \code{"AccessKeyId"}, \code{"SecretAccessKey"}, \code{"SessionToken"} and \code{"Expiration"}
#' @seealso \code{\link{dbConnect}}
#' @examples
#' \dontrun{
#' # Note:
#' # - Require AWS Account to run below example.
#'
#' library(noctua)
#' library(DBI)
#'
#' # Assuming demo ARN role
#' assume_role(
#'   profile_name = "YOUR_PROFILE_NAME",
#'   role_arn = "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name",
#'   set_env = TRUE
#' )
#'
#' # Connect to Athena using ARN Role
#' con <- dbConnect(noctua::athena())
#' }
#' @export
assume_role <- function(profile_name = NULL,
                        region_name = NULL,
                        role_arn = NULL,
                        role_session_name = sprintf("noctua-session-%s", as.integer(Sys.time())),
                        duration_seconds = 3600L,
                        set_env = FALSE) {
  stopifnot(
    is.null(profile_name) || is.character(profile_name),
    is.null(region_name) || is.character(region_name),
    is.character(role_arn),
    is.numeric(duration_seconds),
    is.logical(set_env)
  )

  # get lower level paws methods
  get_region <- pkg_method("get_region", "paws.common")
  get_profile_name <- pkg_method("get_profile_name", "paws.common")

  region_name <- region_name %||% get_region(profile_name)
  profile_name <- profile_name %||% get_profile_name(profile_name)

  config <- cred_set(NULL, NULL, NULL, profile_name, region_name)

  STS <- paws::sts(config)

  tryCatch({
    response <- STS$assume_role(
      RoleArn = role_arn,
      RoleSessionName = role_session_name,
      DurationSeconds = as.integer(duration_seconds)
    )
  })
  if (set_env) {
    set_aws_env(response)
  }
  response$Credentials
}
