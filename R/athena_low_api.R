#' Athena Work Groups
#' 
#' @description 
#' Lower level API access, allows user to create and delete Athena Work Groups.
#' \describe{
#' \item{create_work_group}{Creates a workgroup with the specified name (\href{https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.create_work_group}{link}). 
#'                          The work group utilises parameters from the \code{dbConnect} object, to determine the encryption and output location of the work group.
#'                          The s3_staging_dir, encryption_option and kms_key parameters are gotten from \code{\link{dbConnect}}}
#' \item{tag_options}{Helper function to create tag options for function \code{create_work_group()}}
#' \item{delete_work_group}{Deletes the workgroup with the specified name (\href{https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.delete_work_group}{link}). 
#'                          The primary workgroup cannot be deleted.}
#' \item{list_work_groups}{Lists available workgroups for the account (\href{https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.list_work_groups}{link}).}
#' \item{get_work_group}{Returns information about the workgroup with the specified name (\href{https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.get_work_group}{link}).}
#' \item{update_work_group}{Updates the workgroup with the specified name (\href{https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.update_work_group}{link}). 
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
#' \donttest{
#' # Note: 
#' # - Require AWS Account to run below example.
#' # - Different connection methods can be used please see `paws.athena::dbConnect` documnentation
#' 
#' library(paws.athena)
#' 
#' # Demo connection to Athena using profile name 
#' con <- dbConnect(paws.athena::athena())
#'
#' # List current work group available
#' list_work_groups(con)
#'
#' # Create a new work group
#' wg <- create_work_group(con,
#'                   "demo_work_group",
#'                    description = "This is a demo work group",
#'                    tags = tag_options(key= "demo_work_group", value = "demo_01"))
#'  
#' # List work groups to see new work group
#' list_work_groups(con)
#' 
#' # get meta data from work group
#' wg <- get_work_group(con, "demo_work_group")
#' 
#' # Update work group
#' wg <- update_work_group(con, "demo_work_group",
#'                   description = "This is a demo work group update")
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
                              tags = tag_options(key = NULL, value = NULL)){
  if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
  stopifnot(is.character(work_group),
            is.logical(enforce_work_group_config),
            is.logical(publish_cloud_watch_metrics),
            is.integer(bytes_scanned_cut_off) || is.infinite(bytes_scanned_cut_off),
            is.character(description))
  
  request <- list(Name = work_group)
  request["Configuration"] <- list(work_group_config(conn,
                                                     EnforceWorkGroupConfiguration = enforce_work_group_config,
                                                     PublishCloudWatchMetricsEnabled = publish_cloud_watch_metrics,
                                                     BytesScannedCutoffPerQuery = bytes_scanned_cut_off))
  request["Description"] <- description
  request["Tags"] <- tags
  
  tryCatch(do.call(conn@ptr$Athena$create_work_group, request, quote = T))
  invisible(NULL)
}

#' @rdname work_group
#' @export
tag_options <- function(key = NULL,
                        value = NULL){
  stopifnot(is.character(key),
            is.character(value))
  invisible(list(list(list(Key = key, Value = value))))}

#' @rdname work_group
#' @export
delete_work_group <- function(conn, work_group = NULL, recursive_delete_option = FALSE){
  if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
  stopifnot(is.character(work_group),
            is.logical(recursive_delete_option))
  tryCatch(conn@ptr$Athena$delete_work_group(WorkGroup = work_group, RecursiveDeleteOption = recursive_delete_option))
  invisible(NULL)
}

#' @rdname work_group
#' @export
list_work_groups <- function(conn){
  if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
  tryCatch(response <- conn@ptr$Athena$list_work_groups())
  response[["WorkGroups"]]
}

#' @rdname work_group
#' @export
get_work_group <- function(conn, work_group = NULL){
  if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
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
                              state = c("ENABLED", "DISABLED")){
  if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
  stopifnot(is.character(work_group),
            is.logical(remove_output_location),
            is.logical(enforce_work_group_config),
            is.logical(publish_cloud_watch_metrics),
            is.integer(bytes_scanned_cut_off) || is.infinite(bytes_scanned_cut_off),
            is.character(description))
  
  state <- match.arg(state)
  request <- list(WorkGroup = work_group,
                  Description = description,
                  State = state)
  request["ConfigurationUpdates"] <- list(work_group_config_update(conn,
                                                                   RemoveOutputLocation = remove_output_location,
                                                                   EnforceWorkGroupConfiguration = enforce_work_group_config,
                                                                   PublishCloudWatchMetricsEnabled = publish_cloud_watch_metrics,
                                                                   BytesScannedCutoffPerQuery = bytes_scanned_cut_off))
  
  tryCatch(do.call(conn@ptr$Athena$update_work_group, request, quote = T))
  invisible(NULL)
}
