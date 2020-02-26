#' noctua: a DBI interface into Athena using paws SDK
#' 
#' noctua provides a seamless DBI interface into Athena using the python package 
#' \href{https://github.com/paws-r/paws}{paws}. 
#' 
#' @section Goal of Package:
#' The goal of the \code{noctua} package is to provide a DBI-compliant interface to \href{https://aws.amazon.com/athena/}{Amazon’s Athena}
#' using \code{paws} software development kit (SDK). This allows for an efficient, easy setup connection to Athena using the \code{paws} SDK as a driver.
#' 
#' @section AWS Command Line Interface:
#' As noctua is using \code{paws} as it's backend, \href{https://aws.amazon.com/cli/}{AWS Command Line Interface (AWS CLI)} can be used
#' to remove user credentials when interacting with Athena.
#' 
#' This allows AWS profile names to be set up so that noctua can connect to different accounts from the same machine,
#' without needing hard code any credentials.
#'
#' @import paws
#' @importFrom utils packageVersion head
#' @importFrom stats runif
#' @import data.table
#' @import DBI
"_PACKAGE"
