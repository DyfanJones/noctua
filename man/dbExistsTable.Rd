% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/Connection.R
\docType{methods}
\name{dbExistsTable}
\alias{dbExistsTable}
\alias{dbExistsTable,AthenaConnection,character-method}
\alias{dbExistsTable,AthenaConnection,Id-method}
\title{Does Athena table exist?}
\usage{
\S4method{dbExistsTable}{AthenaConnection,character}(conn, name, ...)

\S4method{dbExistsTable}{AthenaConnection,Id}(conn, name, ...)
}
\arguments{
\item{conn}{A \linkS4class{DBIConnection} object, as returned by
\code{\link[DBI:dbConnect]{dbConnect()}}.}

\item{name}{The table name, passed on to \code{\link[DBI:dbQuoteIdentifier]{dbQuoteIdentifier()}}. Options are:
\itemize{
\item a character string with the unquoted DBMS table name,
e.g. \code{"table_name"},
\item a call to \code{\link[DBI:Id]{Id()}} with components to the fully qualified table name,
e.g. \code{Id(schema = "my_schema", table = "table_name")}
\item a call to \code{\link[DBI:SQL]{SQL()}} with the quoted and fully qualified table name
given verbatim, e.g. \code{SQL('"my_schema"."table_name"')}
}}

\item{...}{Other parameters passed on to methods.}
}
\value{
\code{dbExistsTable()} returns logical scalar. \code{TRUE} if the table exists, \code{FALSE} otherwise.
}
\description{
Returns logical scalar if the table exists or not. \code{TRUE} if the table exists, \code{FALSE} otherwise.
}
\examples{
\dontrun{
# Note:
# - Require AWS Account to run below example.
# - Different connection methods can be used please see `noctua::dbConnect` documnentation

library(DBI)

# Demo connection to Athena using profile name
con <- dbConnect(noctua::athena())

# Write data.frame to Athena table
dbWriteTable(con, "mtcars", mtcars,
  partition = c("TIMESTAMP" = format(Sys.Date(), "\%Y\%m\%d")),
  s3.location = "s3://mybucket/data/"
)

# Check if table exists from Athena
dbExistsTable(con, "mtcars")

# Disconnect conenction
dbDisconnect(con)
}
}
\seealso{
\code{\link[DBI]{dbExistsTable}}
}
