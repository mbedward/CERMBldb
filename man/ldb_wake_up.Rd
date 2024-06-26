% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utility_functions.R
\name{ldb_wake_up}
\alias{ldb_wake_up}
\title{Wake up a possibly dormant database connection}
\usage{
ldb_wake_up(db, retry_seconds = 3, max_tries = 5)
}
\arguments{
\item{db}{An open but possibly sleeping database connection.}

\item{retry_seconds}{Number of seconds in between repeated attempts to wake
the connection.}

\item{max_tries}{Maximum number of times to try waking the connection. The
default is 5. Set to Inf for no limit.}
}
\description{
When working with very large LAS files, loading and processing a file can
take so long that the connection to a database server goes to sleep. This
function submits a trivial query to the server, repeating if necessary
every \code{retry_seconds}, to wake the connection up. Note that this
function will not re-open a connection that has been closed.
}
\examples{
\dontrun{
# After taking some minutes to read a very large LiDAR file...
ldb_wake_up(DB)

# ...now the connection is awake and ready to use again
ldb_load_tile_metadata(db, las, filename = "foo.las")
}

}
