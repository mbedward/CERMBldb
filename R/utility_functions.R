#' Wake up a possibly dormant database connection
#'
#' When working with very large LAS files, loading and processing a file can
#' take so long that the connection to a database server goes to sleep. This
#' function submits a trivial query to the server, repeating if necessary
#' every \code{retry_seconds}, to wake the connection up. Note that this
#' function will not re-open a connection that has been closed.
#'
#' @param db An open but possibly sleeping database connection.
#'
#' @param retry_seconds Number of seconds in between repeated attempts to wake
#'   the connection.
#'
#' @param max_tries Maximum number of times to try waking the connection. The
#'   default is 5. Set to Inf for no limit.
#'
#' @examples
#' \dontrun {
#' # After taking some minutes to read a very large LiDAR file...
#' ldb_wake_up(DB)
#'
#' # ...now the connection is awake and ready to use again
#' ldb_load_tile_metadata(db, las, filename = "foo.las")
#' }
#'
#' @export
#'
ldb_wake_up <- function(db, retry_seconds = 3, max_tries = 5) {
  x <- NULL

  itry <- 1
  while (is.null(x) && itry <= max_tries) {
    x <- tryCatch(
      DBI::dbGetQuery(db, "select 1;"),
      error = function(e) NULL)

    if (is.null(x)) {
      itry <- itry + 1
      Sys.sleep(retry_seconds)
    }
  }
}


#' Write the database creation script to a file
#'
#' The package includes a copy of the SQL script used to create the
#' PostgreSQL/PostGIS database structure that is assumed by the
#' package functions. This function simply copies the script into
#' a specified text file so that you can browse the code or use it
#' to create your very own database.
#'
#' @param dest The destination path and filename. Defaults to
#'   \code{dbcreate.sql} in the current directory.
#'
#' @export
#'
ldb_creation_script <- function(dest = "dbcreate.sql") {
  ok <- file.copy(from = system.file("extdata", "create_database_and_tables.sql",
                                     package = "CERMBldb"),
                  to = dest,
                  overwrite = TRUE)

  if (ok) message("SQL script written to ", dest)
  else message("Problem with destination path")
}
