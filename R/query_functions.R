#' Find LiDAR tiles that intersect with given features
#'
#' @param db A database connection
#'
#' @param x A spatial object supported by the \code{sf} package (e.g. a
#'   spatial data frame with one or more point, line or polygon features)
#'   The object must have a coordinate reference system defined.
#'
#' @export
#'
ldb_find_tiles <- function(db, x) {
  .do_check_db(db)

  valid_classes <- c("sf", "sfc")
  ok <- inherits(x, valid_classes)
  if (!ok) stop("The query feature(s) should be an 'sf' spatial object")

  if (is.na(sf::st_crs(x))) {
    stop("The query feature(s) must have a coordinate reference system defined")
  }

  # If the query features are a spatial data frame
  # just take the geometry column
  if (inherits(x, "sf")) {
    x <- st_geometry(x)
  }

  # Get names of the metadata tables to query
  #
  cmd <- glue::glue("select table_name from information_schema.tables
                     where table_schema = 'lidar' and table_name ~* '^metadata';")

  mtbls <- DBI::dbGetQuery(db, cmd)

  # Query each metadata table
  #
  res <- lapply(mtbls$table_name, function(tblname) {
    cmd <- glue::glue("select ST_SRID(geom) as srid from {tblname} limit 1;")

    # PostGIS ST_Intersects assumes that both sets of features have the same
    # coordinate ref system, so transform the query features if required.
    #
    tbl_srid <- DBI::dbGetQuery(db, cmd) %>%
      dplyr::pull(srid)

    tbl_crs <- sf::st_crs(tbl_srid)

    if (sf::st_crs(x) == tbl_crs) {
      xwkt <- sf::st_as_text(x, EWKT=TRUE)
    } else {
      xwkt <- sf::st_as_text( st_transform(x, tbl_crs), EWKT=TRUE )
    }

    # Do the intersection query
    #
    dplyr::tbl(db, tblname) %>%
      dplyr::filter(ST_Intersects(geom, xwkt)) %>%
      dplyr::collect() %>%

      dplyr::mutate(meta_table = tblname,
                    srid = tbl_srid) %>%

      dplyr::select(meta_table, srid, id, filename, capture_year, point_density)
  })

  # Join results and return
  do.call(rbind, res)
}


#' Check if a database connection is valid and open
#'
#' @param db A database connection
#'
#' @export
#'
ldb_is_connected <- function(db) {
  DBI::dbIsValid(db) &&
    tryCatch(DBI::dbGetQuery(db, "select TRUE;")[,1], # Get bool result from returned data frame
             error = function(e) FALSE)
}


.do_check_db <- function(db) {
  if (!ldb_is_connected(db)) stop("Database connection is invalid or closed")
}
