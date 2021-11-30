#' Find LiDAR tiles that intersect with given features
#'
#' Queries the LiDAR tile metadata table ('lidar.metadata') in the database to
#' identify tiles that intersect with the specified features.
#'
#' @param db A database connection.
#'
#' @param x A spatial object with supported by the \code{sf} package: either an
#'   \code{sf} data frame or a \code{sfc} geometry list. The object must have a
#'   coordinate reference system defined. It can contain one or more point, line
#'   or polygon features.
#'
#' @return A data frame with one row per matching LiDAR tile, and columns:
#'   'tile_id' (unique integer identifier for the tile),
#'   'datum' (original datum, either 'GDA94' or 'GDA2020'),
#'   'zone' (MGA map zone, one of 54, 55 or 56),
#'   'srid' (spatial reference identifier (EPSG code),
#'   'filename' (source file name for the LiDAR data),
#'   'capture_year' (4-digit year number),
#'   'point_density' (average density across the tile expressed as points per square metre).
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
    x <- sf::st_geometry(x)
  }

  # Get the metadata table CRS.
  # PostGIS ST_Intersects assumes that both sets of features have the same
  # coordinate ref system, so we have to transform the query features
  # as required.
  #
  cmd <- glue::glue("select ST_SRID(geom) as srid from metadata limit 1;")

  meta_srid <- DBI::dbGetQuery(db, cmd) %>%
    dplyr::pull(srid)

  meta_crs <- sf::st_crs(meta_srid)

  if (sf::st_crs(x) == meta_crs) {
    xwkt <- sf::st_as_text(x, EWKT=TRUE)
  } else {
    xwkt <- sf::st_as_text( sf::st_transform(x, meta_crs), EWKT=TRUE )
  }

  # Do the intersection query
  #
  cmd <- glue::glue("select m.tile_id, m.datum, m.zone, s.srid,
                     m.filename, m.capture_year, m.point_density
                     from metadata m
                     left join supported_crs s
                     on m.datum = s.datum and m.zone = s.zone
                     where ST_Intersects(geom, ST_GeomFromText('{xwkt}'));")

  dbGetQuery(db, cmd)
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

