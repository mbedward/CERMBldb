#' Import LAS tile metadata into the database
#'
#' This function extracts meta-data values from the given LAS object and
#' writes them to the database. The values are:
#'
#' @param db An open database connection. For this function, the user must have
#'   administrator rights to the database.
#'
#' @param las A LAS object.
#'
#' @param filename Path or filename from which the LAS object was read.
#'
#' @param mapname Name of the map sheet (assumed to be 100k topographic map) to
#'   assign to this LAS tile. If \code{NULL} (the default), the map name will be
#'   taken from the filename using the regular expression pattern
#'   \code{'^[A-Za-z]+'}
#'
#' @param provider Abbreviation for the LiDAR data provider. This value must
#'   appear in the \code{'providers'} database table. The default is 'nsw_ss'
#'   for NSW Spatial Services.
#'
#' @param purpose Character value for the 'purpose' column of the database
#'   table. Usually left as the default (\code{'general'}).
#'
#' @return The integer value of the \code{'id'} field for the newly created
#'   database record.
#'
#' @export
#'
ldb_load_tile_metadata <- function(db,
                                   las,
                                   filename,
                                   mapname = NULL,
                                   provider = "nsw_ss",
                                   purpose = "general") {

  if (!ldb_is_connected(db)) stop("Database connection is not open")

  las.crs <- st_crs(las)
  bounds <- CERMBlidar::get_las_bounds(las, "sf")

  if (is.na(las.crs)) {
    stop("Cannot determine the coordinate reference system for the LAS object")
  }

  epsgcode <- las.crs$epsg

  # Infer the database metadata table name from the LAS CRS
  x <- stringr::str_extract(st_as_text(las.crs), "PROJCS[^\\,]+")
  gda <- stringr::str_extract(x, stringr::regex("GDA\\d+", ignore_case = TRUE))
  if (is.na(gda)) stop("Cannot determine GDA code for coordinate reference system")

  gda <- tolower(gda)

  zone <- stringr::str_extract(x, stringr::regex("zone\\s*\\d+", ignore_case = TRUE)) %>%
    stringr::str_extract("\\d+")

  if (is.na(zone)) stop("Cannot determine MGA map zone for coordinate reference system")

  tblname <- glue::glue("metadata_{gda}_zone{zone}")

  if (!pg_table_exists(db, tblname)) {
    msg <- glue::glue("Table {tblname} not found in the database")
    stop(msg)
  }

  filename <- filename %>%
    fs::path_file() %>%
    fs::path_ext_remove()

  if (is.null(mapname)) {
    mapname <- stringr::str_extract(filename, "^[A-Za-z]+")
  }

  scantimes <- CERMBlidar::get_scan_times(las, by = "all")
  capture_year <- lubridate::year(scantimes$time.start[1])

  pcounts <- CERMBlidar::get_class_frequencies(las)
  ptotal <- Reduce(sum, pcounts)

  nflightlines <- length(unique(las@data$flightlineID))

  wkt <- sf::st_as_text(bounds)
  area <- sf::st_area(bounds)

  command <- glue::glue(
    "insert into {tblname} \\
    (provider, purpose,
     filename, mapname,
     area_m2,
     capture_year, capture_start, capture_end,
     nflightlines,
     npts_ground, npts_veg, npts_water, npts_other,
     point_density,
     bounds)
    values (
    '{provider}',
    '{purpose}',
    '{filename}',
    '{mapname}',
    {area},
    {capture_year},
    '{.tformat(scantimes[1,1])}',
    '{.tformat(scantimes[1,2])}',
    {nflightlines},
    {pcounts$ground},
    {pcounts$veg},
    {pcounts$water},
    {pcounts$building + pcounts$other},
    {ptotal / area},
    ST_GeomFromText('{wkt}', {epsgcode}) );
    ")

  DBI::dbExecute(db, command)

  # Return id value of the record just created
  cmd <- glue::glue("select id from {tblname} where filename = '{filename}';")
  res <- DBI::dbGetQuery(db, cmd)

  res$id[1]
}



#' Check if a table exists in the given database
#'
#' @param db An open database connection.
#'
#' @param tablename Name of the table (schema is assumed to be 'lidar').
#'
#' @return \code{TRUE} if the table was found; \code{FALSE} otherwise.
#'
#' @export
#'
pg_table_exists <- function(db, tablename) {
  command <- glue::glue("select exists (
                        select * from information_schema.tables
                        where table_schema = 'lidar' and
                        table_name = '{tablename}');")

  res <- DBI::dbGetQuery(db, command)

  res$exists
}


# Private helper to format a time stamp to include the time zone
# (used by ldb_load_tile_metadata)
.tformat <- function(timestamp, tz = "UTC") {
  if (inherits(timestamp, c("POSIXt", "Date"))) {
    x <- timestamp
  } else if (inherits(timestamp, "character")) {
    x <- lubridate::parse_date_time(timestamp,
                                    orders = c("ymd", "ymd H", "ymd HM", "ymd HMS"),
                                    tz=tz)
  } else {
    stop("Argument timestamp should be POSIXt, Date, character")
  }

  format(timestamp, format = "%Y-%m-%d %H:%M:%S", usetz = TRUE)
}
