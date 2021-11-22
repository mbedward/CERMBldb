#' Import LAS tile metadata into the database
#'
#' This function extracts meta-data values from the given LAS object and writes
#' them as a new record in one of the database's meta-data tables. The LAS
#' object must have a valid coordinate reference system defined that can be
#' retrieved using the function \code{st_crs} from the \code{'sf'} package. The
#' CRS will be used to select which meta-data table the record should be written
#' to. For example, meta-data for a LAS tile projected in MGA Zone 56 / GDA94
#' (EPSG code 28356) would be written to the table
#' \code{'lidar.metadata_gda94_zone56'}. The function returns an error if either
#' no corresponding meta-data table exists in the database, or the table exists
#' but already contains a record for the LAS tile.
#'
#' The values are:
#' \describe{
#'   \item{provider}{Standard abbreviation for the LiDAR data data provider (
#'     specified as a function argument).}
#'   \item{purpose}{Purpose for which LiDAR data was collected (specified as a
#'     function argument that defaults to 'general').}
#'   \item{filename}{File name of the LAS data file (specified as a function
#'     argument).}
#'   \item{mapname}{Name of the 1:100,000 map sheet containing the data centroid
#'     (can be specified as a function argument or will be inferred from the
#'     file name).}
#'   \item{area_m2}{Area of the bounding rectangle in square metres.}
#'   \item{capture_year}{Four digit integer year number.}
#'   \item{capture_start}{Time stamp (GMT/UCT) of the minimum GPS point time.}
#'   \item{capture_end}{Time stamp (GMT/UCT) of the maximum GPS point time.}
#'   \item{nflightlines}{Number of flight lines within the data.}
#'   \item{npts_ground}{Number of points classified as ground (class 2).}
#'   \item{npts_veg}{Number of points classified as vegetation (classes 3-5).}
#'   \item{npts_water}{Number of points classified as water (class 9).}
#'   \item{npts_other}{Number of points in other classes (e.g. class 6 buildings).}
#'   \item{point_density}{Average point density per square metre.}
#'   \item{bounds}{Bounding rectangle aligned with coordinate reference system axes.}
#' }
#'
#'
#' @param db An open database connection. For this function, the user must have
#'   administrator rights to the database.
#'
#' @param las A LAS object.
#'
#' @param filename Path or file name from which the LAS object was read. Only
#'   the file name, minus any preceding path elements and/or extension, will be
#'   written to the metadata table.
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
#' @examples
#' \dontrun{
#' # Connect to the database as a user with administrator rights
#' db <- DBI::dbConnect(RPostgres::Postgres(),
#'                      dbname = "cermb_lidar",
#'                      host = "some.hostname",
#'                      user = "postgres",
#'                      pasword = "some.password")
#'
#' # Assuming that 'las' is a LAS object, e.g. imported using
#' # CERMBlidar::prepare_tile(), and pre-processed to normalize point
#' # heights and remove any flight line overlap imbalance between
#' # point classes.
#'
#' ldb_load_tile_metadata(
#'   db,
#'   las,
#'   filename = "F:/LAS/Wollongong201304-LID1-C3-AHD_3046190_56_0002_0002.zip"
#' )
#'
#' }
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
    msg <- glue::glue("Meta-data table {tblname}, inferred from the
                      coordinate reference system of the LAS object,
                      does not exist in the database.")
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
