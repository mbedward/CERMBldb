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
#' @param db An open database connection for a user with administrator rights
#'   to the database.
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
#'                      host = "some.host",
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

  las.crs <- sf::st_crs(las)
  bounds <- CERMBlidar::get_las_bounds(las, "sf")

  if (is.na(las.crs)) {
    stop("Cannot determine the coordinate reference system for the LAS object")
  }

  epsgcode <- las.crs$epsg

  # Infer the database metadata table name from the LAS CRS
  crs_wkt <- sf::st_as_text(las.crs)
  x <- stringr::str_extract(crs_wkt, "PROJCS[^\\,]+")

  gda <- stringr::str_extract(x, stringr::regex("GDA\\d+", ignore_case = TRUE))
  if (is.na(gda)) {
    stop("Cannot determine GDA code for coordinate reference system")
  }
  gda <- tolower(gda)

  zone <- stringr::str_extract(x, stringr::regex("zone\\s*\\d+", ignore_case = TRUE)) %>%
    stringr::str_extract("\\d+")

  if (is.na(zone)) {
    stop("Cannot determine MGA map zone for coordinate reference system")
  }

  tblname <- glue::glue("metadata_{gda}_zone{zone}")

  if (!pg_table_exists(db, tblname)) {
    msg <- glue::glue("Require meta-data table {tblname}, as inferred from
                      the coordinate reference system of the LAS object,
                      but it does not exist in the database.")
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
     geom)
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


#' Create a raster table record for point counts derived from a LAS tile
#'
#' This function associates a multi-band raster of point counts, derived from a
#' LAS point cloud using the \code{CERMBlidar::get_stratum_counts} function,
#' with an existing meta-data record for the LAS tile. Usually, it will be
#' called from the \code{ldb_import_tile()} function, but can also be called
#' directly if the integer ID value of the associated meta-data table record is
#' known. The database uses \emph{out-db} storage for raster files, where a
#' raster table record stores the path to the file location, which will usually
#' be AWS S3-compatible storage although other types of storage can be used.
#'
#' If any credentials are required to access the file (e.g. AWS Access Key) it
#' is assumed that this have been set in the environment from which the function
#' is being called.
#'
#' @param db An open database connection for a user with administrator rights
#'   to the database.
#'
#' @param raster_url A URL giving the file location, e.g. in an accessible S3
#'   bucket.
#'
#' @param strata_def A standard strata definition abbreviation, supported by the
#'   database. Default to \code{'cermb'} which corresponds to
#'   \code{CERMBlidar::StrataCERMB}.
#'
#' @param raster_crs The coordinate reference system of the raster file in a
#'   form that can be understood by the \code{sf::st_crs()} function (e.g. a WKT
#'   string or an integer EPSG code. This is used to infer the name of the
#'   relevant meta-data and raster tables in the database. If \code{NULL}
#'   (default), the function will attempt to retrieve the CRS from the raster
#'   file.
#'
#' @param metadata_id Integer ID for the associated record in the meta-data table
#'   associated with the destination raster table. If \code{NULL} (default),
#'   the function will search for a meta-data record based on the file name of
#'   the input raster.
#'
#' @param protocol The name of a GDAL-supported virtual file system protocol
#'   to use when accessing the raster file from storage. The default is
#'   \code{'vsicurl'}. See
#'   \href{https://gdal.org/user/virtual_file_systems.html}{GDAL online documentation}
#'   for other options. This will be stored as part of the raster file path in
#'   the database raster table record.
#'
#' @param tilew The tile size to use for the raster.
#'   See this \href{https://blog.crunchydata.com/blog/postgis-raster-and-crunchy-bridge}{
#'   Postgis raster example} for more details.
#'
#' @param R2P The path to the Postgis command line program \code{raster2pgsql}.
#'   If \code{NULL} or not found, the function will attempt to locate the
#'   program on the client system.
#'
#' @param host Host/server on which the database resides. If \code{NULL}
#'   (default), will be determined from the database connection object.
#'
#' @param dbname Database name. If \code{NULL} (default), will be determined
#'   from the database connection object.
#'
#' @param user User name. If \code{NULL} (default), will be determined from the
#'   database connection object.
#'
#' @param password User password. If \code{NULL}, this will automatically fall
#'   back to the user's \code{pgpass.conf} file, if one exists, or the
#'   \code{PGPASSWORD} environment variable. On a Windows client, the \code{pgpass.conf} file
#'   in the directlry \code{~/AppData/Roaming/postgresql}.
#'
#' @export
#'
# Function to load one or more rasters given the S3 URL for the GeoTIFF file(s)
#
fn_load_raster <- function(db,
                           raster_url,
                           strata_def = "cermb",
                           raster_crs = NULL,
                           metadata_id = NULL,
                           protocol = "vsicurl",
                           tilew = 256,
                           R2P = "C:/Program Files/PostgreSQL/12/bin/raster2pgsql.exe",
                           host = NULL,
                           dbname = NULL,
                           username = NULL,
                           password = NULL) {

  protocol <- stringr::str_trim(protocol[1])
  if (!grepl("^/", protocol)) protocol <- glue::glue("/{protocol}")

  vsi_url <- glue::glue("{protocol}/{raster_url}")

  # Check that the raster2pgsql program can be found
  if (is.null(R2P)) {
    R2P <- .find_raster2pgsql()
  } else {
    info <- file.info(R2P)

    if (is.na(info$exe)) {
      msg <- glue::glue("The raster2psql program was not found at {R2P}.")
      stop(msg)

    } else if (info$exe == "no") {
      msg <- glue::glue("The supplied path for R2P ({R2P})
                         is not an executable file.")
      stop(msg)
    }
  }

  r <- terra::rast(vsi_url)

  # Check that the number of layers corresponds to the
  # strata definition
  cmd <- glue::glue("select * from stratum where strata_def = '{strata_def}';")
  dat_strata <- dbGetQuery(db, cmd)

  if (nrow(dat_strata) == 0) {
    msg <- glue::glue("'{strata_def}' is not recognized as a supported
                       strata definition in the database.")
    stop(msg)
  }

  nb <- terra::nlyr(r)
  if (nb != nrow(dat_strata)) {
    msg <- glue::glue("Number of raster bands ({nbands}) does not correspond to
                       the strata definition '{strata_def}' which has {nrow(dat_strata)} levels.")
    stop(msg)
  }


  rcrs <- sf::st_crs(r)

  if (rcrs$IsGeographic) {
    msg <- glue::glue("The raster is in a geographic coordinate system but needs
                       to be in a projected coordinate system supported by the database.")
    stop(msg)
  }

  ZONE <- rcrs$zone
  if (is.null(ZONE)) {
    msg <- glue::glue("Cannot determine the map zone. Make sure that the
                       raster is in a projected coordinate system supported by
                       the database.")

    stop(msg)
  }

  GDA <- stringr::str_extract(sf::st_as_text(rcrs),
                              stringr::regex("GDA\\d+", ignore_case = TRUE)) %>%
    stringr::str_extract("\\d+") %>%
    as.integer()

  ok <- (GDA %in% c(94, 2020)) && (ZONE %in% 55:56)
  if (!ok) {
    msg <- glue::glue("The raster does not appear to be in one of the GDA94 or GDA2020
                       projected coordinate reference systems supported by the database.")
    stop(msg)
  }

  EPSG <- rcrs$epsg
  if (is.na(EPSG)) {
    msg <- glue::glue("Cannot determine the EPSG code for the raster's
                       coordinate reference system.")
  }

  # Table name suffix
  suffix <- glue::glue("gda{GDA}_zone{ZONE}")

  # Check for meta-data
  #
  if (!(is.null(metadata_id) || is.na(metadata_id))) {
    # metadata_id is provided: check that it exists in the database table
    cmd <- glue::glue("select exists (
                         select 1 from lidar.metadata_{suffix}
                         where id = {metadata_id}
                       );")

    ok <- dbGetQuery(db, cmd)$exists

    if (!ok) {
      msg <- glue::glue("No meta-data record was found in the table
                         {metadata_{suffix} with id = {metadata_id}.")
      stop(msg)
    }
  } else {
    # metadata_id is not provided: try to find it by matching the input
    # raster file name
    fname <- fs::path_file(vsi_url) %>%
      fs::path_ext_remove(.)

    cmd <- glue::glue("select id from lidar.metadata_{suffix}
                       where filename = '{fname}';")

    res <- dbGetQuery(db, cmd)

    if (nrow(res) == 0) {
      msg <- glue::glue("No existing record was found that corresponds to this
                         file name in the meta-data table metadata_{suffix}.")
      stop(msg)

    } else if (nrow(res) > 1) {
      msg <- glue::glue("More than one meta-data record found that corresponds
                         to this file name in the meta-data table metadata_{suffix}.
                         This should never happen! Please check the database.")
      stop(msg)
    }

    metadata_id <- res$id
  }

  # Check for an existing raster record
  cmd <- glue::glue("select exists(
                      select 1 from pointcounts_{suffix}
                      where meta_id = {metadata_id}
                    )")

  res <- dbGetQuery(db, cmd)
  if (res$exists) {
    msg <- glue::glue("There is an existing raster record in the raster table
                       pointcounts_{suffix} with a matching meta_id value ({metadata_id}).
                       Please check before proceeding further.")
    stop(msg)
  }

  # Use the command line raster2pgsql program to create the SQL
  # for the raster import
  fsql <- tempfile(pattern = "sql", fileext = ".txt")

  args <- glue::glue("-s {EPSG} -t {tilew}x{tilew} \\
                     -I -R {vsi_url} \\
                     lidar.temp_load")

  system2(command = R2P, args = args, stdout = fsql)

  # Run the generated SQL to load the raster to the 'temp_load' table
  queries <- readLines(fsql)
  sapply(queries, function(q) dbExecute(db, q))

  # Copy the raster from the temp_load table to the point
  # counts table for this map zone

  cmd <- glue::glue("
    insert into lidar.pointcounts_{suffix} (strata_def, meta_id, rast)
    select 'cermb_old' as strata_def,
    {metadata_id} as meta_id,
    rast from lidar.temp_load;")

  res <- dbExecute(db, cmd)

  dbExecute(db, "drop table lidar.temp_load;")

  if (!is.null(res) && res > 0) {
    TRUE
  } else {
    FALSE
  }
}


# Private helper to find the raster2pgsql Postgis command line program.
#
.find_raster2pgsql <- function() {
  stop("Bummer! The .find_raster2pgsql() helper function is not working yet")
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
