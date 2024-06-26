#' Import LAS tile metadata into the database
#'
#' This function extracts meta-data values from the given LAS object and writes
#' them as a new record in the 'lidar.metadata' database table. The LAS
#' object must have a valid coordinate reference system defined that can be
#' retrieved using the function \code{st_crs} from the \code{'sf'} package.
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
#' @param crs_fallback The coordinate reference system to apply if one cannot be
#'   retrieved from the input LAS object. This can either be an object of class
#'   \code{'crs'} created with function \code{\link[sf]{st_crs}}, an integer EPSG
#'   code, or any spatial object from which a CRS can be extracted. The default
#'   value \code{NULL}, or a missing value will cause the function to stop with
#'   an error if the LAS object does not have a CRS.
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
                                   purpose = "general",
                                   crs_fallback = NULL) {

  if (!ldb_is_connected(db)) stop("Database connection is not open")

  las.crs <- CERMBlidar::get_horizontal_crs(las)

  if (is.na(las.crs)) {
    # Try getting a CRS from the crs_fallback argument.
    # This will result in an NA crs the arg is NULL.
    #
    las.crs <- CERMBlidar::get_horizontal_crs(crs_fallback)

    if (is.na(las.crs)) {
      stop("Cannot determine the coordinate reference system for the LAS object")

    } else {
      # Assign the CRS to the LAS object so that further steps (below) will work
      lidR::crs(las) <- las.crs
    }
  }

  epsgcode <- las.crs$epsg

  # Check that this projection is supported by the database
  cmd <- glue::glue("select exists (
                       select 1 from supported_crs
                       where srid = {epsgcode} );")

  res <- DBI::dbGetQuery(db, cmd)
  if (!res$exists[1]) {
    msg <- glue::glue("The coordinate reference system (EPSG:{epsgcode})
                       is not (yet) supported by the database.")
    stop(msg)
  }

  # Get the datum and zone from the LAS CRS
  crs_wkt <- sf::st_as_text(las.crs)
  x <- stringr::str_extract(crs_wkt, "PROJCS[^\\,]+")

  datum <- stringr::str_extract(x, stringr::regex("GDA\\d+", ignore_case = TRUE))
  if (is.na(datum)) {
    stop("Cannot determine GDA code for coordinate reference system")
  }

  zone <- stringr::str_extract(x, stringr::regex("zone\\s*\\d+", ignore_case = TRUE)) %>%
    stringr::str_extract("\\d+")

  if (is.na(zone)) {
    stop("Cannot determine MGA map zone for coordinate reference system")
  }

  # Get the tile bounds in its native CRS coordinates
  bounds <- CERMBlidar::get_las_bounds(las, "sf")

  # Reproject bounds into the CRS used by the metadata table
  cmd <- "select Find_SRID('lidar', 'metadata', 'geom') AS srid;"
  res <- DBI::dbGetQuery(db, cmd)

  if (nrow(res) != 1) {
    msg <- glue::glue("Something really bad has happened!!!
                       Unable to determine the projection for the metadata table.
                       This suggests that the database might have been corrupted.")
    stop(msg)
  }

  metadata_epsg <- res$srid;
  bounds_reproj <- sf::st_transform(bounds, metadata_epsg)

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

  wkt <- sf::st_as_text(bounds_reproj, EWKT = TRUE)
  area <- sf::st_area(bounds)

  command <- glue::glue(
    "insert into metadata \\
    (filename, datum, zone,
     provider, purpose,
     mapname,
     area_m2,
     capture_year, capture_start, capture_end,
     nflightlines,
     npts_ground, npts_veg, npts_water, npts_other,
     point_density,
     geom)
    values (
    '{filename}',
    '{datum}',
    {zone},
    '{provider}',
    '{purpose}',
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
    ST_GeomFromText('{wkt}') ) returning tile_id;
    ")

  res <- DBI::dbGetQuery(db, command)

  # Return tile_id value of the record just created
  res$tile_id[1]
}


#' Create a raster table record for point counts derived from a LAS tile
#'
#' This function associates a multi-band raster of point counts, derived from a
#' LAS point cloud using the \code{CERMBlidar::get_stratum_counts} function,
#' with an existing meta-data record for the LAS tile. The database uses
#' \emph{out-db} storage for raster files, where a raster table record stores
#' the path to the file location, which will usually be AWS S3-compatible
#' storage although other types of storage can be used.
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
#' @param crs_fallback The coordinate reference system of the raster file in a
#'   form that can be understood by the \code{sf::st_crs()} function (e.g. a WKT
#'   string or an integer EPSG code. This is used to infer the name of the
#'   relevant meta-data and raster tables in the database. If \code{NULL}
#'   (default), the function will attempt to retrieve the CRS from the raster
#'   file.
#'
#' @param tile_id Integer ID for the corresponding existing record in the
#'   'metadata' table. If \code{NULL} (default), the function will search for a
#'   meta-data record based on the file name of the input raster.
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
#' @export
#'
ldb_load_pointcount_raster <- function(db,
                                       raster_url,
                                       strata_def = "cermb",
                                       crs_fallback = NULL,
                                       tile_id = NULL,
                                       protocol = "vsicurl",
                                       tilew = 256,
                                       R2P = "C:/Program Files/PostgreSQL/12/bin/raster2pgsql.exe") {

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
  rcrs <- sf::st_crs(r)

  # Check CRS - sometimes reading via /vsicurl seems to lose
  # the the CRS of the raster
  #
  if (is.na(sf::st_crs(r))) {
    if (!is.null(crs_fallback)) {
      rcrs <- sf::st_crs(crs_fallback)
      terra::crs(r) <- sf::st_as_text(rcrs)
    } else {
      stop("Raster has no CRS defined and no crs_fallback value was provided")
    }
  }

  if (rcrs$IsGeographic) {
    msg <- glue::glue("The raster is in a geographic coordinate system but needs
                       to be in a projected coordinate system supported by the database.")
    stop(msg)
  }

  # Check that the number of layers corresponds to the
  # strata definition
  cmd <- glue::glue("select * from stratum where strata_def = '{strata_def}';")
  dat_strata <- DBI::dbGetQuery(db, cmd)

  if (nrow(dat_strata) == 0) {
    msg <- glue::glue("'{strata_def}' is not recognized as a supported
                       strata definition in the database.")
    stop(msg)
  }

  nb <- terra::nlyr(r)
  if (nb != nrow(dat_strata)) {
    msg <- glue::glue("Number of raster bands ({nb}) does not correspond to
                       the strata definition '{strata_def}' which has {nrow(dat_strata)} levels.")
    stop(msg)
  }

  # Check that this projection is supported by the database and,
  # if so, get the datum and zone.
  EPSG <- rcrs$epsg

  cmd <- glue::glue("select datum, zone
                     from supported_crs
                     where srid = {EPSG};")

  res <- DBI::dbGetQuery(db, cmd)
  if (nrow(res) != 1) {
    msg <- glue::glue("The coordinate reference system (EPSG:{epsgcode})
                       is not (yet) supported by the database.")
    stop(msg)
  }

  ZONE <- res$zone
  DATUM <- res$datum

  # Table name suffix
  suffix <- glue::glue("{tolower(DATUM)}_zone{ZONE}")

  # Check for meta-data record
  #
  if (!(is.null(tile_id) || is.na(tile_id))) {
    # tile_id was provided: check that it exists in the metadata table
    cmd <- glue::glue("select exists (
                         select 1 from lidar.metadata
                         where tile_id = {tile_id}
                       );")

    ok <- DBI::dbGetQuery(db, cmd)$exists

    if (!ok) {
      msg <- glue::glue("No meta-data record was found with tile_id = {tile_id}.")
      stop(msg)
    }

  } else {
    # tile_id was not provided: try to find it by matching the input
    # raster file name
    fname <- fs::path_file(vsi_url) %>%
      fs::path_ext_remove(.)

    cmd <- glue::glue("select tile_id from lidar.metadata
                       where filename = '{fname}' and
                       datum = '{DATUM}' and
                       zone = {ZONE};")

    res <- DBI::dbGetQuery(db, cmd)

    if (nrow(res) == 0) {
      msg <- glue::glue("No existing record was found that corresponds to this
                         raster's file name and map projection in the meta-data table.")
      stop(msg)

    } else if (nrow(res) > 1) {
      msg <- glue::glue("More than one meta-data record found that corresponds
                         to this file name, datum and zone.

                         This should never happen! Please check the database.")
      stop(msg)
    }

    tile_id <- res$tile_id
  }

  # Check for an existing raster record
  cmd <- glue::glue("select exists(
                      select 1 from pointcounts_{suffix}
                      where tile_id = {tile_id}
                    )")

  res <- DBI::dbGetQuery(db, cmd)
  if (res$exists) {
    msg <- glue::glue("There is an existing raster record in the raster table
                       pointcounts_{suffix} with a matching tile_id value ({tile_id}).
                       Please check before proceeding further.")
    stop(msg)
  }

  x <- rlang::hash(Sys.time())
  TEMP_LOAD_TABLE <- glue::glue("temp_load_{x}")

  # Use the command line raster2pgsql program to create the SQL
  # for the raster import
  fsql <- tempfile(pattern = "sql", fileext = ".txt")

  args <- glue::glue("-s {EPSG} -t {tilew}x{tilew} \\
                     -I -R {vsi_url} \\
                     lidar.{TEMP_LOAD_TABLE}")

  system2(command = R2P, args = args, stdout = fsql)

  # Run the generated SQL to load the raster to the temporary load table
  queries <- readLines(fsql)
  sapply(queries, function(q) DBI::dbExecute(db, q))

  # Copy the raster from the temp_load table to the point
  # counts table for this map zone

  cmd <- glue::glue("
    insert into lidar.pointcounts_{suffix} (strata_def, tile_id, rast)
    select '{strata_def}' as strata_def,
    {tile_id} as tile_id,
    rast from lidar.{TEMP_LOAD_TABLE};")

  res <- DBI::dbExecute(db, cmd)

  # Drop the temp load table
  cmd <- glue::glue("drop table lidar.{TEMP_LOAD_TABLE};")
  DBI::dbExecute(db, cmd)

  if (!is.null(res) && res > 0) {
    TRUE
  } else {
    FALSE
  }
}


#' Create a raster table record for vegetation cover derived from a LAS tile
#'
#' This function associates a single-band raster of vegetation cover, usually
#' derived from a LAS point cloud using the \code{CERMBlidar::get_stratum_cover}
#' function, with an existing meta-data record for the LAS tile. The database
#' uses \emph{out-db} storage for raster files, where a raster table record
#' stores the path to the file location, which will usually be AWS S3-compatible
#' storage although other types of storage can be used.
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
#' @param crs_fallback The coordinate reference system of the raster file in a
#'   form that can be understood by the \code{sf::st_crs()} function (e.g. a WKT
#'   string or an integer EPSG code. This is used to infer the name of the
#'   relevant meta-data and raster tables in the database. If \code{NULL}
#'   (default), the function will attempt to retrieve the CRS from the raster
#'   file.
#'
#' @param tile_id Integer ID for the corresponding existing record in the
#'   'metadata' table. If \code{NULL} (default), the function will search for a
#'   meta-data record based on the file name of the input raster.
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
#' @export
#'
ldb_load_vegcover_raster <- function(db,
                                     raster_url,
                                     strata_def = "specht",
                                     crs_fallback = NULL,
                                     tile_id = NULL,
                                     protocol = "vsicurl",
                                     tilew = 256,
                                     R2P = "C:/Program Files/PostgreSQL/12/bin/raster2pgsql.exe") {

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
  rcrs <- sf::st_crs(r)

  # Check CRS - sometimes reading via /vsicurl seems to lose
  # the the CRS of the raster
  #
  if (is.na(sf::st_crs(r))) {
    if (!is.null(crs_fallback)) {
      rcrs <- sf::st_crs(crs_fallback)
      terra::crs(r) <- sf::st_as_text(rcrs)
    } else {
      stop("Raster has no CRS defined and no crs_fallback value was provided")
    }
  }

  if (rcrs$IsGeographic) {
    msg <- glue::glue("The raster is in a geographic coordinate system but needs
                       to be in a projected coordinate system supported by the database.")
    stop(msg)
  }

  # Check that the number of layers corresponds to the
  # strata definition (minus 1 for the ground layer)
  cmd <- glue::glue("select * from stratum where strata_def = '{strata_def}';")
  dat_strata <- DBI::dbGetQuery(db, cmd)

  if (nrow(dat_strata) == 0) {
    msg <- glue::glue("'{strata_def}' is not recognized as a supported
                       strata definition in the database.")
    stop(msg)
  }

  nb <- terra::nlyr(r)
  if (nb != nrow(dat_strata) - 1) {
    msg <- glue::glue("Number of raster bands ({nb}) does not correspond to
                       the strata definition '{strata_def}'. There should be {nrow(dat_strata)-1} bands.")
    stop(msg)
  }

  # Check that this projection is supported by the database and,
  # if so, get the datum and zone.
  EPSG <- rcrs$epsg

  cmd <- glue::glue("select datum, zone
                     from supported_crs
                     where srid = {EPSG};")

  res <- DBI::dbGetQuery(db, cmd)

  if (nrow(res) != 1) {
    msg <- glue::glue("The coordinate reference system (EPSG:{epsgcode})
                       is not (yet) supported by the database.")
    stop(msg)
  }

  ZONE <- res$zone
  DATUM <- res$datum

  # Table name
  TBL_NAME <- glue::glue("vegcover_{tolower(DATUM)}_zone{ZONE}")


  # Check that the table exists
  if (!pg_table_exists(db, TBL_NAME)) {
    msg <- glue::glue("No vegetation cover table for {DATUM}, zone {ZONE}")
    stop(msg)
  }

  # Check for meta-data record
  #
  if (!(is.null(tile_id) || is.na(tile_id))) {
    # tile_id was provided: check that it exists in the metadata table
    cmd <- glue::glue("select exists (
                         select 1 from lidar.metadata
                         where tile_id = {tile_id}
                       );")

    ok <-DBI::dbGetQuery(db, cmd)$exists

    if (!ok) {
      msg <- glue::glue("No meta-data record was found with tile_id = {tile_id}.")
      stop(msg)
    }

  } else {
    # tile_id was not provided: try to find it by matching the input
    # raster file name
    fname <- fs::path_file(vsi_url) %>%
      fs::path_ext_remove(.)

    cmd <- glue::glue("select tile_id from lidar.metadata
                       where filename = '{fname}' and
                       datum = '{DATUM}' and
                       zone = {ZONE};")

    res <-DBI::dbGetQuery(db, cmd)

    if (nrow(res) == 0) {
      msg <- glue::glue("No existing record was found that corresponds to this
                         raster's file name and map projection in the meta-data table.")
      stop(msg)

    } else if (nrow(res) > 1) {
      msg <- glue::glue("More than one meta-data record found that corresponds
                         to this file name, datum and zone.

                         This should never happen! Please check the database.")
      stop(msg)
    }

    tile_id <- res$tile_id
  }

  # Check for an existing raster record
  cmd <- glue::glue("select exists(
                      select 1 from {TBL_NAME}
                      where tile_id = {tile_id}
                    )")

  res <- DBI::dbGetQuery(db, cmd)
  if (res$exists) {
    msg <- glue::glue("There is an existing raster record in the raster table
                       {TBL_NAME} with a matching tile_id value ({tile_id}).
                       Please check before proceeding further.")
    stop(msg)
  }

  x <- rlang::hash(Sys.time())
  TEMP_LOAD_TABLE <- glue::glue("temp_load_{x}")

  # Use the command line raster2pgsql program to create the SQL
  # for the raster import
  fsql <- tempfile(pattern = "sql", fileext = ".txt")

  args <- glue::glue("-s {EPSG} -t {tilew}x{tilew} \\
                     -I -R {vsi_url} \\
                     lidar.{TEMP_LOAD_TABLE}")

  system2(command = R2P, args = args, stdout = fsql)

  # Run the generated SQL to load the raster to the temporary load table
  queries <- readLines(fsql)
  sapply(queries, function(q) DBI::dbExecute(db, q))

  # Copy the raster from the temp_load table to the veg cover
  # table for this map zone

  cmd <- glue::glue("
    insert into lidar.{TBL_NAME} (tile_id, strata_def, rast)
    select {tile_id} as tile_id,
      '{strata_def}' as strata_def,
      rast from lidar.{TEMP_LOAD_TABLE};")

  res <- DBI::dbExecute(db, cmd)

  # Drop the temp load table
  cmd <- glue::glue("drop table lidar.{TEMP_LOAD_TABLE};")
  DBI::dbExecute(db, cmd)

  if (!is.null(res) && res > 0) {
    TRUE
  } else {
    FALSE
  }
}



#' Create a raster table record for vegetation height derived from a LAS tile
#'
#' This function associates a single-band raster of vegetation height, usually
#' derived from a LAS point cloud using the \code{CERMBlidar::get_max_height}
#' function, with an existing meta-data record for the LAS tile. The database
#' uses \emph{out-db} storage for raster files, where a raster table record
#' stores the path to the file location, which will usually be AWS S3-compatible
#' storage although other types of storage can be used.
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
#' @param crs_fallback The coordinate reference system of the raster file in a
#'   form that can be understood by the \code{sf::st_crs()} function (e.g. a WKT
#'   string or an integer EPSG code. This is used to infer the name of the
#'   relevant meta-data and raster tables in the database. If \code{NULL}
#'   (default), the function will attempt to retrieve the CRS from the raster
#'   file.
#'
#' @param tile_id Integer ID for the corresponding existing record in the
#'   'metadata' table. If \code{NULL} (default), the function will search for a
#'   meta-data record based on the file name of the input raster.
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
#' @export
#'
ldb_load_vegheight_raster <- function(db,
                                      raster_url,
                                      crs_fallback = NULL,
                                      tile_id = NULL,
                                      protocol = "vsicurl",
                                      tilew = 256,
                                      R2P = "C:/Program Files/PostgreSQL/12/bin/raster2pgsql.exe") {

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
  rcrs <- sf::st_crs(r)

  # Check CRS - sometimes reading via /vsicurl seems to lose
  # the the CRS of the raster
  #
  if (is.na(sf::st_crs(r))) {
    if (!is.null(crs_fallback)) {
      rcrs <- sf::st_crs(crs_fallback)
      terra::crs(r) <- sf::st_as_text(rcrs)
    } else {
      stop("Raster has no CRS defined and no crs_fallback value was provided")
    }
  }

  if (rcrs$IsGeographic) {
    msg <- glue::glue("The raster is in a geographic coordinate system but needs
                       to be in a projected coordinate system supported by the database.")
    stop(msg)
  }

  # Check that this projection is supported by the database and,
  # if so, get the datum and zone.
  EPSG <- rcrs$epsg

  cmd <- glue::glue("select datum, zone
                     from supported_crs
                     where srid = {EPSG};")

  res <- DBI::dbGetQuery(db, cmd)

  if (nrow(res) != 1) {
    msg <- glue::glue("The coordinate reference system (EPSG:{epsgcode})
                       is not (yet) supported by the database.")
    stop(msg)
  }

  ZONE <- res$zone
  DATUM <- res$datum

  # Table name
  TBL_NAME <- glue::glue("vegheight_{tolower(DATUM)}_zone{ZONE}")


  # Check that the table exists
  if (!pg_table_exists(db, TBL_NAME)) {
    msg <- glue::glue("No vegetation height table for {DATUM}, zone {ZONE}")
    stop(msg)
  }

  # Check for meta-data record
  #
  if (!(is.null(tile_id) || is.na(tile_id))) {
    # tile_id was provided: check that it exists in the metadata table
    cmd <- glue::glue("select exists (
                         select 1 from lidar.metadata
                         where tile_id = {tile_id}
                       );")

    ok <-DBI::dbGetQuery(db, cmd)$exists

    if (!ok) {
      msg <- glue::glue("No meta-data record was found with tile_id = {tile_id}.")
      stop(msg)
    }

  } else {
    # tile_id was not provided: try to find it by matching the input
    # raster file name
    fname <- fs::path_file(vsi_url) %>%
      fs::path_ext_remove(.)

    cmd <- glue::glue("select tile_id from lidar.metadata
                       where filename = '{fname}' and
                       datum = '{DATUM}' and
                       zone = {ZONE};")

    res <-DBI::dbGetQuery(db, cmd)

    if (nrow(res) == 0) {
      msg <- glue::glue("No existing record was found that corresponds to this
                         raster's file name and map projection in the meta-data table.")
      stop(msg)

    } else if (nrow(res) > 1) {
      msg <- glue::glue("More than one meta-data record found that corresponds
                         to this file name, datum and zone.

                         This should never happen! Please check the database.")
      stop(msg)
    }

    tile_id <- res$tile_id
  }

  # Check for an existing raster record
  cmd <- glue::glue("select exists(
                      select 1 from {TBL_NAME}
                      where tile_id = {tile_id}
                    )")

  res <- DBI::dbGetQuery(db, cmd)
  if (res$exists) {
    msg <- glue::glue("There is an existing raster record in the raster table
                       {TBL_NAME} with a matching tile_id value ({tile_id}).
                       Please check before proceeding further.")
    stop(msg)
  }

  x <- rlang::hash(Sys.time())
  TEMP_LOAD_TABLE <- glue::glue("temp_load_{x}")

  # Use the command line raster2pgsql program to create the SQL
  # for the raster import
  fsql <- tempfile(pattern = "sql", fileext = ".txt")

  args <- glue::glue("-s {EPSG} -t {tilew}x{tilew} \\
                     -I -R {vsi_url} \\
                     lidar.{TEMP_LOAD_TABLE}")

  system2(command = R2P, args = args, stdout = fsql)

  # Run the generated SQL to load the raster to the temporary load table
  queries <- readLines(fsql)
  sapply(queries, function(q) DBI::dbExecute(db, q))

  # Copy the raster from the temp_load table to the veg height
  # table for this map zone

  cmd <- glue::glue("
    insert into lidar.{TBL_NAME} (tile_id, rast)
    select {tile_id} as tile_id,
    rast from lidar.{TEMP_LOAD_TABLE};")

  res <- DBI::dbExecute(db, cmd)

  # Drop the temp load table
  cmd <- glue::glue("drop table lidar.{TEMP_LOAD_TABLE};")
  DBI::dbExecute(db, cmd)

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
#' This is used as a helper function by other package functions.
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
