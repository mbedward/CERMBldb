-- Specify that we will use out-of-db storage for raster data

ALTER DATABASE cermb_lidar SET postgis.enable_outdb_rasters = true;

ALTER DATABASE cermb_lidar SET postgis.gdal_enabled_drivers TO 'ENABLE_ALL';


-- Create a lidar schema to hold the data tables

CREATE SCHEMA lidar
    AUTHORIZATION postgres;

GRANT ALL ON SCHEMA lidar TO postgres;

GRANT USAGE ON SCHEMA lidar TO readonly_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA lidar
GRANT SELECT ON TABLES TO readonly_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA lidar
GRANT USAGE ON SEQUENCES TO readonly_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA lidar
GRANT EXECUTE ON FUNCTIONS TO readonly_user;


-- Set the search path for users to have the lidar
-- schema in first position

ALTER DATABASE cermb_lidar SET search_path TO lidar, "$user", public;


-- Create a 'readonly_user' role if one does not already exist
-- on the server
DO
$do$
BEGIN
	IF NOT EXISTS (
		SELECT FROM pg_catalog.pg_roles  -- SELECT list can be empty for this
		WHERE  rolname = 'readonly_user') THEN
			CREATE ROLE readonly_user;
	END IF;
END
$do$;

GRANT CONNECT ON DATABASE cermb_lidar TO readonly_user;

GRANT USAGE ON SCHEMA lidar TO readonly_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA lidar TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA lidar TO readonly_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA lidar TO readonly_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA lidar GRANT SELECT ON TABLES TO readonly_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA lidar GRANT USAGE ON SEQUENCES TO readonly_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA lidar GRANT EXECUTE ON FUNCTIONS TO readonly_user;


-- Example of how to create a standard user
-- CREATE ROLE somebody LOGIN INHERIT PASSWORD 'some_password' IN ROLE readonly_user;

--------------------------------
-- Create data providers table

CREATE TABLE lidar.providers (
	abbrev TEXT PRIMARY KEY,
    name TEXT NOT NULL,
	contact TEXT
);

ALTER TABLE lidar.providers
    OWNER to postgres;

GRANT ALL ON TABLE lidar.providers TO postgres;


------------------------
-- Strata definitions

CREATE TABLE IF NOT EXISTS lidar.strata_defs (
	abbrev text PRIMARY KEY,  -- short unique label, e.g. 'ofh'
	title text NOT NULL,      -- long label, e.g. 'Overall Fuel Hazard strata'
	numbands integer NOT NULL CHECK (numbands > 0)
);

ALTER TABLE lidar.strata_defs
    OWNER to postgres;

GRANT ALL ON TABLE lidar.strata_defs TO postgres;


CREATE TABLE IF NOT EXISTS lidar.stratum (
	strata_def text NOT NULL REFERENCES lidar.strata_defs (abbrev),
	index integer NOT NULL,
	label text NOT NULL,
	lowerht numeric(5, 2) NOT NULL,
	upperht numeric(5, 2) NOT NULL CHECK (upperht > lowerht),

	PRIMARY KEY (strata_def, index)
);

ALTER TABLE lidar.stratum
    OWNER to postgres;

GRANT ALL ON TABLE lidar.stratum TO postgres;

CREATE INDEX idx_stratum_strata_def
	ON lidar.stratum USING btree
	(strata_def);

CREATE INDEX idx_stratum_layer_index
	ON lidar.stratum USING btree
	(strata_def, index);

CREATE INDEX idx_stratum_layer_label
	ON lidar.stratum USING btree
	(strata_def, label);


----------------------------------------------------
-- Table for 1:100,000 topo map key.
-- Note: presently using GDA94 lon-lat coordinates.

CREATE TABLE IF NOT EXISTS lidar.maps100k (
	mapnumber text PRIMARY KEY,
	mapname text NOT NULL UNIQUE,
	zone integer NOT NULL CHECK (zone >= 54 AND zone <= 56),
	geom GEOMETRY(Polygon, 4283) NOT NULL
);

ALTER TABLE lidar.maps100k
    OWNER to postgres;

GRANT ALL ON TABLE lidar.maps100k TO postgres;

CREATE INDEX idx_maps100k_mapname
	ON lidar.maps100k USING btree
	(mapname);


---------------------------------------------------------------
-- LiDAR tile metadata table. Has all tile boundary polygons
-- projected into GDA 2020 / NSW Lambert (EPSG:8058)

CREATE TABLE IF NOT EXISTS lidar.metadata (
	tile_id serial PRIMARY KEY,
	filename text NOT NULL UNIQUE,
	datum text NOT NULL CHECK (datum IN ('GDA94', 'GDA2020')),
	zone integer NOT NULL CHECK (zone IN (54, 55, 56)),
	provider text NOT NULL REFERENCES lidar.providers (abbrev),
	purpose text NOT NULL CHECK (purpose IN ('general', 'special')),
	mapname text NOT NULL REFERENCES lidar.maps100k (mapname),
	area_m2 numeric NOT NULL CHECK (area_m2 > 0),
	capture_year integer NOT NULL CHECK (capture_year >= 2000),
	capture_start timestamp without time zone NOT NULL,
	capture_end timestamp without time zone NOT NULL CHECK (capture_end >= capture_start),
	nflightlines integer NOT NULL CHECK (nflightlines >= 0),
	npts_ground integer NOT NULL CHECK (npts_ground >= 0),
	npts_veg integer NOT NULL CHECK (npts_veg >= 0),
	npts_water integer NOT NULL CHECK (npts_water >= 0),
	npts_other integer NOT NULL CHECK (npts_other >= 0),
	point_density numeric NOT NULL CHECK (point_density > 0),
	geom GEOMETRY(Polygon, 8058) NOT NULL  -- GDA 2020 / NSW Lambert projection
);


ALTER TABLE lidar.metadata
    OWNER to postgres;

GRANT ALL ON TABLE lidar.metadata TO postgres;

CREATE INDEX sidx_metadata
    ON lidar.metadata USING gist
    (geom);

CREATE INDEX idx_metadata_mapname
	ON lidar.metadata USING btree
	(mapname);

CREATE INDEX idx_metadata_year
	ON lidar.metadata USING btree
	(capture_year);


------------------------------------------------------------
------------------------------------------------------------
-- Raster tables for point counts and veg cover

-- Function to return the datum and zone for a given tile_id value.
-- Used for check constraints in the raster tables.

CREATE OR REPLACE FUNCTION lidar.check_datum_zone(
	tile_id integer, datum text, zone integer) RETURNS boolean
	AS $$
		SELECT EXISTS (
			SELECT 1 FROM metadata
			WHERE tile_id = $1 AND datum = $2 AND zone = $3)
	$$
	LANGUAGE SQL;

ALTER FUNCTION lidar.check_datum_zone
	OWNER TO postgres;

GRANT ALL ON FUNCTION lidar.check_datum_zone TO postgres;


-- Function to check that a given strata_def label is
-- consistent with the number of raster bands. Used for
-- check constraint in the point count and veg cover
-- raster tables. If the argument 'is_veg' is false, the
-- number of raster bands must equal the number of strata_def
-- bands; if 'is_veg' is true, the number of bands must be
-- the number of strata_def bands minus one (for the ground
-- layer).

CREATE OR REPLACE FUNCTION check_strata_def(strata_def text, rast raster, is_veg boolean) RETURNS boolean
	AS $$
		SELECT EXISTS (
			SELECT 1 FROM strata_defs
			WHERE abbrev = $1 AND numbands = ST_NumBands(rast) + is_veg::integer)
	$$
	LANGUAGE SQL;

ALTER FUNCTION lidar.check_strata_def
	OWNER TO postgres;

GRANT ALL ON FUNCTION lidar.check_strata_def TO postgres;


------------------------------------------------------------
-- Raster tables for MGA Zone 55 / GDA94 data (EPSG: 28355)

-- Point count rasters (allow differing strata defs)

CREATE TABLE IF NOT EXISTS lidar.pointcounts_gda94_zone55 (
	rid serial PRIMARY KEY,
	tile_id integer NOT NULL REFERENCES lidar.metadata (tile_id),
	strata_def text NOT NULL REFERENCES lidar.strata_defs (abbrev),
	rast raster,

	CONSTRAINT pointcounts_gda94_zone55_datum_zone
		CHECK (check_datum_zone(tile_id, datum => 'GDA94', zone => 55)),

	CONSTRAINT pointcounts_gda94_zone55_strata_def
		CHECK (check_strata_def(strata_def, rast, is_veg => false))
);


ALTER TABLE IF EXISTS lidar.pointcounts_gda94_zone55
    OWNER to postgres;

GRANT ALL ON TABLE lidar.pointcounts_gda94_zone55 TO postgres;

CREATE INDEX IF NOT EXISTS sidx_pointcounts_gda94_zone55
    ON lidar.pointcounts_gda94_zone55 USING gist
    (st_convexhull(rast));


-- Vegetation cover rasters (allow differing strata defs)

CREATE TABLE IF NOT EXISTS lidar.vegcover_gda94_zone55 (
	rid serial PRIMARY KEY,
	tile_id integer NOT NULL REFERENCES lidar.metadata (tile_id),
	strata_def text NOT NULL REFERENCES lidar.strata_defs (abbrev),
	rast raster,

	CONSTRAINT vegcover_gda94_zone55_datum_zone
		CHECK (check_datum_zone(tile_id, datum => 'GDA94', zone => 55)),

	CONSTRAINT vegcover_gda94_zone55_strata_defs
		CHECK (check_strata_defs(strata_def, rast, is_veg => true))
);


ALTER TABLE IF EXISTS lidar.vegcover_gda94_zone55
    OWNER to postgres;

GRANT ALL ON TABLE lidar.vegcover_gda94_zone55 TO postgres;

CREATE INDEX IF NOT EXISTS sidx_vegcover_gda94_zone55
    ON lidar.vegcover_gda94_zone55 USING gist
    (st_convexhull(rast));


-- Vegetation height rasters

CREATE TABLE IF NOT EXISTS lidar.vegheight_gda94_zone55 (
	rid serial PRIMARY KEY,
	tile_id integer NOT NULL REFERENCES lidar.metadata (tile_id),
	rast raster,

	CONSTRAINT vegheight_gda94_zone55_datum_zone
		CHECK (check_datum_zone(tile_id, datum => 'GDA94', zone => 55))
);

ALTER TABLE IF EXISTS lidar.vegheight_gda94_zone55
    OWNER to postgres;

GRANT ALL ON TABLE lidar.vegheight_gda94_zone55 TO postgres;

CREATE INDEX IF NOT EXISTS sidx_vegheight_gda94_zone55
    ON lidar.vegheight_gda94_zone55 USING gist
    (st_convexhull(rast));


------------------------------------------------------------
-- Raster tables for MGA Zone 56 / GDA94 data (EPSG: 28356)

-- Point count rasters (allow differing strata defs)

CREATE TABLE IF NOT EXISTS lidar.pointcounts_gda94_zone56 (
	rid serial PRIMARY KEY,
	tile_id integer NOT NULL REFERENCES lidar.metadata (tile_id),
	strata_def text NOT NULL REFERENCES lidar.strata_defs (abbrev),
	rast raster,

	CONSTRAINT pointcounts_gda94_zone56_datum_zone
		CHECK (check_datum_zone(tile_id, datum => 'GDA94', zone => 56)),

	CONSTRAINT pointcounts_gda94_zone56_strata_def
		CHECK (check_strata_def(strata_def, rast, is_veg => false))
);

ALTER TABLE IF EXISTS lidar.pointcounts_gda94_zone56
    OWNER to postgres;

GRANT ALL ON TABLE lidar.pointcounts_gda94_zone56 TO postgres;

CREATE INDEX IF NOT EXISTS sidx_pointcounts_gda94_zone56
    ON lidar.pointcounts_gda94_zone56 USING gist
    (st_convexhull(rast));


-- Vegetation cover rasters (allow differing strata defs)

CREATE TABLE IF NOT EXISTS lidar.vegcover_gda94_zone56 (
	rid serial PRIMARY KEY,
	tile_id integer NOT NULL REFERENCES lidar.metadata (tile_id),
	strata_def text NOT NULL REFERENCES lidar.strata_defs (abbrev),
	rast raster,

	CONSTRAINT vegcover_gda94_zone56_datum_zone
		CHECK (check_datum_zone(tile_id, datum => 'GDA94', zone => 56)),

	CONSTRAINT vegcover_gda94_zone56_strata_defs
		CHECK (check_strata_defs(strata_def, rast, is_veg => true))
);

ALTER TABLE IF EXISTS lidar.vegcover_gda94_zone56
    OWNER to postgres;

GRANT ALL ON TABLE lidar.vegcover_gda94_zone56 TO postgres;

CREATE INDEX IF NOT EXISTS sidx_vegcover_gda94_zone56
    ON lidar.vegcover_gda94_zone56 USING gist
    (st_convexhull(rast));


-- Vegetation height rasters

CREATE TABLE IF NOT EXISTS lidar.vegheight_gda94_zone56 (
	rid serial PRIMARY KEY,
	tile_id integer NOT NULL REFERENCES lidar.metadata (tile_id),
	rast raster,

	CONSTRAINT vegheight_gda94_zone56_datum_zone
		CHECK (check_datum_zone(tile_id, datum => 'GDA94', zone => 56))
);

ALTER TABLE IF EXISTS lidar.vegheight_gda94_zone56
    OWNER to postgres;

GRANT ALL ON TABLE lidar.vegheight_gda94_zone56 TO postgres;

CREATE INDEX IF NOT EXISTS sidx_vegheight_gda94_zone56
    ON lidar.vegheight_gda94_zone56 USING gist
    (st_convexhull(rast));



-------------------------------------------------------------
-- Raster tables for MGA Zone 55 / GDA2020 data (EPSG: 7855)

-- Point count rasters (allow differing strata defs)

CREATE TABLE IF NOT EXISTS lidar.pointcounts_gda2020_zone55 (
	rid serial PRIMARY KEY,
	meta_id integer NOT NULL REFERENCES lidar.metadata_gda2020_zone55 (id),
	strata_def text NOT NULL REFERENCES lidar.strata_defs (abbrev),
	rast raster,

	CONSTRAINT pointcounts_gda2020_zone55_datum_zone
		CHECK (check_datum_zone(tile_id, datum => 'GDA2020', zone => 55)),

	CONSTRAINT pointcounts_gda2020_zone55_strata_def
		CHECK (check_strata_def(strata_def, rast, is_veg => false))
);

ALTER TABLE IF EXISTS lidar.pointcounts_gda2020_zone55
    OWNER to postgres;

GRANT ALL ON TABLE lidar.pointcounts_gda2020_zone55 TO postgres;

CREATE INDEX IF NOT EXISTS sidx_pointcounts_gda2020_zone55
    ON lidar.pointcounts_gda2020_zone55 USING gist
    (st_convexhull(rast));


-- Vegetation cover rasters (allow differing strata defs)

CREATE TABLE IF NOT EXISTS lidar.vegcover_gda2020_zone55 (
	rid serial PRIMARY KEY,
	tile_id integer NOT NULL REFERENCES lidar.metadata (tile_id),
	strata_def text NOT NULL REFERENCES lidar.strata_defs (abbrev),
	rast raster,

	CONSTRAINT vegcover_gda2020_zone55_datum_zone
		CHECK (check_datum_zone(tile_id, datum => 'GDA2020', zone => 55)),

	CONSTRAINT vegcover_gda2020_zone55_strata_def
		CHECK (check_strata_def(strata_def, rast, is_veg => true))
);

ALTER TABLE IF EXISTS lidar.vegcover_gda2020_zone55
    OWNER to postgres;

GRANT ALL ON TABLE lidar.vegcover_gda2020_zone55 TO postgres;

CREATE INDEX IF NOT EXISTS sidx_vegcover_gda2020_zone55
    ON lidar.vegcover_gda2020_zone55 USING gist
    (st_convexhull(rast));


-- Vegetation height rasters

CREATE TABLE IF NOT EXISTS lidar.vegheight_gda2020_zone55 (
	rid serial PRIMARY KEY,
	tile_id integer NOT NULL REFERENCES lidar.metadata (tile_id),
	rast raster,

	CONSTRAINT vegheight_gda2020_zone55_datum_zone
		CHECK (check_datum_zone(tile_id, datum => 'GDA2020', zone => 55))
);

ALTER TABLE IF EXISTS lidar.vegheight_gda2020_zone55
    OWNER to postgres;

GRANT ALL ON TABLE lidar.vegheight_gda2020_zone55 TO postgres;

CREATE INDEX IF NOT EXISTS sidx_vegheight_gda2020_zone55
    ON lidar.vegheight_gda2020_zone55 USING gist
    (st_convexhull(rast));


-----------------------------------------------------
-- Tables for MGA Zone 56 / GDA2020 data (EPSG: 7856)

-- Point count rasters (allow differing strata defs)

CREATE TABLE IF NOT EXISTS lidar.pointcounts_gda2020_zone56 (
	rid serial PRIMARY KEY,
	meta_id integer NOT NULL REFERENCES lidar.metadata_gda2020_zone56 (id),
	strata_def text NOT NULL REFERENCES lidar.strata_defs (abbrev),
	rast raster,

	CONSTRAINT pointcounts_gda2020_zone56_datum_zone
		CHECK (check_datum_zone(tile_id, datum => 'GDA2020', zone => 56)),

	CONSTRAINT pointcounts_gda2020_zone56_strata_def
		CHECK (check_strata_def(strata_def, rast, is_veg => false))
);

ALTER TABLE IF EXISTS lidar.pointcounts_gda2020_zone56
    OWNER to postgres;

GRANT ALL ON TABLE lidar.pointcounts_gda2020_zone56 TO postgres;

CREATE INDEX IF NOT EXISTS sidx_pointcounts_gda2020_zone56
    ON lidar.pointcounts_gda2020_zone56 USING gist
    (st_convexhull(rast));


-- Vegetation cover rasters (allow differing strata defs)

CREATE TABLE IF NOT EXISTS lidar.vegcover_gda2020_zone56 (
	rid serial PRIMARY KEY,
	tile_id integer NOT NULL REFERENCES lidar.metadata (tile_id),
	strata_def text NOT NULL REFERENCES lidar.strata_defs (abbrev),
	rast raster,

	CONSTRAINT vegcover_gda2020_zone56_datum_zone
		CHECK (check_datum_zone(tile_id, datum => 'GDA2020', zone => 56)),

	CONSTRAINT vegcover_gda2020_zone56_strata_def
		CHECK (check_strata_def(strata_def, rast, is_veg => true))
);

ALTER TABLE IF EXISTS lidar.vegcover_gda2020_zone56
    OWNER to postgres;

GRANT ALL ON TABLE lidar.vegcover_gda2020_zone56 TO postgres;

CREATE INDEX IF NOT EXISTS sidx_vegcover_gda2020_zone56
    ON lidar.vegcover_gda2020_zone56 USING gist
    (st_convexhull(rast));


-- Vegetation height rasters

CREATE TABLE IF NOT EXISTS lidar.vegheight_gda2020_zone56 (
	rid serial PRIMARY KEY,
	tile_id integer NOT NULL REFERENCES lidar.metadata (tile_id),
	rast raster,

	CONSTRAINT vegheight_gda2020_zone56_datum_zone
		CHECK (check_datum_zone(tile_id, datum => 'GDA2020', zone => 56))
);

ALTER TABLE IF EXISTS lidar.vegheight_gda2020_zone56
    OWNER to postgres;

GRANT ALL ON TABLE lidar.vegheight_gda2020_zone56 TO postgres;

CREATE INDEX IF NOT EXISTS sidx_vegheight_gda2020_zone56
    ON lidar.vegheight_gda2020_zone56 USING gist
    (st_convexhull(rast));


-- Summary of coordinate reference systems supported by the database.
--   TODO: convert this to a dynamic query rather than using hard-coded
--   data.
DROP TABLE IF EXISTS lidar.supported_crs;

CREATE TABLE lidar.supported_crs AS
SELECT * FROM (
	VALUES
	('GDA94', 55, 28355),
	('GDA94', 56, 28356),
	('GDA2020', 55, 7855),
	('GDA2020', 56, 7856)
) as t (datum, zone, srid);

ALTER TABLE lidar.supported_crs
    OWNER TO postgres;

GRANT ALL ON TABLE lidar.supported_crs TO postgres;


-- View of lidar coverage, times and point density per 100k map sheet / datum

CREATE OR REPLACE VIEW lidar.coverage AS
	SELECT mapname,
		zone,
		datum,
		capture_year,
		count(*) AS ntiles,
		min(point_density) AS point_density_min,
		percentile_cont(0.5) WITHIN GROUP (ORDER BY (point_density)) AS point_density_median,
		max(point_density) AS point_density_max
	FROM metadata
	GROUP BY mapname, zone, datum, capture_year
	ORDER BY mapname, zone, datum, capture_year;

ALTER TABLE lidar.coverage
    OWNER TO postgres;

GRANT ALL ON TABLE lidar.coverage TO postgres;


-----------------------------------------------------------------------
-- Various utility functions

-- Function to calculate the interval, in whole months, between
-- two date-times. By default, the absolute number of months is
-- returned. Alternatively, if abs_diff => false then negative
-- values will be returned if start_datetime is after end_datetime.
--
-- This is useful to identify clusters of lidar tiles that are close
-- spatially and temporally.
--
-- Adapted from:
-- http://www.lexykassan.com/coding-conundrums/calculating-months-between-dates-in-postgresql/

CREATE OR REPLACE FUNCTION lidar.month_diff(
	start_datetime timestamp without time zone,
	end_datetime timestamp without time zone,
    abs_diff boolean = true)
	returns numeric
	as $$
		WITH t AS (
			SELECT (
				EXTRACT(year FROM AGE(end_datetime, start_datetime))*12 +
				EXTRACT(month FROM AGE(end_datetime, start_datetime))
			)::numeric AS months
		)
		SELECT CASE
			WHEN abs_diff THEN ABS(months)
			ELSE months
		END
		FROM t;
	$$
	language sql;


-- Function to parse a filename that is in the standard from used by Spatial Services NSW
-- into its parts: name, year, month, easting km, northing km, zone.
--
-- Example usage:
-- select filename, (parse_filename(filename)).*
-- from metadata
-- limit 5;
--

CREATE OR REPLACE FUNCTION lidar.parse_filename(filename text)
	RETURNS table(name text, year integer, month integer, east_km integer, north_km integer, zone integer) AS
$$
	WITH parts AS (
		SELECT
			regexp_match(filename,
						 '^([A-Za-z]+)(\d{4})(\d{2}).+AHD_(\d{3})(\d{4})_(\d{2})') AS p
	)
	SELECT
		p[1] AS name,
		(p[2])::integer AS year,
		(p[3])::integer AS month,
		(p[4])::integer AS east_km,
		(p[5])::integer AS north_km,
		(p[6])::integer AS zone
	FROM parts;
$$
LANGUAGE sql;


-- Function to count the number of records in each of the
-- pointcounts* and vegcover* raster tables in the lidar schema.
--
-- Example usage:
-- select * from count_rastertable_records()

CREATE OR REPLACE FUNCTION lidar.count_rastertable_records()
  RETURNS table(tblname text, nrecs bigint) as
$$
DECLARE
	tblname text;
BEGIN
	FOR tblname IN
		SELECT table_name FROM information_schema.tables tt
		WHERE tt.table_schema = 'lidar' AND tt.table_name ~* '^(pointcounts|vegcover)'
		ORDER BY tt.table_name
	LOOP
		RETURN QUERY EXECUTE format('select cast(%L as text), count(*)
									from lidar.%I',
									tblname, tblname);
	END LOOP;
END
$$ LANGUAGE plpgsql;


-- Function to get table name and tile_id from the combined
-- pointcounts* raster tables.
--
-- Example usage:
-- select * from get_pointcounts_tile_id()

CREATE OR REPLACE FUNCTION lidar.get_pointcounts_tile_id()
  RETURNS table(tblname text, tile_id integer) as
$$
DECLARE
	tblname text;
BEGIN
	FOR tblname IN
		SELECT table_name FROM information_schema.tables tt
		WHERE tt.table_schema = 'lidar' AND tt.table_name ~* '^pointcounts'
		ORDER BY tt.table_name
	LOOP
		RETURN QUERY EXECUTE format('select cast(%L as text), tile_id
									from lidar.%I',
									tblname, tblname);
	END LOOP;
END
$$ LANGUAGE plpgsql;


-- Function to get tile_id from the combined vegcover* raster tables.
--
-- Example usage:
-- select get_vegcover_tile_id()

CREATE OR REPLACE FUNCTION lidar.get_vegcover_tile_id()
  RETURNS table(tblname text, tile_id integer) as
$$
DECLARE
	tblname text;
BEGIN
	FOR tblname IN
		SELECT table_name FROM information_schema.tables tt
		WHERE tt.table_schema = 'lidar' AND tt.table_name ~* '^vegcover'
		ORDER BY tt.table_name
	LOOP
		RETURN QUERY EXECUTE format('select cast(%L as text), tile_id
									from lidar.%I',
									tblname, tblname);
	END LOOP;
END
$$ LANGUAGE plpgsql;


-- Function to get the S3 storage path to a raster file of
-- point counts or Specht veg cover corresponding to a given tile_id.
-- Example usage:
-- select get_s3path(33822, 'p')  -- for point counts raster
-- select get_s3path(33822, 'v')  -- for Specht veg cover raster
--
CREATE OR REPLACE FUNCTION lidar.get_raster_s3path(tid int, filetype text) RETURNS text
AS $$
DECLARE
	tblprefix text;
	tblname text;
	s3path text;
BEGIN
  IF LOWER(filetype) = 'p' THEN
		tblprefix := 'pointcounts_';
	ELSIF LOWER(filetype) = 'v' THEN
		tblprefix := 'vegcover_';
	ELSIF LOWER(filetype) = 'h' THEN
		tblprefix := 'vegheight_';
	ELSE
		RAISE EXCEPTION 'The filetype argument must be one of p (point counts), v (veg cover) or h (veg height)';
	END IF;

	SELECT tblprefix || LOWER(datum) || '_zone' || zone::text
	INTO tblname
	FROM lidar.metadata
	WHERE tile_id = tid;

	IF NOT found THEN
		return NULL;
	ELSE
		EXECUTE format('SELECT (st_bandmetadata(rast)).path
						FROM %I
						WHERE tile_id = $1;',
						tblname)
		INTO s3path
		USING tid;

    	RETURN s3path;
	END IF;
END
$$ LANGUAGE plpgsql;


-- Function to send a notification to QGIS when the metadata table is updated
-- so that layers being displayed in QGIS can auto-refresh.
--
CREATE FUNCTION lidar.notify_qgis() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ 
        BEGIN NOTIFY qgis;
        RETURN NULL;
        END; 
    $$;

ALTER FUNCTION lidar.notify_qgis()
    OWNER TO postgres;

COMMENT ON FUNCTION lidar.notify_qgis()
    IS 'Send a notification to QGIS when the metadata table is updated';

-- Attach a trigger to the metadata table to use the above function
CREATE TRIGGER notify_qgis_edit 
  AFTER INSERT OR UPDATE OR DELETE ON lidar.metadata 
    FOR EACH STATEMENT EXECUTE PROCEDURE lidar.notify_qgis();
