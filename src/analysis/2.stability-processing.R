library(dadmtools)
source('src/utils/functions.R')

conn_list <- dadmtools::get_pg_conn_list()
db <- DBI::dbConnect(conn_list["driver"][[1]],
				host = conn_list["host"][[1]],
				user = conn_list["user"][[1]],
				dbname = conn_list["dbname"][[1]],
				password = conn_list["password"][[1]],
				port = conn_list["port"][[1]])
start_time <- Sys.time()
print(glue("Script started at {format(start_time, '%Y-%m-%d %I:%M:%S %p')}"))

query <- "DROP TABLE IF EXISTS thlb_proxy.inoperable_gr_skey"
run_sql_r(query, conn_list)
query <- "DROP TABLE IF EXISTS thlb_proxy.inoperable_thresholds"
run_sql_r(query, conn_list)


## as TSA 04 is too large, a new set of boundaries were created
query <- "DROP TABLE IF EXISTS thlb_proxy.tsa_boundaries_2020_inoperable"
run_sql_r(query, conn_list)
query <- "CREATE TABLE thlb_proxy.tsa_boundaries_2020_inoperable AS 
SELECT tsa_number, geom FROM thlb_proxy.tsa_boundaries_2020 WHERE tsa_number != '04'
UNION ALL
(WITH pts AS (
	SELECT
		(ST_Dump(ST_GeneratePoints(geom, 2000))).geom AS geom
	from
		thlb_proxy.tsa_boundaries_2020
	where 
		tsa_number = '04'
), pts_clustered AS (
	select
		geom, ST_ClusterKMeans(geom, 3) over () AS cluster
	from
		pts
), centers AS (
  SELECT
	cluster, ST_Centroid(ST_collect(geom)) AS geom
  FROM 
		pts_clustered
	GROUP BY 
		cluster
), veronoi_polys AS (
SELECT
	(ST_Dump(ST_VoronoiPolygons(ST_collect(geom)))).geom AS geom
FROM
	centers
)
SELECT
	'04-' || row_number() OVER() as tsa_number,
	ST_Intersection(a.geom, b.geom) AS geom
FROM
	veronoi_polys b
CROSS JOIN
	(SELECT geom FROM thlb_proxy.tsa_boundaries_2020 WHERE tsa_number = '04') a
)"
run_sql_r(query, conn_list)

## Loop over the TSA numbers
query <- "SELECT tsa_number FROM thlb_proxy.tsa_boundaries_2020_inoperable WHERE tsa_number ilike '04%' GROUP BY tsa_number"
tsa_numbers <- sql_to_df(query, conn_list)$tsa_number

for (tsa_number in tsa_numbers) {
	start_time <- Sys.time()
	print(glue("TSA #: {tsa_number}, started at: {format(start_time, '%Y-%m-%d %I:%M:%S %p')}"))
	## define data driven queries for cutblocks, tsa's, stability
	cutblock_query <- glue("SELECT
						distinct on (blk.veg_consolidated_cut_block_id)
						blk.geom
					FROM 
						thlb_proxy.veg_consolidated_cut_blocks_sp blk
					JOIN 
						thlb_proxy.tsa_boundaries_2020_inoperable reg ON ST_Intersects(reg.geom, blk.geom)
					WHERE 
						reg.tsa_number = '{tsa_number}'
					ORDER BY 
						blk.veg_consolidated_cut_block_id;")

	mgmt_unit_query <- glue("SELECT 
							ST_Union(geom) as geom
						FROM 
							thlb_proxy.tsa_boundaries_2020_inoperable 
						WHERE 
							tsa_number = '{tsa_number}'")

	stability_query <- glue("SELECT
						1::int as class2,
						stab.geom as geom
					FROM 
						thlb_proxy.ste_ter_attribute_polys_svw_ar stab
					JOIN 
						thlb_proxy.tsa_boundaries_2020_inoperable reg ON ST_Intersects(reg.geom, stab.geom)
					WHERE 
						reg.tsa_number = '{tsa_number}';")
	
	message('Convert queries to SpatVector')

	cutblock_vect  <- create_sampler(db, cutblock_query)
	mgmt_unit_vect <- create_sampler(db, mgmt_unit_query)
	stability_vect <- create_sampler(db, stability_query)

	message('Clip 25 meter resolution DEM rasters to mgmt_unit_vects')
	dem_clipped          <- get_dem(mgmt_unit_vect)
	message('Extract Thresholds: 99 percentile for elevation')
	inoperable_elevation <- elev_inop(dem_clipped, cutblock_vect, mgmt_unit_vect, tsa_number, conn_list)
	# writeRaster(inoperable_elevation, "data\\analysis\\inoperable_elevation.tif", overwrite=TRUE)

	message('Rasterize stability vector to 25m DEM to mgmt_unit_vects')
	stability_clipped <- terra::rasterize(stability_vect, background=0, rast(dem_clipped), 'class2')
	# writeRaster(stability_clipped, "data\\analysis\\stability_clipped.tif", overwrite=TRUE)
	rm(dem_clipped)

 	message('Clip 25 meter resolution slope rasters to mgmt_unit_vects')
	slope_clipped     <- get_slope(mgmt_unit_vect)
	message('Extract Thresholds: 99 percentile for slope')
	inoperable_slope  <- slp_inop(slope_clipped, cutblock_vect, mgmt_unit_vect, tsa_number, conn_list)
	# writeRaster(inoperable_slope, "data\\analysis\\inoperable_slope.tif", overwrite=TRUE)

	rm(slope_clipped)

	message('Read in 100 meter resolution template raster')
    template_tif <- rast('S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif')
	template_100m_cropped <- terra::crop(template_tif, mgmt_unit_vect)
	template_100m_cropped <- terra::mask(template_100m_cropped, mgmt_unit_vect)
	# writeRaster(template_100m_cropped, "data\\analysis\\template_100m_cropped.tif", overwrite=TRUE)
	rm(template_tif)

	# add_3_together <- inoperable_elevation + inoperable_slope + stability_clipped
	# writeRaster(inoperable_elevation, "data\\analysis\\inoperable_elevation.tif", overwrite=TRUE)
	# writeRaster(inoperable_slope, "data\\analysis\\inoperable_slope.tif", overwrite=TRUE)
	# writeRaster(stability_clipped, "data\\analysis\\stability_clipped.tif", overwrite=TRUE)
	message('Aggregating slope, stability and elev')
	phy_ops_df <- aggregate(stability_clipped, inoperable_elevation, inoperable_slope, template_100m_cropped)
	message("Writing to output of aggregate to PG")
	df_to_pg(Id(schema = 'thlb_proxy', table = glue('inoperable_gr_skey')), phy_ops_df, conn_list, overwrite=FALSE, append=TRUE)
	end_time <- Sys.time()
	duration <- difftime(end_time, start_time, units = "mins")
	print(glue("TSA #: {tsa_number}, took: {duration} minutes\n"))
}



