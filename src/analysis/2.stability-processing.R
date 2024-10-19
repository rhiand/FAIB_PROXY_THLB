# library(parallel)
# library(doParallel)
# library(foreach)
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
dst_schema <- "thlb_proxy"

query <- glue("DROP TABLE IF EXISTS {dst_schema}.inoperable_gr_skey")
run_sql_r(query, conn_list)
query <- glue("DROP TABLE IF EXISTS {dst_schema}.inoperable_thresholds")
run_sql_r(query, conn_list)
query <- glue("DROP TABLE IF EXISTS {dst_schema}.inoperable_cutblock_summary")
run_sql_r(query, conn_list)

## as TSA 04 is too large, a new set of boundaries were created
query <- glue("DROP TABLE IF EXISTS {dst_schema}.inoperable_all_mgmt_units")
run_sql_r(query, conn_list)
query <- glue("CREATE TABLE {dst_schema}.inoperable_all_mgmt_units AS 
SELECT 
	'TSA' || tsa_number as mgmt_unit_name
	, ST_Union(geom) as geom
FROM 
	{dst_schema}.tsa_boundaries_2020 
WHERE 
	tsa_number != '04'
GROUP BY 
	tsa_number
UNION ALL
(WITH pts AS (
	SELECT
		(ST_Dump(ST_GeneratePoints(geom, 2000))).geom AS geom
	from
		{dst_schema}.tsa_boundaries_2020
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
	'TSA04-' || row_number() OVER() as mgmt_unit_name
	, ST_Intersection(a.geom, b.geom) AS geom
FROM
	veronoi_polys b
CROSS JOIN
	(SELECT geom FROM {dst_schema}.tsa_boundaries_2020 WHERE tsa_number = '04') a
)
UNION ALL
SELECT
	forest_file_id as mgmt_unit_name
	,ST_Union(geom) as geom
FROM
	{dst_schema}.fadm_tfl_all_sp
GROUP BY
	forest_file_id
UNION ALL
SELECT
	CASE 
	WHEN LOWER(forest_file_id) LIKE 'n%%'
		THEN 'FNWL - ' || forest_file_id
	WHEN LOWER(forest_file_id) LIKE 'k%%'
		THEN 'Community Forest - ' ||  forest_file_id
	WHEN LOWER(forest_file_id) LIKE 'w%%'
		 THEN 'Woodlot - ' || forest_file_id
	END AS mgmt_unit_name
	, ST_Union(geom) as geom
FROM
	{dst_schema}.ften_managed_licence_poly_svw
GROUP BY
	forest_file_id")
run_sql_r(query, conn_list)

query_escaped <- gsub("\'","\'\'", query)
todays_date <- format(Sys.time(), "%Y-%m-%d %I:%M:%S %p")
query <- glue("COMMENT ON TABLE {dst_schema}.inoperable_all_mgmt_units IS 'Table created at {todays_date}.
Data source query:
{query_escaped}'")
run_sql_r(query, conn_list)
srid_query <- glue('ALTER TABLE {dst_schema}.inoperable_all_mgmt_units ALTER COLUMN geom TYPE geometry(MultiPolygon, 3005) USING ST_SetSRID(geom, 3005)')
run_sql_r(srid_query, conn_list)
## Loop over the management polygons
query <- glue("SELECT mgmt_unit_name FROM {dst_schema}.inoperable_all_mgmt_units")## where mgmt_unit_name in ('Community Forest - K1E', 'Woodlot - W1208')")
mgmt_units <- sql_to_df(query, conn_list)$mgmt_unit_name


# Register the parallel backend
# num_cores <- detectCores() - 1 # Use one less than the total number of cores
# num_cores <- 4
# cl <- makeCluster(num_cores)
# registerDoParallel(cl)

# Use `foreach` for parallel processing
# foreach(mgmt_unit = mgmt_units, .packages = c('glue', 'terra', 'DBI', 'dadmtools')) %dopar% {

for (mgmt_unit in mgmt_units) {
	it_start_time <- Sys.time()
	print(glue("MGMT UNIT: {mgmt_unit}, started at: {format(start_time, '%Y-%m-%d %I:%M:%S %p')}"))
	## define data driven queries for cutblocks, tsa's, stability
	cutblock_query <- glue("SELECT
						distinct on (blk.veg_consolidated_cut_block_id)
						blk.geom
					FROM 
						{dst_schema}.veg_consolidated_cut_blocks_sp blk
					JOIN 
						{dst_schema}.inoperable_all_mgmt_units reg ON ST_Intersects(reg.geom, blk.geom)
					WHERE 
						reg.mgmt_unit_name = '{mgmt_unit}'
					AND
						harvest_year >= (extract(year from now()) - 10)
					ORDER BY 
						blk.veg_consolidated_cut_block_id;")

	mgmt_unit_query <- glue("SELECT 
							geom as geom
						FROM 
							{dst_schema}.inoperable_all_mgmt_units 
						WHERE 
							mgmt_unit_name = '{mgmt_unit}'")

	stability_query <- glue("SELECT
						1::int as class2,
						stab.geom as geom
					FROM 
						{dst_schema}.ste_ter_attribute_polys_svw_ar stab
					JOIN 
						{dst_schema}.inoperable_all_mgmt_units reg ON ST_Intersects(reg.geom, stab.geom)
					WHERE 
						reg.mgmt_unit_name = '{mgmt_unit}';")
	
	message('Convert queries to SpatVector')

	cutblock_vect  <- create_sampler(db, cutblock_query)
	mgmt_unit_vect <- create_sampler(db, mgmt_unit_query)
	stability_vect <- create_sampler(db, stability_query)

	
	if (length(cutblock_vect) < 1) {
		cutblock_summary_df <- data.frame(
			mgmt_unit = mgmt_unit,
			number_of_cutblocks = length(cutblock_vect),
			cutblock_area_m2 = 0,
			mgmt_area_m2 = sum(terra::expanse(mgmt_unit_vect, unit = "m")),
			created_at = Sys.time()
		)
		df_to_pg(Id(schema = glue('{dst_schema}'), table = glue('inoperable_cutblock_summary')), cutblock_summary_df, conn_list, overwrite=FALSE, append=TRUE)
		print(glue('Skipping iteration as management unit: {mgmt_unit} has no cutblocks'))
		next
	} else {
		cutblock_summary_df <- data.frame(
			mgmt_unit = mgmt_unit,
			number_of_cutblocks = length(cutblock_vect),
			cutblock_area_m2 = sum(terra::expanse(cutblock_vect, unit = "m")),
			mgmt_area_m2 = sum(terra::expanse(mgmt_unit_vect, unit = "m")),
			created_at = Sys.time()
		)
		df_to_pg(Id(schema = glue('{dst_schema}'), table = glue('inoperable_cutblock_summary')), cutblock_summary_df, conn_list, overwrite=FALSE, append=TRUE)
	}

	message('Clip 25 meter resolution DEM rasters to mgmt_unit_vects')
	dem_clipped          <- get_dem(mgmt_unit_vect)
	message('Extract Thresholds: 99 percentile for elevation')
	inoperable_elevation <- elev_inop(dem_clipped, cutblock_vect, mgmt_unit_vect, mgmt_unit, conn_list)
	# writeRaster(inoperable_elevation, "data\\analysis\\inoperable_elevation.tif", overwrite=TRUE)

	message('Rasterize stability vector to 25m DEM to mgmt_unit_vects')
	stability_clipped <- terra::rasterize(stability_vect, background=0, rast(dem_clipped), 'class2')
	# writeRaster(stability_clipped, "data\\analysis\\stability_clipped.tif", overwrite=TRUE)
	rm(dem_clipped)

 	message('Clip 25 meter resolution slope rasters to mgmt_unit_vects')
	slope_clipped     <- get_slope(mgmt_unit_vect)
	message('Extract Thresholds: 99 percentile for slope')
	inoperable_slope  <- slp_inop(slope_clipped, cutblock_vect, mgmt_unit_vect, mgmt_unit, conn_list)
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
	df_to_pg(Id(schema = glue('{dst_schema}'), table = glue('inoperable_gr_skey')), phy_ops_df, conn_list, overwrite=FALSE, append=TRUE)
	end_time <- Sys.time()
	duration <- difftime(end_time, it_start_time, units = "mins")
	print(glue("TSA #: {mgmt_unit}, took: {duration} minutes\n"))
}
# Stop the parallel cluster
# stopCluster(cl)
end_time <- Sys.time()
duration <- difftime(end_time, start_time, units = "mins")
print(glue("Script started at {format(start_time, '%Y-%m-%d %I:%M:%S %p')}"))
print(glue("Script ended at {format(end_time, '%Y-%m-%d %I:%M:%S %p')}"))
duration <- difftime(end_time, start_time, units = "mins")
print(glue("Script took: {duration} minutes\n"))


