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
query <- "DROP TABLE IF EXISTS whse.inoperable_gr_skey"
run_sql_r(query, conn_list)

## Loop over the TSA numbers
query <- "SELECT tsa_number FROM thlb_proxy.tsa_boundaries_2020 WHERE tsa_number = '04' GROUP BY tsa_number"
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
						thlb_proxy.tsa_boundaries_2020 reg ON ST_Intersects(reg.geom, blk.geom)
					WHERE 
						reg.tsa_number = '{tsa_number}'
					ORDER BY 
						blk.veg_consolidated_cut_block_id;")

	mgmt_unit_query <- glue("SELECT 
							ST_Union(geom) as geom
						FROM 
							thlb_proxy.tsa_boundaries_2020 
						WHERE 
							tsa_number = '{tsa_number}'")

	stability_query <- glue("SELECT
						1::int as class2,
						stab.geom as geom
					FROM 
						thlb_proxy.ste_ter_attribute_polys_svw_ar stab
					JOIN 
						thlb_proxy.tsa_boundaries_2020 reg ON ST_Intersects(reg.geom, stab.geom)
					WHERE 
						reg.tsa_number = '{tsa_number}';")
	
	message('Convert queries to SpatVector')

	cutblock_vect  <- create_sampler(db, cutblock_query)
	mgmt_unit_vect <- create_sampler(db, mgmt_unit_query)
	stability_vect <- create_sampler(db, stability_query)

	message('Clip 25 meter resolution rasters to mgmt_unit_vects')
	dem_clipped       <- get_dem(mgmt_unit_vect)
	slope_clipped     <- get_slope(mgmt_unit_vect)
	stability_clipped <- terra::rasterize(stability_vect, background=0, rast(dem_clipped), 'class2')
	# stability_mask    <- mask(stability_clipped, stability_vect)

 	message('Extract Thresholds: 99 percentile for slope and elevation')
	inoperable_elevation <- elev_inop(dem_clipped, cutblock_vect, mgmt_unit_vect)

	inoperable_slope     <- slp_inop(slope_clipped, cutblock_vect, mgmt_unit_vect)

	message('Read in 100 meter resolution template raster')
    template_tif <- rast('S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif')
	template_100m_cropped <- terra::crop(template_tif, mgmt_unit_vect)
	template_100m_cropped <- terra::mask(template_100m_cropped, mgmt_unit_vect)

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



