library(dadmtools)
source('src/utils/functions.R')

conn_list <- dadmtools::get_pg_conn_list()
db <- DBI::dbConnect(conn_list["driver"][[1]],
				host = conn_list["host"][[1]],
				user = conn_list["user"][[1]],
				dbname = conn_list["dbname"][[1]],
				password = conn_list["password"][[1]],
				port = conn_list["port"][[1]])

## Get the provincial 25 meter resolution DEM and create a unit raster. Here we’ll use the get_dem function in the functions.R function repository. This functions requires access to the image warehouse. The path is hard-coded in the get_dem function so you’ll need to map it before running the script. Any changes to drive names need to be made there (the current drive name is "R").
elevation <- get_dem()
slope <- get_slope(elevation)

stab_query <- "SELECT geom, 1::int as class2 FROM whse_sp.ste_ter_attribute_polys_svw_union"
stability <- get_stability(db, stab_query, rast(elevation), 'class2')

blk_query <- "SELECT harvest_year, geom FROM whse_sp.veg_consolidated_cut_blocks_sp WHERE harvest_year > 0;"
sampler <- create_sampler(db, blk_query)

# inoperable_elevation <- elev_inop(elevation,sampler,unit)
blk_elev <- terra::extract(elevation, sampler)
blkelev99 <- quantile(blk_elev$bc_elevation_25m_bcalb, probs = 0.99, na.rm = TRUE) # determine 99th percentile
print("blk 99 percentile is ", blkelev99)




