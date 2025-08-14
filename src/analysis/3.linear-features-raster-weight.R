library(terra)
library(dplyr)
library(dadmtools)
library(DBI)
source('src/utils/functions.R')

pg_conn_param = get_pg_conn_list()

dst_schema <- "whse"
vector_schema <- "whse_vector"
repo_path <- 'C:/projects/FAIB_PROXY_THLB'


## the following query does a few things
## a) Clips the spatial tables (rails, integrated roads etc) to a single 50k grid mapsheet (Ie. NTS 50 which has 1,209 records)
## b) It merges & buffers & then dissolves all the results - the resultant is a dissolved polygon (Ie. single record) clipped to the mapsheet
 
spatial_query <- "WITH buffered_intersects AS (
SELECT
	ST_Buffer(vect.geom, 15) as geom
FROM
	whse_vector.GBA_RAILWAY_TRACKS_SP vect
JOIN
	{grid_tbl} grid
ON
	ST_Intersects(vect.geom, grid.{grid_geom_fld})
WHERE
	grid.{grid_loop_fld} = '{grid_row}'
UNION ALL
SELECT
	ST_Buffer(vect.geom, 25) as geom
FROM
	whse_vector.GBA_TRANSMISSION_LINES_SP vect
JOIN
	{grid_tbl} grid
ON
	ST_Intersects(vect.geom, grid.{grid_geom_fld})
WHERE
	grid.{grid_loop_fld} = '{grid_row}'
UNION ALL
SELECT
	ST_Buffer(vect.geom, 15) as geom
FROM
	whse_vector.DRP_OIL_GAS_PIPELINES_BC_SP vect
JOIN
	{grid_tbl} grid
ON
	ST_Intersects(vect.geom, grid.{grid_geom_fld})
WHERE
	grid.{grid_loop_fld} = '{grid_row}'
UNION ALL
SELECT
	ST_Buffer(vect.geom, 15) as geom
FROM
	whse_vector.OG_PIPELINE_AREA_PERMIT_SP vect
JOIN
	{grid_tbl} grid
ON
	ST_Intersects(vect.geom, grid.{grid_geom_fld})
WHERE
	grid.{grid_loop_fld} = '{grid_row}'
UNION ALL
SELECT
	vect.geom as geom -- no buffer needed, already buffered
FROM
	whse_vector.ta_crown_rights_of_way_svw vect
JOIN
	{grid_tbl} grid
ON
	ST_Intersects(vect.geom, grid.{grid_geom_fld})
WHERE
	grid.{grid_loop_fld} = '{grid_row}'
UNION ALL
SELECT
	vect.geom as geom -- no buffer needed, already buffered
FROM
	whse_vector.integratedroadsbuffers vect
JOIN
	{grid_tbl} grid
ON
	ST_Intersects(vect.geom, grid.{grid_geom_fld})
WHERE
	grid.{grid_loop_fld} = '{grid_row}'
)
SELECT
	ST_Intersection(ST_Union(vect.geom), grid.{grid_geom_fld}) as geom
FROM
	buffered_intersects vect
JOIN
	{grid_tbl} grid
ON
	ST_Intersects(vect.geom, grid.{grid_geom_fld})
WHERE
	grid.{grid_loop_fld} = '{grid_row}'
GROUP BY 	
	grid.{grid_geom_fld}"

spatial_query_when_error <- "WITH buffered_intersects AS (
SELECT
	ST_Buffer(vect.geom, 15) as geom
FROM
	whse_vector.GBA_RAILWAY_TRACKS_SP vect
JOIN
	{grid_tbl} grid
ON
	ST_Intersects(vect.geom, grid.{grid_geom_fld})
WHERE
	grid.{grid_loop_fld} = '{grid_row}'
UNION ALL
SELECT
	ST_Buffer(vect.geom, 25) as geom
FROM
	whse_vector.GBA_TRANSMISSION_LINES_SP vect
JOIN
	{grid_tbl} grid
ON
	ST_Intersects(vect.geom, grid.{grid_geom_fld})
WHERE
	grid.{grid_loop_fld} = '{grid_row}'
UNION ALL
SELECT
	ST_Buffer(vect.geom, 15) as geom
FROM
	whse_vector.DRP_OIL_GAS_PIPELINES_BC_SP vect
JOIN
	{grid_tbl} grid
ON
	ST_Intersects(vect.geom, grid.{grid_geom_fld})
WHERE
	grid.{grid_loop_fld} = '{grid_row}'
UNION ALL
SELECT
	ST_Buffer(vect.geom, 15) as geom
FROM
	whse_vector.OG_PIPELINE_AREA_PERMIT_SP vect
JOIN
	{grid_tbl} grid
ON
	ST_Intersects(vect.geom, grid.{grid_geom_fld})
WHERE
	grid.{grid_loop_fld} = '{grid_row}'
UNION ALL
SELECT
	vect.geom as geom -- no buffer needed, already buffered
FROM
	whse_vector.ta_crown_rights_of_way_svw vect
JOIN
	{grid_tbl} grid
ON
	ST_Intersects(vect.geom, grid.{grid_geom_fld})
WHERE
	grid.{grid_loop_fld} = '{grid_row}'
UNION ALL
SELECT
	vect.geom as geom -- no buffer needed, already buffered
FROM
	whse_vector.integratedroadsbuffers vect
JOIN
	{grid_tbl} grid
ON
	ST_Intersects(vect.geom, grid.{grid_geom_fld})
WHERE
	grid.{grid_loop_fld} = '{grid_row}'
)
SELECT
	ST_Intersection(ST_Buffer(ST_Union(vect.geom), 0.0001), grid.{grid_geom_fld}) as geom
FROM
	buffered_intersects vect
JOIN
	{grid_tbl} grid
ON
	ST_Intersects(vect.geom, grid.{grid_geom_fld})
WHERE
	grid.{grid_loop_fld} = '{grid_row}'
GROUP BY 	
	grid.{grid_geom_fld}"

tbl_comment <- "COMMENT ON TABLE {dst_schema}.{dst_tbl} IS 'Table created at {today_date}.
Table contains the gr_skey pixel percent coverage by the following layers:
Layer: whse_vector.GBA_RAILWAY_TRACKS_SP
Buffer: 15

Layer: whse_vector.GBA_TRANSMISSION_LINES_SP
Buffer: 25

Layer: whse_vector.DRP_OIL_GAS_PIPELINES_BC_SP
Buffer: 15

Layer: whse_vector.OG_PIPELINE_AREA_PERMIT_SP
Buffer: 15

Layer: whse_vector.integratedroadsbuffers
Buffer: No buffer, layer already buffered in Analysis Ready Dataset

Layer: WHSE_TANTALIS.TA_CROWN_RIGHTS_OF_WAY_SVW'"

linear_weight(template_tif           = glue("{repo_path}\\data\\input\\bc_01ha_gr_skey.tif"),  ## "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif"
			mask_tif                 = glue("{repo_path}\\data\\input\\BC_Boundary_Terrestrial.tif"), ## 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
			crop_extent              = c(273287.5,1870587.5,367787.5,1735787.5),
			grid_tbl                 = "whse_vector.nts_50k_grid",
			grid_loop_fld            = "map_tile",
			grid_geom_fld            = "geom",
			dst_schema               = dst_schema,
			dst_tbl                  = "bc_linear_features",
			pg_conn_param            = pg_conn_param,
			create_vector_lyr        = TRUE,
			spatial_query            = spatial_query,
			spatial_query_when_error = spatial_query_when_error,
			tbl_comment              = tbl_comment)

## Check output
query <- glue("select * from {dst_schema}.bc_linear_features where fact > 1")
review_lin <- sql_to_df(query, conn_list)
## at time of processing, there were 475 cells where fact > 1 and the largest was 1.01963
## That level of error is acceptable - adjust down to 1
if (nrow(review_lin) == 0) {
	print('Success: No records where fact > 1')
} else {
	print(glue('Error: There are records in {dst_schema}.bc_linear_features where fact > 1'))
}
