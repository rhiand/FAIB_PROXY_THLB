library(dadmtools)
source('src/utils/functions.R')
conn_list <- dadmtools::get_pg_conn_list()
## Note: the function: import_bcgw_to_pg relies on he oracle foreign server: oradb existing. If working on a fresh db that doesn't have the oracle foreign server set up yet - be sure to import layers using dadmtools first as it will set up the oracle foreign server

dst_schema <- "whse"
vector_schema <- "whse_vector"

## Import 50k grid
## WHSE_BASEMAPPING.BCGS_20K_GRID
import_bcgw_to_pg(src_schema     = "WHSE_BASEMAPPING",
				  src_layer      = "NTS_50K_GRID",
				  fdw_schema     = "load",
				  dst_schema     = vector_schema,
				  dst_layer      = "NTS_50K_GRID",
				  fields_to_keep = "map_tile",
				  geometry_name  = "geometry",
				  geometry_type  = "MultiPolygon",
				  grouping_name  = "map tile",
				  pg_conn_list   = conn_list)

## Rationale: Kootenay Lake data package for roads, rails, trail, tranmission lines etc
## WHSE_BASEMAPPING.GBA_RAILWAY_TRACKS_SP
import_bcgw_to_pg(src_schema     = "WHSE_BASEMAPPING",
				  src_layer      = "GBA_RAILWAY_TRACKS_SP",
				  fdw_schema     = "load",
				  dst_schema     = vector_schema,
				  dst_layer      = "GBA_RAILWAY_TRACKS_SP",
				  fields_to_keep = "railway_track_id",
				  geometry_name  = "shape",
				  geometry_type  = "MultiLineString",
				  grouping_name  = "railway",
				  pg_conn_list   = conn_list)

## Rationale: Kootenay Lake data package for roads, rails, trail, tranmission lines etc
## WHSE_BASEMAPPING.GBA_TRANSMISSION_LINES_SP
import_bcgw_to_pg(src_schema     = "WHSE_BASEMAPPING",
				  src_layer      = "GBA_TRANSMISSION_LINES_SP",
				  fdw_schema     = "load",
				  dst_schema     = vector_schema,
				  dst_layer      = "GBA_TRANSMISSION_LINES_SP",
				  fields_to_keep = "transmission_line_id",
				  geometry_name  = "shape",
				  geometry_type  = "MultiLineString",
				  grouping_name  = "hydro line",
				  pg_conn_list   = conn_list)

## Rationale: Kootenay Lake data package for roads, rails, trail, tranmission lines etc
## WHSE_IMAGERY_AND_BASE_MAPS.DRP_OIL_GAS_PIPELINES_BC_SP
import_bcgw_to_pg(src_schema     = "WHSE_IMAGERY_AND_BASE_MAPS",
				  src_layer      = "DRP_OIL_GAS_PIPELINES_BC_SP",
				  fdw_schema     = "load",
				  dst_schema     = vector_schema,
				  dst_layer      = "DRP_OIL_GAS_PIPELINES_BC_SP",
				  fields_to_keep = "oil_gas_pipeline_bc_id",
				  geometry_name  = "geometry",
				  geometry_type  = "MultiLineString",
				  grouping_name  = "pipeline",
				  pg_conn_list   = conn_list)

## Rationale: Kootenay Lake data package for roads, rails, trail, tranmission lines etc
## WHSE_MINERAL_TENURE.OG_PIPELINE_AREA_PERMIT_SP
import_bcgw_to_pg(src_schema     = "WHSE_MINERAL_TENURE",
				  src_layer      = "OG_PIPELINE_AREA_PERMIT_SP",
				  fdw_schema     = "load",
				  dst_schema     = vector_schema,
				  dst_layer      = "OG_PIPELINE_AREA_PERMIT_SP",
				  fields_to_keep = "og_pipeline_area_permit_id",
				  geometry_name  = "shape",
				  geometry_type  = "MultiPolygon",
				  grouping_name  = "pipeline",
				  pg_conn_list   = conn_list)

## Rationale: Sunshine Coast data package for roads, rails, trail, tranmission lines etc
## WHSE_TANTALIS.TA_CROWN_RIGHTS_OF_WAY_SVW
import_bcgw_to_pg(src_schema     = "WHSE_TANTALIS",
				  src_layer      = "TA_CROWN_RIGHTS_OF_WAY_SVW",
				  fdw_schema     = "load",
				  dst_schema     = vector_schema,
				  dst_layer      = "TA_CROWN_RIGHTS_OF_WAY_SVW",
				  fields_to_keep = "intrid_sid",
				  geometry_name  = "shape",
				  geometry_type  = "MultiPolygon",
				  grouping_name  = "right-of-way",
				  pg_conn_list   = conn_list)

# ## 2021 import
# src_path <- "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\AR2023\\BC_CE_Integrated_Roads_2021_20210805.gdb"
# src_lyr <- "integratedRoadsBuffers"
# ## import the integratedroads buffers using ogr2ogr
# ogr_cmd <- glue('ogr2ogr -overwrite -a_srs EPSG:3005 -nln {src_lyr} -lco SCHEMA={vector_schema} -nlt MULTIPOLYGON -sql "SELECT SHAPE as geom, INTEGRATED_ROADS_ID, DRA_ROAD_CLASS, Integrated_Road_Class_Num, Integrated_Road_Class_Descr, CEF_Road_Buffer_Width_m, BUFF_DIST FROM {src_lyr}" -lco OVERWRITE=YES --config PG_USE_COPY YES -f PostgreSQL PG:dbname={conn_list$dbname} {src_path}')
# system(ogr_cmd)
# today_date <- format(Sys.time(), "%Y-%m-%d %I:%M:%S %p")
# tbl_comment <- glue("COMMENT ON TABLE {vector_schema}.{src_lyr} IS 'Table created at {today_date}.
# Imported from {src_path} | layername = {src_lyr}'")
# run_sql_r(tbl_comment, conn_list)



## 2024 dataset 
src_path <- "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\AR2024\\local_inputs\\BC_CE_Integrated_Roads_2024_with_buffers.gdb"
src_lyr <- "integratedRoadsBuffers"
ogr_cmd <- glue("ogr2ogr -overwrite -a_srs EPSG:3005 -nln {src_lyr} -lco SCHEMA={vector_schema} -nlt MULTIPOLYGON -lco OVERWRITE=YES --config PG_USE_COPY YES -f PostgreSQL PG:dbname={conn_list$dbname} {src_path}")
system(ogr_cmd)
today_date <- format(Sys.time(), "%Y-%m-%d %I:%M:%S %p")
tbl_comment <- glue("COMMENT ON TABLE {vector_schema}.{src_lyr} IS 'Table created at {today_date}.
Imported from {src_path} | layername = {src_lyr}'")
run_sql_r(tbl_comment, conn_list)