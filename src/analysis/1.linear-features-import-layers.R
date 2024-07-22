library(RPostgres)
library(glue)
library(devtools)
# install_github("bcgov/FAIB_DADMTOOLS")
library(dadmtools)
source('src/utils/functions.R')
## relies on install_github("bcgov/FAIB_DATA_MANAGEMENT") being installed at some point
conn_list <- faibDataManagement::get_pg_conn_list()

## Import 20k grid
## WHSE_BASEMAPPING.BCGS_20K_GRID
import_bcgw_to_pg(src_schema    = "WHSE_BASEMAPPING",
				  src_layer     = "BCGS_20K_GRID",
				  fdw_schema    = "load",
				  dst_schema    = "whse_sp",
				  dst_layer     = "BCGS_20K_GRID",
				  layer_id      = "map_tile",
				  geometry_name = "geometry",
				  geometry_type = "MultiPolygon",
				  grouping_name = "map tile",
				  pg_conn_list  = conn_list)

## Import 50k grid
## WHSE_BASEMAPPING.BCGS_20K_GRID
import_bcgw_to_pg(src_schema    = "WHSE_BASEMAPPING",
				  src_layer     = "NTS_50K_GRID",
				  fdw_schema    = "load",
				  dst_schema    = "whse_sp",
				  dst_layer     = "NTS_50K_GRID",
				  layer_id      = "map_tile",
				  geometry_name = "geometry",
				  geometry_type = "MultiPolygon",
				  grouping_name = "map tile",
				  pg_conn_list  = conn_list)

## Import 250k grid
## WHSE_BASEMAPPING.NTS_250K_GRID
import_bcgw_to_pg(src_schema    = "WHSE_BASEMAPPING",
				  src_layer     = "NTS_250K_GRID",
				  fdw_schema    = "load",
				  dst_schema    = "whse_sp",
				  dst_layer     = "NTS_250K_GRID",
				  layer_id      = "map_tile",
				  geometry_name = "geometry",
				  geometry_type = "MultiPolygon",
				  grouping_name = "map tile",
				  pg_conn_list  = conn_list)

## Rationale: Kootenay Lake data package for roads, rails, trail, tranmission lines etc
## WHSE_BASEMAPPING.GBA_RAILWAY_TRACKS_SP
import_bcgw_to_pg(src_schema    = "WHSE_BASEMAPPING",
				  src_layer     = "GBA_RAILWAY_TRACKS_SP",
				  fdw_schema    = "load",
				  dst_schema    = "whse_sp",
				  dst_layer     = "GBA_RAILWAY_TRACKS_SP",
				  layer_id      = "railway_track_id",
				  geometry_name = "shape",
				  geometry_type = "MultiLineString",
				  grouping_name = "railway",
				  pg_conn_list  = conn_list)

## Rationale: Kootenay Lake data package for roads, rails, trail, tranmission lines etc
## WHSE_BASEMAPPING.GBA_TRANSMISSION_LINES_SP
import_bcgw_to_pg(src_schema    = "WHSE_BASEMAPPING",
				  src_layer     = "GBA_TRANSMISSION_LINES_SP",
				  fdw_schema    = "load",
				  dst_schema    = "whse_sp",
				  dst_layer     = "GBA_TRANSMISSION_LINES_SP",
				  layer_id      = "transmission_line_id",
				  geometry_name = "shape",
				  geometry_type = "MultiLineString",
				  grouping_name = "hydro line",
				  pg_conn_list  = conn_list)

## Rationale: Kootenay Lake data package for roads, rails, trail, tranmission lines etc
## WHSE_IMAGERY_AND_BASE_MAPS.DRP_OIL_GAS_PIPELINES_BC_SP
import_bcgw_to_pg(src_schema    = "WHSE_IMAGERY_AND_BASE_MAPS",
				  src_layer     = "DRP_OIL_GAS_PIPELINES_BC_SP",
				  fdw_schema    = "load",
				  dst_schema    = "whse_sp",
				  dst_layer     = "DRP_OIL_GAS_PIPELINES_BC_SP",
				  layer_id      = "oil_gas_pipeline_bc_id",
				  geometry_name = "geometry",
				  geometry_type = "MultiLineString",
				  grouping_name = "pipeline",
				  pg_conn_list  = conn_list)

## Rationale: Kootenay Lake data package for roads, rails, trail, tranmission lines etc
## WHSE_MINERAL_TENURE.OG_PIPELINE_AREA_PERMIT_SP
import_bcgw_to_pg(src_schema    = "WHSE_MINERAL_TENURE",
				  src_layer     = "OG_PIPELINE_AREA_PERMIT_SP",
				  fdw_schema    = "load",
				  dst_schema    = "whse_sp",
				  dst_layer     = "OG_PIPELINE_AREA_PERMIT_SP",
				  layer_id      = "og_pipeline_area_permit_id",
				  geometry_name = "shape",
				  geometry_type = "MultiPolygon",
				  grouping_name = "pipeline",
				  pg_conn_list  = conn_list)

## Rationale: Sunshine Coast data package for roads, rails, trail, tranmission lines etc
## WHSE_TANTALIS.TA_CROWN_RIGHTS_OF_WAY_SVW
import_bcgw_to_pg(src_schema    = "WHSE_TANTALIS",
				  src_layer     = "TA_CROWN_RIGHTS_OF_WAY_SVW",
				  fdw_schema    = "load",
				  dst_schema    = "whse_sp",
				  dst_layer     = "TA_CROWN_RIGHTS_OF_WAY_SVW",
				  layer_id      = "intrid_sid",
				  geometry_name = "shape",
				  geometry_type = "MultiPolygon",
				  grouping_name = "right-of-way",
				  pg_conn_list  = conn_list)

## Rationale: Kootenay Lake data package for roads, rails, trail, tranmission lines etc
## FGDB import
src_path <- "W:\\FOR\\VIC\\HTS\\ANA\\Workarea\\PROVINCIAL\\BC_CE_Integrated_Roads_2021_20210805.gdb"
src_lyr <- "integratedRoadsBuffers"
## import the integratedroads buffers using ogr2ogr
ogr_cmd <- glue('ogr2ogr -overwrite -a_srs EPSG:3005 -nln {src_lyr} -lco SCHEMA=whse_sp -nlt MULTIPOLYGON -sql "SELECT SHAPE as geom, INTEGRATED_ROADS_ID, DRA_ROAD_CLASS, Integrated_Road_Class_Num, Integrated_Road_Class_Descr, CEF_Road_Buffer_Width_m, BUFF_DIST FROM {src_lyr}" -lco OVERWRITE=YES --config PG_USE_COPY YES -f PostgreSQL PG:dbname=prov_data {src_path}')