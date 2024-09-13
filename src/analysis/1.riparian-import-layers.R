library(RPostgres)
library(glue)
library(devtools)
library(dadmtools)
source('src/utils/functions.R')

## relies on install_github("bcgov/FAIB_DATA_MANAGEMENT") being installed at some point
conn_list <- dadmtools::get_pg_conn_list()

## Rationale: Kootenay Lake data package for riparian etc
## WHSE_BASEMAPPING.FWA_STREAM_NETWORKS_SP
import_bcgw_to_pg(src_schema    = "WHSE_BASEMAPPING",
				  src_layer     = "FWA_STREAM_NETWORKS_SP",
				  fdw_schema    = "load",
				  dst_schema    = "thlb_proxy",
				  dst_layer     = "FWA_STREAM_NETWORKS_SP",
				  layer_id      = "linear_feature_id, watershed_group_id, edge_type, blue_line_key, watershed_key, fwa_watershed_code, local_watershed_code,watershed_group_code, downstream_route_measure, length_metre, gnis_name, stream_order, stream_magnitude",
				  geometry_name = "geometry",
				  geometry_type = "MultiLineString",
				  grouping_name = "stream",
				  pg_conn_list  = conn_list)

## Rationale: Kootenay Lake data package for riparian etc
## WHSE_BASEMAPPING.FWA_RIVERS_POLY
import_bcgw_to_pg(src_schema    = "WHSE_BASEMAPPING",
				  src_layer     = "FWA_RIVERS_POLY",
				  fdw_schema    = "load",
				  dst_schema    = "thlb_proxy",
				  dst_layer     = "FWA_RIVERS_POLY",
				  layer_id      = "waterbody_poly_id,watershed_group_id,waterbody_type,gnis_name_1,fwa_watershed_code,local_watershed_code,watershed_group_code,left_right_tributary,feature_area_sqm,feature_length_m",
				  geometry_name = "geometry",
				  geometry_type = "MultiPolygon",
				  grouping_name = NULL,
				  pg_conn_list  = conn_list)

## Rationale: Kootenay Lake data package for riparian etc
## WHSE_BASEMAPPING.FWA_LAKES_POLY
import_bcgw_to_pg(src_schema    = "WHSE_BASEMAPPING",
				  src_layer     = "FWA_LAKES_POLY",
				  fdw_schema    = "load",
				  dst_schema    = "thlb_proxy",
				  dst_layer     = "FWA_LAKES_POLY",
				  layer_id      = "waterbody_poly_id,watershed_group_id,waterbody_type,gnis_name_1,fwa_watershed_code,local_watershed_code,watershed_group_code,left_right_tributary,feature_area_sqm,feature_length_m",
				  geometry_name = "geometry",
				  geometry_type = "MultiPolygon",
				  grouping_name = NULL,
				  pg_conn_list  = conn_list)

## Rationale: Kootenay Lake data package for riparian etc
## WHSE_BASEMAPPING.FWA_WETLANDS_POLY
import_bcgw_to_pg(src_schema    = "WHSE_BASEMAPPING",
				  src_layer     = "FWA_WETLANDS_POLY",
				  fdw_schema    = "load",
				  dst_schema    = "thlb_proxy",
				  dst_layer     = "FWA_WETLANDS_POLY",
				  layer_id      = "waterbody_poly_id,watershed_group_id,waterbody_type,gnis_name_1,fwa_watershed_code,local_watershed_code,watershed_group_code,left_right_tributary,feature_area_sqm,feature_length_m",
				  geometry_name = "geometry",
				  geometry_type = "MultiPolygon",
				  grouping_name = NULL,
				  pg_conn_list  = conn_list)

## Rationale: Needed to calculate riparian buffers
## WHSE_WATER_MANAGEMENT.WLS_COMMUNITY_WS_PUB_SVW
import_bcgw_to_pg(src_schema    = "WHSE_WATER_MANAGEMENT",
				  src_layer     = "WLS_COMMUNITY_WS_PUB_SVW",
				  fdw_schema    = "load",
				  dst_schema    = "thlb_proxy",
				  dst_layer     = "WLS_COMMUNITY_WS_PUB_SVW",
				  layer_id      = "wls_cw_sysid, cw_code, cw_name, cw_source_name, pod_number, cw_legislation, cw_date_created, cw_status, organization",
				  geometry_name = "shape",
				  geometry_type = "MultiPolygon",
				  grouping_name = NULL,
				  pg_conn_list  = conn_list)

## Rationale: Needed to calculate riparian buffers
## whse_forest_vegetation.bec_biogeoclimatic_poly
import_bcgw_to_pg(src_schema    = "WHSE_FOREST_VEGETATION",
				  src_layer     = "bec_biogeoclimatic_poly",
				  fdw_schema    = "load",
				  dst_schema    = "thlb_proxy",
				  dst_layer     = "bec_biogeoclimatic_poly",
				  layer_id      = "zone, subzone, variant, phase, natural_disturbance, map_label, bgc_label, zone_name, subzone_name, variant_name, phase_name, natural_disturbance_name, feature_area_sqm, feature_length_m",
				  geometry_name = "geometry",
				  geometry_type = "MultiPolygon",
				  grouping_name = NULL,
				  pg_conn_list  = conn_list)

## Rationale: Needed to calculate floodplain for S1A streams
## WHSE_BASEMAPPING.CWB_FLOODPLAINS_BC_AREA_SP
import_bcgw_to_pg(src_schema    = "WHSE_BASEMAPPING",
				  src_layer     = "CWB_FLOODPLAINS_BC_AREA_SP",
				  fdw_schema    = "load",
				  dst_schema    = "thlb_proxy",
				  dst_layer     = "CWB_FLOODPLAINS_BC_AREA_SP",
				  layer_id      = "floodplains_bc_area_id, floodplain_name, designation_date",
				  geometry_name = "geometry",
				  geometry_type = "MultiPolygon",
				  grouping_name = NULL,
				  pg_conn_list  = conn_list)

## Rationale: Needed to do region specific stream order classification
## WHSE_ADMIN_BOUNDARIES.ADM_NR_AREAS_SP
import_bcgw_to_pg(src_schema    = "WHSE_ADMIN_BOUNDARIES",
				  src_layer     = "ADM_NR_AREAS_SP",
				  fdw_schema    = "load",
				  dst_schema    = "thlb_proxy",
				  dst_layer     = "ADM_NR_AREAS_SP",
				  layer_id      = "area_number, area_name, org_unit, org_unit_name, feature_code, feature_name",
				  geometry_name = "shape",
				  geometry_type = "MultiPolygon",
				  grouping_name = NULL,
				  pg_conn_list  = conn_list)


## Rationale: channel width needed to calculate riparian buffers
## Data source from Simon Norris/Craig Mount
## see data\raw\raw_README.txt for further details
query <- "DROP TABLE IF EXISTS thlb_proxy.fwa_stream_networks_channel_width;"
run_sql_r(query, conn_list)
query <- "CREATE TABLE thlb_proxy.fwa_stream_networks_channel_width
(
    linear_feature_id integer,
    channel_width_source character varying(50),
    channel_width real
)
TABLESPACE pg_default;"
run_sql_r(query, conn_list)
query <- "COPY thlb_proxy.fwa_stream_networks_channel_width from 'data\\input\\fwa_stream_networks_channel_width.csv' CSV HEADER"
run_sql_r(query, conn_list)

## Rationale: channel width needed to calculate riparian buffers
## Data source from Simon Norris/Craig Mount
## see data\raw\raw_README.txt for further details
src_path <- "data\\input\\fishpassage.gpkg"
src_lyr <- "modelled_habitat_potential"
## import the integratedroads buffers using ogr2ogr
ogr_cmd <- glue('ogr2ogr -overwrite -a_srs EPSG:3005 -nln {src_lyr} -lco SCHEMA=thlb_proxy -nlt MULTILINESTRING -lco OVERWRITE=YES --config PG_USE_COPY YES -f PostgreSQL PG:dbname=prov_data {src_path}')
# Execute the command
print(ogr_cmd)
system(ogr_cmd)

## Manually edit down the original excel spreadsheet to only include needed columns
query <- "DROP TABLE IF EXISTS thlb_proxy.june13_riparian_data_for_faib"
run_sql_r(query, conn_list)
query <- "CREATE TABLE IF NOT EXISTS thlb_proxy.june13_riparian_data_for_faib
(
	objectid integer,
    evaluation_year integer,
    evaluation_date character varying(50),
    year_of_harvest integer,
    stream_name character varying(50),
    stream_class_in_field character varying(50),
    channel_depth real,
    channel_width real,
    channel_gradient_pct real,
    bcalbers_easting double precision,
    bcalbers_northing double precision
)

TABLESPACE pg_default;"
run_sql_r(query, conn_list)
query <- "COPY thlb_proxy.june13_riparian_data_for_faib from 'data\\input\\June13_Riparian data_for_FAIB.csv' CSV HEADER"
run_sql_r(query, conn_list)


src_path <- "data\\input\\BCTS_Field_Streams.gpkg"
src_lyr <- "bcts_field_streams"
ogr_cmd <- glue('ogr2ogr -overwrite -a_srs EPSG:3005 -nln {src_lyr} -lco SCHEMA=thlb_proxy -nlt MULTILINESTRING -lco OVERWRITE=YES --config PG_USE_COPY YES -f PostgreSQL PG:dbname=prov_data {src_path}')
system(ogr_cmd)