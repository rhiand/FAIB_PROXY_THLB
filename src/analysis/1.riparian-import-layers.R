library(dadmtools)
source('src/utils/functions.R')

conn_list <- dadmtools::get_pg_conn_list()

repo_path <- 'C:/projects/FAIB_PROXY_THLB'
setwd(repo_path)

dst_schema <- "whse"
vector_schema <- "whse_vector"

## Rationale: Kootenay Lake data package for riparian etc
## WHSE_BASEMAPPING.FWA_RIVERS_POLY
import_bcgw_to_pg(src_schema     = "WHSE_BASEMAPPING",
				  src_layer      = "FWA_RIVERS_POLY",
				  fdw_schema     = "load",
				  dst_schema     = vector_schema,
				  dst_layer      = "FWA_RIVERS_POLY",
				  fields_to_keep = "waterbody_poly_id,watershed_group_id,waterbody_type,gnis_name_1,fwa_watershed_code,local_watershed_code,watershed_group_code,left_right_tributary,feature_area_sqm,feature_length_m",
				  geometry_name  = "geometry",
				  geometry_type  = "MultiPolygon",
				  grouping_name  = NULL,
				  pg_conn_list   = conn_list)

## Rationale: Kootenay Lake data package for riparian etc
## WHSE_BASEMAPPING.FWA_LAKES_POLY
import_bcgw_to_pg(src_schema     = "WHSE_BASEMAPPING",
				  src_layer      = "FWA_LAKES_POLY",
				  fdw_schema     = "load",
				  dst_schema     = vector_schema,
				  dst_layer      = "FWA_LAKES_POLY",
				  fields_to_keep = "waterbody_poly_id,watershed_group_id,waterbody_type,gnis_name_1,fwa_watershed_code,local_watershed_code,watershed_group_code,left_right_tributary,feature_area_sqm,feature_length_m",
				  geometry_name  = "geometry",
				  geometry_type  = "MultiPolygon",
				  grouping_name  = NULL,
				  pg_conn_list   = conn_list)

## Rationale: Kootenay Lake data package for riparian etc
## WHSE_BASEMAPPING.FWA_WETLANDS_POLY
import_bcgw_to_pg(src_schema     = "WHSE_BASEMAPPING",
				  src_layer      = "FWA_WETLANDS_POLY",
				  fdw_schema     = "load",
				  dst_schema     = vector_schema,
				  dst_layer      = "FWA_WETLANDS_POLY",
				  fields_to_keep = "waterbody_poly_id,watershed_group_id,waterbody_type,gnis_name_1,fwa_watershed_code,local_watershed_code,watershed_group_code,left_right_tributary,feature_area_sqm,feature_length_m",
				  geometry_name  = "geometry",
				  geometry_type  = "MultiPolygon",
				  grouping_name  = NULL,
				  pg_conn_list   = conn_list)

## Rationale: Needed to calculate riparian buffers
## WHSE_WATER_MANAGEMENT.WLS_COMMUNITY_WS_PUB_SVW
import_bcgw_to_pg(src_schema     = "WHSE_WATER_MANAGEMENT",
				  src_layer      = "WLS_COMMUNITY_WS_PUB_SVW",
				  fdw_schema     = "load",
				  dst_schema     = vector_schema,
				  dst_layer      = "WLS_COMMUNITY_WS_PUB_SVW",
				  fields_to_keep = "wls_cw_sysid, cw_code, cw_name, cw_source_name, pod_number, cw_legislation, cw_date_created, cw_status, organization",
				  geometry_name  = "shape",
				  geometry_type  = "MultiPolygon",
				  grouping_name  = NULL,
				  pg_conn_list   = conn_list)

## Rationale: Needed to calculate riparian buffers
## whse_forest_vegetation.bec_biogeoclimatic_poly
import_bcgw_to_pg(src_schema     = "WHSE_FOREST_VEGETATION",
				  src_layer      = "bec_biogeoclimatic_poly",
				  fdw_schema     = "load",
				  dst_schema     = vector_schema,
				  dst_layer      = "bec_biogeoclimatic_poly",
				  fields_to_keep = "zone, subzone, variant, phase, natural_disturbance, map_label, bgc_label, zone_name, subzone_name, variant_name, phase_name, natural_disturbance_name, feature_area_sqm, feature_length_m",
				  geometry_name  = "geometry",
				  geometry_type  = "MultiPolygon",
				  grouping_name  = NULL,
				  pg_conn_list   = conn_list)

## Rationale: Needed to calculate floodplain for S1A streams
## WHSE_BASEMAPPING.CWB_FLOODPLAINS_BC_AREA_SP
import_bcgw_to_pg(src_schema     = "WHSE_BASEMAPPING",
				  src_layer      = "CWB_FLOODPLAINS_BC_AREA_SP",
				  fdw_schema     = "load",
				  dst_schema     = vector_schema,
				  dst_layer      = "CWB_FLOODPLAINS_BC_AREA_SP",
				  fields_to_keep = "floodplains_bc_area_id, floodplain_name, designation_date",
				  geometry_name  = "geometry",
				  geometry_type  = "MultiPolygon",
				  grouping_name  = NULL,
				  pg_conn_list   = conn_list)

## Rationale: Needed to do region specific stream order classification
## WHSE_ADMIN_BOUNDARIES.ADM_NR_AREAS_SP
import_bcgw_to_pg(src_schema     = "WHSE_ADMIN_BOUNDARIES",
				  src_layer      = "ADM_NR_AREAS_SP",
				  fdw_schema     = "load",
				  dst_schema     = vector_schema,
				  dst_layer      = "ADM_NR_AREAS_SP",
				  fields_to_keep = "area_number, area_name, org_unit, org_unit_name, feature_code, feature_name",
				  geometry_name  = "shape",
				  geometry_type  = "MultiPolygon",
				  grouping_name  = NULL,
				  pg_conn_list   = conn_list)


## Rationale: channel width needed to calculate riparian buffers
## Data source from Simon Norris/Craig Mount
## see \\spatialfiles2.bcgov\work\FOR\VIC\HTS\DAM\Staff_WorkArea\heckstrand\thlb_proxy\local_inputs\raw_README.txt for further details
query <- glue("DROP TABLE IF EXISTS {dst_schema}.fwa_stream_networks_channel_width;")
run_sql_r(query, conn_list)
query <- glue("CREATE TABLE {dst_schema}.fwa_stream_networks_channel_width
(
    linear_feature_id integer,
    channel_width_source character varying(50),
    channel_width real
)
TABLESPACE pg_default;")
run_sql_r(query, conn_list)

query <- glue("COPY {dst_schema}.fwa_stream_networks_channel_width from '{repo_path}\\data\\input\\fwa_stream_networks_channel_width.csv' CSV HEADER")
run_sql_r(query, conn_list)

## Rationale: channel width needed to calculate riparian buffers
## Data source from Simon Norris/Craig Mount
## see \\spatialfiles2.bcgov\work\FOR\VIC\HTS\DAM\Staff_WorkArea\heckstrand\thlb_proxy\local_inputs\raw_README.txt for further details
src_path <- glue("{repo_path}\\data\\input\\fishpassage.gpkg")
src_lyr <- "modelled_habitat_potential"
## import the integratedroads buffers using ogr2ogr
ogr_cmd <- glue('ogr2ogr -overwrite -a_srs EPSG:3005 -nln {src_lyr} -lco SCHEMA={dst_schema} -nlt MULTILINESTRING -lco OVERWRITE=YES --config PG_USE_COPY YES -f PostgreSQL PG:dbname={conn_list$dbname} {src_path}')
# Execute the command
print(ogr_cmd)
system(ogr_cmd)

## Manually edit down the original excel spreadsheet to only include needed columns
query <- glue("DROP TABLE IF EXISTS {dst_schema}.june13_riparian_data_for_faib")
run_sql_r(query, conn_list)
query <- glue("CREATE TABLE IF NOT EXISTS {dst_schema}.june13_riparian_data_for_faib
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

TABLESPACE pg_default;")
run_sql_r(query, conn_list)
query <- glue("COPY {dst_schema}.june13_riparian_data_for_faib from '{repo_path}\\data\\input\\June13_Riparian data_for_FAIB.csv' CSV HEADER")
run_sql_r(query, conn_list)

## Jul 2025: never used the BCTS_Field_Streams to check the riparian product.
# src_path <- glue("{repo_path}\\data\\input\\BCTS_Field_Streams.gpkg")
# src_lyr <- "bcts_field_streams"
# ogr_cmd <- glue('ogr2ogr -overwrite -a_srs EPSG:3005 -nln {src_lyr} -lco SCHEMA={dst_schema} -nlt MULTILINESTRING -lco OVERWRITE=YES --config PG_USE_COPY YES -f PostgreSQL PG:dbname={conn_list$dbname} {src_path}')
# system(ogr_cmd)