library(dadmtools)
library(httr)
library(rvest)
source('src/utils/functions.R')
conn_list <- dadmtools::get_pg_conn_list()


import_bcgw_to_pg(src_schema    = "WHSE_TERRESTRIAL_ECOLOGY",
				  src_layer     = "STE_TER_ATTRIBUTE_POLYS_SVW",
				  fdw_schema    = "load",
				  dst_schema    = "thlb_proxy",
				  dst_layer     = "STE_TER_ATTRIBUTE_POLYS_SVW",
				  layer_id      = "teis_id, slope_stability_class_w_roads, slope_stability_class_txt, project_type",
				  geometry_name = "geometry",
				  geometry_type = "MultiPolygon",
				  grouping_name = "NULL",
				  pg_conn_list  = conn_list)

import_bcgw_to_pg(src_schema    = "WHSE_FOREST_VEGETATION",
				  src_layer     = "VEG_CONSOLIDATED_CUT_BLOCKS_SP",
				  fdw_schema    = "load",
				  dst_schema    = "thlb_proxy",
				  dst_layer     = "VEG_CONSOLIDATED_CUT_BLOCKS_SP",
				  layer_id      = "veg_consolidated_cut_block_id, harvest_year",
				  geometry_name = "shape",
				  geometry_type = "MultiPolygon",
				  grouping_name = "NULL",
				  pg_conn_list  = conn_list)

import_bcgw_to_pg(src_schema    = "WHSE_ADMIN_BOUNDARIES",
				  src_layer     = "ADM_NR_REGIONS_SP",
				  fdw_schema    = "load",
				  dst_schema    = "thlb_proxy",
				  dst_layer     = "ADM_NR_REGIONS_SP",
				  layer_id      = "region_name",
				  geometry_name = "shape",
				  geometry_type = "MultiPolygon",
				  grouping_name = "NULL",
				  pg_conn_list  = conn_list)


## post processing the stability layer
query <- "DROP TABLE IF EXISTS thlb_proxy.ste_ter_attribute_polys_svw_union;"
run_sql_r(query, conn_list)
query <- "CREATE TABLE thlb_proxy.ste_ter_attribute_polys_svw_union AS
SELECT
	ST_Union(stab.geom) as geom
FROM
thlb_proxy.ste_ter_attribute_polys_svw stab
WHERE
	stab.slope_stability_class_txt IN ('Potentially unstable', 'Potentially unstable after road building', 'Unstable')"
run_sql_r(query, conn_list)

query <- "CREATE TABLE thlb_proxy.ste_ter_attribute_polys_svw_ar AS
SELECT
	(ST_Dump(geom)).geom as geom
FROM
	thlb_proxy.ste_ter_attribute_polys_svw_union"
run_sql_r(query, conn_list)

query <- "DROP TABLE IF EXISTS thlb_proxy.ste_ter_attribute_polys_svw_union"
run_sql_r(query, conn_list)