library(dadmtools)
library(httr)
library(rvest)
source('src/utils/functions.R')
conn_list <- dadmtools::get_pg_conn_list()

## import stability vector into postgres
## Filter the dataset retaining:
classes_to_keep <- "('Potentially unstable', 'Potentially unstable after road building', 'Unstable')"
import_bcgw_to_pg(src_schema     = "WHSE_TERRESTRIAL_ECOLOGY",
				  src_layer      = "STE_TER_ATTRIBUTE_POLYS_SVW",
				  fdw_schema     = "load",
				  dst_schema     = "thlb_proxy",
				  dst_layer      = "STE_TER_ATTRIBUTE_POLYS_SVW",
				  fields_to_keep = "teis_id, slope_stability_class_w_roads, slope_stability_class_txt, project_type",
				  geometry_name  = "geometry",
				  geometry_type  = "MultiPolygon",
				  grouping_name  = NULL,
				  where_clause   = glue("slope_stability_class_txt IN {classes_to_keep}"),
				  pg_conn_list   = conn_list)

## import the most recent consolidated cutblocks from bcgw
import_bcgw_to_pg(src_schema     = "WHSE_FOREST_VEGETATION",
				  src_layer      = "VEG_CONSOLIDATED_CUT_BLOCKS_SP",
				  fdw_schema     = "load",
				  dst_schema     = "thlb_proxy",
				  dst_layer      = "VEG_CONSOLIDATED_CUT_BLOCKS_SP",
				  fields_to_keep = "veg_consolidated_cut_block_id, harvest_year",
				  geometry_name  = "shape",
				  geometry_type  = "MultiPolygon",
				  grouping_name  = NULL,
				  pg_conn_list   = conn_list)

## Import the Forest Tenure Managed Licences (i.e. area based tenures)
import_bcgw_to_pg(src_schema   = "WHSE_FOREST_TENURE",
				src_layer      = "FTEN_MANAGED_LICENCE_POLY_SVW",
				fdw_schema     = "load",
				dst_schema     = "thlb_proxy",
				dst_layer      = "FTEN_MANAGED_LICENCE_POLY_SVW",
				fields_to_keep = "forest_file_id,map_block_id,ml_type_code,map_label,file_status_code",
				geometry_name  = "geometry",
				geometry_type  = "MultiPolygon",
				grouping_name  = NULL,
				where_clause   = glue("(Retirement_Date is Null or RETIREMENT_DATE > CURRENT_DATE) and LIFE_CYCLE_STATUS_CODE = 'ACTIVE'  and FILE_STATUS_CODE = 'HI'"),
				pg_conn_list   = conn_list)
## post processing the stability layer
query <- "DROP TABLE IF EXISTS thlb_proxy.ste_ter_attribute_polys_svw_union;"
run_sql_r(query, conn_list)
query <- glue("CREATE TABLE thlb_proxy.ste_ter_attribute_polys_svw_union AS
SELECT
	ST_Union(stab.geom) as geom
FROM
	thlb_proxy.ste_ter_attribute_polys_svw")
run_sql_r(query, conn_list)

query <- "CREATE TABLE thlb_proxy.ste_ter_attribute_polys_svw_ar AS
SELECT
	(ST_Dump(geom)).geom as geom
FROM
	thlb_proxy.ste_ter_attribute_polys_svw_union"
run_sql_r(query, conn_list)

query <- "DROP TABLE IF EXISTS thlb_proxy.ste_ter_attribute_polys_svw_union"
run_sql_r(query, conn_list)