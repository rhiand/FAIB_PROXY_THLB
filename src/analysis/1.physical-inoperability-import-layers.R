library(dadmtools)
source('src/utils/functions.R')
conn_list <- dadmtools::get_pg_conn_list()

dst_schema <- "whse"
vector_schema <- "whse_vector"

## create a lookup table of the TSA with the greatest area overlap with the TFL or area based managed licences
query <- glue('DROP TABLE IF EXISTS {dst_schema}.tsa_link_tfl_manlic;')
run_sql_r(query, conn_list)
query <- glue('CREATE TABLE {dst_schema}.tsa_link_tfl_manlic AS
(
SELECT
	distinct on (mu_tfl.man_unit)
	mu_tsa.man_unit as tsa,
	mu_tfl.man_unit as man_unit
FROM
{dst_schema}.tsa_boundaries_gr_skey tsa_key
JOIN {dst_schema}.tsa_boundaries tsa USING (pgid)
LEFT JOIN {dst_schema}.fadm_tfl_all_sp_gr_skey tfl_key on tfl_key.gr_skey = tsa_key.gr_skey
LEFT JOIN {dst_schema}.fadm_tfl_all_sp tfl on tfl.pgid = tfl_key.pgid
LEFT JOIN {dst_schema}.mu_lookup_table_im mu_tsa ON tsa.tsa = mu_tsa.tsa_number
LEFT JOIN {dst_schema}.mu_lookup_table_im mu_tfl ON tfl.forest_file_id = mu_tfl.forest_file_id
WHERE
	mu_tfl.man_unit IS NOT NULL
GROUP BY 
	mu_tsa.man_unit,
	mu_tfl.man_unit
ORDER BY 
	mu_tfl.man_unit,
	mu_tsa.man_unit,
	count(*) DESC
	)
UNION ALL
(
SELECT
	distinct on (mu_manlic.man_unit)
	mu_tsa.man_unit as tsa,
	mu_manlic.man_unit as tfl
FROM
{dst_schema}.tsa_boundaries_gr_skey tsa_key
JOIN {dst_schema}.tsa_boundaries tsa USING (pgid)
LEFT JOIN {dst_schema}.ften_managed_licence_poly_svw_gr_skey manlic_key on manlic_key.gr_skey = tsa_key.gr_skey
LEFT JOIN {dst_schema}.ften_managed_licence_poly_svw manlic on manlic.pgid = manlic_key.pgid
LEFT JOIN {dst_schema}.mu_lookup_table_im mu_tsa ON tsa.tsa = mu_tsa.tsa_number
LEFT JOIN {dst_schema}.mu_lookup_table_im mu_manlic ON manlic.forest_file_id = mu_manlic.forest_file_id
WHERE
	mu_manlic.man_unit IS NOT NULL
GROUP BY 
	mu_tsa.man_unit,
	mu_manlic.man_unit
ORDER BY 
	mu_manlic.man_unit,
	mu_tsa.man_unit,
	count(*) DESC
)')
run_sql_r(query, conn_list)

## import stability vector into postgres
## Filter the dataset retaining:
classes_to_keep <- "('Potentially unstable', 'Potentially unstable after road building', 'Unstable')"
import_bcgw_to_pg(src_schema     = "WHSE_TERRESTRIAL_ECOLOGY",
				  src_layer      = "STE_TER_ATTRIBUTE_POLYS_SVW",
				  fdw_schema     = "load",
				  dst_schema     = vector_schema,
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
				  dst_schema     = vector_schema,
				  dst_layer      = "VEG_CONSOLIDATED_CUT_BLOCKS_SP",
				  fields_to_keep = "vccb_sysid, harvest_start_year_calendar",
				  geometry_name  = "shape",
				  geometry_type  = "MultiPolygon",
				  grouping_name  = NULL,
				  pg_conn_list   = conn_list)

## Import the Forest Tenure Managed Licences (i.e. area based tenures)
import_bcgw_to_pg(src_schema   = "WHSE_FOREST_TENURE",
				src_layer      = "FTEN_MANAGED_LICENCE_POLY_SVW",
				fdw_schema     = "load",
				dst_schema     = vector_schema,
				dst_layer      = "FTEN_MANAGED_LICENCE_POLY_SVW",
				fields_to_keep = "forest_file_id,map_block_id,ml_type_code,map_label,file_status_code",
				geometry_name  = "geometry",
				geometry_type  = "MultiPolygon",
				grouping_name  = NULL,
				where_clause   = glue("(Retirement_Date is Null or RETIREMENT_DATE > CURRENT_DATE) and FILE_STATUS_CODE = 'HI'"),
				pg_conn_list   = conn_list)

## Import the TFLs
import_bcgw_to_pg(src_schema   = "WHSE_ADMIN_BOUNDARIES",
				src_layer      = "FADM_TFL_ALL_SP",
				fdw_schema     = "load",
				dst_schema     = vector_schema,
				dst_layer      = "FADM_TFL_ALL_SP",
				fields_to_keep = "tfl_all_sysid,forest_file_id,tfl_type,licencee",
				geometry_name  = "shape",
				geometry_type  = "MultiPolygon",
				grouping_name  = NULL,
				where_clause   = NULL,
				pg_conn_list   = conn_list)

## FGDB import
print('Importing tsa_boundaries_2020')
src_path <- "W:\\FOR\\VIC\\HTS\\ANA\\Workarea\\PROVINCIAL\\provincial.gdb"
src_lyr <- "tsa_boundaries_2020"
dbname <- conn_list[['dbname']]
ogr_cmd <- glue('ogr2ogr -overwrite -a_srs EPSG:3005 -lco GEOMETRY_NAME=geom -nln {src_lyr} -lco SCHEMA={vector_schema} -nlt MULTIPOLYGON -lco OVERWRITE=YES --config PG_USE_COPY YES -f PostgreSQL PG:dbname={dbname} {src_path} {src_lyr}')
print(ogr_cmd)
system(ogr_cmd)

## post processing the stability layer
print('Unioning the stability layer')
query <- glue("DROP TABLE IF EXISTS {vector_schema}.ste_ter_attribute_polys_svw_union;")
run_sql_r(query, conn_list)
query <- glue("CREATE TABLE {vector_schema}.ste_ter_attribute_polys_svw_union AS
SELECT
	ST_Union(stab.geom) as geom
FROM
	{vector_schema}.ste_ter_attribute_polys_svw stab")
run_sql_r(query, conn_list)

query <- glue("CREATE TABLE {vector_schema}.ste_ter_attribute_polys_svw_ar AS
SELECT
	(ST_Dump(geom)).geom as geom
FROM
	{vector_schema}.ste_ter_attribute_polys_svw_union")
run_sql_r(query, conn_list)

query <- glue("DROP TABLE IF EXISTS {vector_schema}.ste_ter_attribute_polys_svw_union")
run_sql_r(query, conn_list)