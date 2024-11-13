library(dadmtools)
library(httr)
library(rvest)
source('src/utils/functions.R')
conn_list <- dadmtools::get_pg_conn_list()

## Identify TFLS that overlap with multiple TSA boundaries (> 20%)
query <- "WITH A AS 
(
	SELECT
		mu_tsa.man_unit as tsa,
		mu_tfl.man_unit as manlic,
		count(*) OVER (PARTITION BY mu_tfl.man_unit) as man_unit_count,
		count(*) as ha
	FROM
	whse.tsa_boundaries_gr_skey tsa_key
	JOIN whse.tsa_boundaries tsa USING (pgid)
	LEFT JOIN whse.fadm_tfl_all_sp_gr_skey tfl_key on tfl_key.gr_skey = tsa_key.gr_skey
	LEFT JOIN whse.fadm_tfl_all_sp tfl on tfl.pgid = tfl_key.pgid
	LEFT JOIN whse.mu_lookup_table_im mu_tsa ON tsa.tsa = mu_tsa.tsa_number
	LEFT JOIN whse.mu_lookup_table_im mu_tfl ON tfl.forest_file_id = mu_tfl.forest_file_id
	GROUP BY 
		mu_tsa.man_unit,
		mu_tfl.man_unit
	ORDER BY 
		mu_tfl.man_unit,
		count(*) DESC
), percent_breakdown AS (
select
	tsa,
	manlic,
	man_unit_count,
	ha,
	round(((ha::real/sum(ha) OVER (PARTITION BY manlic)) * 100)::numeric, 0)
from 
	a 
where 
	man_unit_count > 1

order by 
	manlic,
	ha DESC
)
SELECT
* FROM 
percent_breakdown
where round >= 25 and round <= 75"
## TFL 25 has sig areas within both TSAs 46 - GBR North TSA (129,809 ha)      & - 47 - GBR South TSA (66,381 ha)
## TFL 39 has sig areas within both TSAs 39 - Sunshine Coast TSA (136,445 ha) & - 47 - GBR South TSA (51,549 ha)
## TFL 47 has sig areas within both TSAs 47 - GBR South TSA (76,487 ha)       & - 48 - North Island TSA (49,028 ha)

## Identify area based managed licences that overlap with multiple TSA boundaries (> 20%)
query <- "WITH A AS 
(
	SELECT
		mu_tsa.man_unit as tsa,
		mu_manlic.man_unit as manlic,
		count(*) OVER (PARTITION BY mu_manlic.man_unit) as man_unit_count,
		count(*) as ha
	FROM
	whse.tsa_boundaries_gr_skey tsa_key
	JOIN whse.tsa_boundaries tsa USING (pgid)
	LEFT JOIN whse.ften_managed_licence_poly_svw_gr_skey manlic_key ON manlic_key.gr_skey = tsa_key.gr_skey
	LEFT JOIN whse.ften_managed_licence_poly_svw man_lic ON manlic_key.pgid = man_lic.pgid
	LEFT JOIN whse.mu_lookup_table_im mu_tsa ON tsa.tsa = mu_tsa.tsa_number
	LEFT JOIN whse.mu_lookup_table_im mu_manlic ON man_lic.forest_file_id = mu_manlic.forest_file_id
	GROUP BY 
		mu_tsa.man_unit,
		mu_manlic.man_unit
	ORDER BY 
		mu_manlic.man_unit,
		count(*) DESC
), percent_breakdown AS (
select
	tsa,
	manlic,
	man_unit_count,
	ha,
	round(((ha::real/sum(ha) OVER (PARTITION BY manlic)) * 100)::numeric, 0)
from 
	a 
where 
	man_unit_count > 1

order by 
	manlic,
	ha DESC
)
SELECT
* FROM 
percent_breakdown
where round >= 25 and round <= 75"
## area based managed licences
## N2D - FNWL       split between 48 - North Island TSA (2022 ha)  & 47 - GBR South TSA (1213 ha)
## N2I - FNWL       split between 14 - Lakes TSA (24,450 ha)       & 20 - Morice TSA (12,044 ha)
## W0117 - Woodlot  split between 20 - Morice TSA (594 ha)         & 14 - Lakes TSA (576 ha)
## W1737 - Woodlot  split between 18 - Merritt TSA (688 ha)        & 15 - Lillooet TSA (612 ha)
## W1506 - Woodlot  split between 3 - Bulkley TSA (597 ha)         & 20 - Morice TSA (594 ha)
## W1823 - Woodlot  split between 22 - Okanagan TSA (821 ha)       & 11 - Kamloops TSA (598 ha)


## create a lookup table of the TSA with the greatest area overlap with the TFL or area based managed licences
query <- 'DROP TABLE IF EXISTS thlb_proxy.tsa_link_tfl_manlic;'
run_sql_r(query, conn_list)
query <- 'CREATE TABLE thlb_proxy.tsa_link_tfl_manlic AS
(
SELECT
	distinct on (mu_tfl.man_unit)
	mu_tsa.man_unit as tsa,
	mu_tfl.man_unit as man_unit
FROM
whse.tsa_boundaries_gr_skey tsa_key
JOIN whse.tsa_boundaries tsa USING (pgid)
LEFT JOIN whse.fadm_tfl_all_sp_gr_skey tfl_key on tfl_key.gr_skey = tsa_key.gr_skey
LEFT JOIN whse.fadm_tfl_all_sp tfl on tfl.pgid = tfl_key.pgid
LEFT JOIN whse.mu_lookup_table_im mu_tsa ON tsa.tsa = mu_tsa.tsa_number
LEFT JOIN whse.mu_lookup_table_im mu_tfl ON tfl.forest_file_id = mu_tfl.forest_file_id
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
whse.tsa_boundaries_gr_skey tsa_key
JOIN whse.tsa_boundaries tsa USING (pgid)
LEFT JOIN whse.ften_managed_licence_poly_svw_gr_skey manlic_key on manlic_key.gr_skey = tsa_key.gr_skey
LEFT JOIN whse.ften_managed_licence_poly_svw manlic on manlic.pgid = manlic_key.pgid
LEFT JOIN whse.mu_lookup_table_im mu_tsa ON tsa.tsa = mu_tsa.tsa_number
LEFT JOIN whse.mu_lookup_table_im mu_manlic ON manlic.forest_file_id = mu_manlic.forest_file_id
WHERE
	mu_manlic.man_unit IS NOT NULL
GROUP BY 
	mu_tsa.man_unit,
	mu_manlic.man_unit
ORDER BY 
	mu_manlic.man_unit,
	mu_tsa.man_unit,
	count(*) DESC
)'
run_sql_r(query, conn_list)
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

## Import the TFLs
import_bcgw_to_pg(src_schema   = "WHSE_ADMIN_BOUNDARIES",
				src_layer      = "FADM_TFL_ALL_SP",
				fdw_schema     = "load",
				dst_schema     = "thlb_proxy",
				dst_layer      = "FADM_TFL_ALL_SP",
				fields_to_keep = "tfl_all_sysid,forest_file_id,tfl_type,licencee",
				geometry_name  = "shape",
				geometry_type  = "MultiPolygon",
				grouping_name  = NULL,
				where_clause   = NULL,
				pg_conn_list   = conn_list)

## FGDB import
src_path <- "W:\\FOR\\VIC\\HTS\\ANA\\Workarea\\PROVINCIAL\\provincial.gdb"
src_lyr <- "tsa_boundaries_2020"

dbname <- conn_list[['dbname']]
ogr_cmd <- glue('ogr2ogr -overwrite -a_srs EPSG:3005 -lco GEOMETRY_NAME=geom -nln {src_lyr} -lco SCHEMA=thlb_proxy -nlt MULTIPOLYGON -lco OVERWRITE=YES --config PG_USE_COPY YES -f PostgreSQL PG:dbname={dbname} {src_path} {src_lyr}')
print(ogr_cmd)
system(ogr_cmd)

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