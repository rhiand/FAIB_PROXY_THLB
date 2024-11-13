library(dadmtools)
source('src/utils/functions.R')

conn_list <- dadmtools::get_pg_conn_list()
db <- DBI::dbConnect(conn_list["driver"][[1]],
				host = conn_list["host"][[1]],
				user = conn_list["user"][[1]],
				dbname = conn_list["dbname"][[1]],
				password = conn_list["password"][[1]],
				port = conn_list["port"][[1]])
start_time <- Sys.time()
print(glue("Script started at {format(start_time, '%Y-%m-%d %I:%M:%S %p')}"))
dst_schema <- "thlb_proxy"
query <- glue("DROP TABLE IF EXISTS {dst_schema}.bc_merchantability_gr_skey")
run_sql_r(query, conn_list)

query <- glue("CREATE TABLE {dst_schema}.bc_merchantability_gr_skey AS
WITH p5_site_index AS (
	SELECT
		vri_key.gr_skey
		, CASE 
			WHEN si_manlic.man_unit IS NOT null THEN si_manlic.man_unit
			WHEN si_tfl.man_unit IS NOT NULL THEN si_tfl.man_unit
			WHEN si_tsa.man_unit IS NOT NULL THEN si_tsa.man_unit
		END AS man_unit
		, CASE 
			WHEN si_manlic.man_unit IS NOT null THEN si_manlic.tfl_integrated_p5
			WHEN si_tfl.man_unit IS NOT NULL THEN si_tfl.tfl_integrated_p5
			WHEN si_tsa.man_unit IS NOT NULL THEN si_tsa.tfl_integrated_p5
		END AS p5
		, vri.site_index
	FROM
	whse.veg_comp_lyr_r1_poly_internal_2023_gr_skey vri_key
	LEFT JOIN (SELECT pgid, CASE WHEN site_index = 0 THEN NULL ELSE site_index END AS site_index FROM whse.veg_comp_lyr_r1_poly_internal_2023) vri USING (pgid)
	LEFT JOIN whse.tsa_boundaries_gr_skey tsa_key on tsa_key.gr_skey = vri_key.gr_skey
	LEFT JOIN whse.tsa_boundaries tsa on tsa.pgid = tsa_key.pgid 
	LEFT JOIN {dst_schema}.tsa_tfl_abt_5p_site_index_cc si_tsa ON si_tsa.forest_file_id = tsa.tsa_number
	LEFT JOIN whse.fadm_tfl_all_sp_gr_skey tfl_key ON tfl_key.gr_skey = vri_key.gr_skey
	LEFT JOIN whse.fadm_tfl_all_sp tfl ON tfl.pgid = tfl_key.pgid 
	LEFT JOIN {dst_schema}.tsa_tfl_abt_5p_site_index_cc si_tfl ON tfl.forest_file_id = si_tfl.forest_file_id
	LEFT JOIN whse.ften_managed_licence_poly_svw_gr_skey manlic_key ON manlic_key.gr_skey = vri_key.gr_skey
	LEFT JOIN whse.ften_managed_licence_poly_svw manlic ON manlic.pgid = manlic_key.pgid 
	LEFT JOIN {dst_schema}.tsa_tfl_abt_5p_site_index_cc si_manlic ON manlic.forest_file_id = si_manlic.forest_file_id
)
SELECT
	gr_skey,
	man_unit,
	CASE
		WHEN site_index IS NULL THEN NULL
		WHEN site_index > p5 THEN 1 
		ELSE 0 
	END AS merchantability
FROM
	p5_site_index")
run_sql_r(query, conn_list)

