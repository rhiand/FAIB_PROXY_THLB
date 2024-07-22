library(RPostgres)
library(glue)
library(faibDataManagement)
start_time <- Sys.time()
print(glue("Script started at {format(start_time, '%Y-%m-%d %I:%M:%S %p')}"))
## relies on install_github("bcgov/FAIB_DATA_MANAGEMENT") being installed at some point
conn_list <- faibDataManagement::get_pg_conn_list()
## relies on the keyring connList being populated
conn <- DBI::dbConnect(conn_list["driver"][[1]],
				host = conn_list["host"][[1]],
								user = conn_list["user"][[1]],
								dbname = conn_list["dbname"][[1]],
								password = conn_list["password"][[1]],
								port = conn_list["port"][[1]])

## remove private lands from proxy
query <- "
WITH ws_no_cutblocks AS (
	SELECT 
		ws.pgid 
	FROM 
		whse.fwa_assessment_ws_gr_skey ws_key 
	LEFT JOIN whse.fwa_assessment_ws ws ON ws_key.pgid = ws.pgid 
	LEFT JOIN whse.veg_consolidated_cut_blocks_sp_gr_skey cutblocks_key ON cutblocks_key.gr_skey = ws_key.gr_skey 
	LEFT JOIN whse.veg_consolidated_cut_blocks_sp cutblocks ON cutblocks.pgid = cutblocks_key.pgid
	GROUP BY 
		ws.pgid 
	HAVING 
		sum(case when cutblocks.pgid is null then 0 else 1 end)::real/count(*)::real = 0 
)
SELECT 
	CASE  
		WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited', 'High Restricted') OR harvest_lu.rollup_name IS NULL THEN 0 
		WHEN f_own.own < 60 THEN 0
		WHEN coast.pgid IS NULL THEN 
			CASE  
				WHEN slope_key.val > 50 THEN 0 
				WHEN vri.site_index < 7 THEN 0 
				WHEN ws_no_cutblocks.pgid IS NOT NULL THEN 0 
				ELSE og.tp_falb 
			END 
		ELSE 
			CASE
				WHEN slope_key.val > 70 THEN 0 
				WHEN vri.site_index < 12 then 0 
				WHEN ws_no_cutblocks.pgid IS NOT NULL 
				THEN 0 ELSE og.tp_falb 
			END 
		END as raster_value, 
		st_buffer(geom, 50, 'endcap=square') 
FROM 
	ogsr.og_prov_res og 
LEFT JOIN whse.f_own_gr_skey f_own_key USING (gr_skey)
LEFT JOIN whse.f_own f_own on f_own.pgid = f_own_key.pgid
LEFT JOIN  whse.rr_restriction_gr_skey harvest_key on harvest_key.gr_skey = og.gr_skey
LEFT JOIN whse.rr_restriction harvest ON harvest.pgid = harvest_key.pgid 
LEFT JOIN whse.rr_restriction_three_zone harvest_lu ON harvest_lu.rr_restriction = harvest.rr_restriction 
LEFT JOIN whse.coast_forest_act_gr_skey coast_key ON coast_key.gr_skey = og.gr_skey 
LEFT JOIN whse.coast_forest_act coast ON coast.pgid = coast_key.pgid 
LEFT JOIN whse.slope_gr_skey slope_key ON og.gr_skey = slope_key.gr_skey 
LEFT JOIN whse.veg_comp_lyr_r1_poly_internal_2022_gr_skey vri_key ON vri_key.gr_skey = og.gr_skey 
LEFT JOIN whse.veg_comp_lyr_r1_poly_internal_2022 vri ON vri_key.pgid = vri.pgid 
LEFT JOIN whse.fwa_assessment_ws_gr_skey ws_key on ws_key.gr_skey = og.gr_skey 
LEFT JOIN ws_no_cutblocks ON ws_key.pgid = ws_no_cutblocks.pgid 
LEFT JOIN whse.all_bc_gr_skey geom ON geom.gr_skey = og.gr_skey;"

## IN ONE LINE for the tif writer
## WITH ws_no_cutblocks AS (SELECT ws.pgid FROM whse.fwa_assessment_ws_gr_skey ws_key LEFT JOIN whse.fwa_assessment_ws ws ON ws_key.pgid = ws.pgid LEFT JOIN whse.veg_consolidated_cut_blocks_sp_gr_skey cutblocks_key ON cutblocks_key.gr_skey = ws_key.gr_skey LEFT JOIN whse.veg_consolidated_cut_blocks_sp cutblocks ON cutblocks.pgid = cutblocks_key.pgid GROUP BY ws.pgid HAVING sum(case when cutblocks.pgid is null then 0 else 1 end)::real/count(*)::real = 0 ) SELECT CASE WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited', 'High Restricted') OR harvest_lu.rollup_name IS NULL THEN 0 WHEN f_own.own < 60 THEN 0 WHEN coast.pgid IS NULL THEN CASE WHEN slope_key.val > 50 THEN 0 WHEN vri.site_index < 7 THEN 0 WHEN ws_no_cutblocks.pgid IS NOT NULL THEN 0 ELSE og.tp_falb END ELSE CASE WHEN slope_key.val > 70 THEN 0 WHEN vri.site_index < 12 then 0 WHEN ws_no_cutblocks.pgid IS NOT NULL THEN 0 ELSE og.tp_falb END END as raster_value, st_buffer(geom, 50, 'endcap=square') FROM ogsr.og_prov_res og LEFT JOIN whse.f_own_gr_skey f_own_key USING (gr_skey) LEFT JOIN whse.f_own f_own on f_own.pgid = f_own_key.pgid LEFT JOIN whse.rr_restriction_gr_skey harvest_key on harvest_key.gr_skey = og.gr_skey LEFT JOIN whse.rr_restriction harvest ON harvest.pgid = harvest_key.pgid LEFT JOIN whse.rr_restriction_three_zone harvest_lu ON harvest_lu.rr_restriction = harvest.rr_restriction LEFT JOIN whse.coast_forest_act_gr_skey coast_key ON coast_key.gr_skey = og.gr_skey LEFT JOIN whse.coast_forest_act coast ON coast.pgid = coast_key.pgid LEFT JOIN whse.slope_gr_skey slope_key ON og.gr_skey = slope_key.gr_skey LEFT JOIN whse.veg_comp_lyr_r1_poly_internal_2022_gr_skey vri_key ON vri_key.gr_skey = og.gr_skey LEFT JOIN whse.veg_comp_lyr_r1_poly_internal_2022 vri ON vri_key.pgid = vri.pgid LEFT JOIN whse.fwa_assessment_ws_gr_skey ws_key on ws_key.gr_skey = og.gr_skey LEFT JOIN ws_no_cutblocks ON ws_key.pgid = ws_no_cutblocks.pgid LEFT JOIN whse.all_bc_gr_skey geom ON geom.gr_skey = og.gr_skey;
## to be created into a tif
