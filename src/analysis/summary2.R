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


# query <- "SELECT
# 	'FALB' as land_classification
# 	,sum(og.tp_falb) as thlb_area
# 	-- , CASE 
# 	-- 	WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited', 'High Restricted') OR harvest_lu.rollup_name IS NULL THEN 0
# 	-- 	-- not in coast
# 	-- 	WHEN coast.pgid IS NULL THEN
# 	-- 		CASE 
# 	-- 			WHEN slope_key.val > 50 THEN 0
# 	-- 			WHEN vri.site_index < 7 THEN 0
# 	-- 			ELSE og.tp_falb
# 	-- 		END
# 	-- 	ELSE
# 	-- 	-- coast
# 	-- 		CASE 
# 	-- 			WHEN slope_key.val > 70 THEN 0
# 	-- 			WHEN vri.site_index < 12 then 0
# 	-- 			ELSE og.tp_falb
# 	-- 		END
# 	-- END as thlb
#    FROM
# 	ogsr.og_prov_res og
#     -- LEFT JOIN 
#     -- 	whse.rr_restriction_gr_skey harvest_key USING (gr_skey)
#     -- LEFT JOIN
#     -- 	whse.rr_restriction harvest ON harvest.pgid = harvest_key.pgid
#     -- LEFT JOIN
#     -- 	whse.rr_restriction_three_zone harvest_lu ON harvest_lu.rr_restriction = harvest.rr_restriction
#     -- LEFT JOIN
#     -- 	whse.coast_forest_act_gr_skey coast_key ON coast_key.gr_skey = og.gr_skey
#     -- LEFT JOIN
#     -- 	whse.coast_forest_act coast ON coast.pgid = coast_key.pgid
#     -- LEFT JOIN
#     -- 	whse.slope_gr_skey slope_key ON og.gr_skey = slope_key.gr_skey
#     -- LEFT JOIN
#     -- 	whse.veg_comp_lyr_r1_poly_internal_2022_gr_skey vri_key ON vri_key.gr_skey = og.gr_skey
#     -- LEFT JOIN
#     -- 	whse.veg_comp_lyr_r1_poly_internal_2022 vri ON vri_key.pgid = vri.pgid;
# 	"""
# result1 <- dbGetQuery(conn, query)
# netdown_results <- result1


# query <- "SELECT
# 	'Protected and Prohibited' as land_classification
# 	,sum(
# 	CASE 
# 	 	WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited') OR harvest_lu.rollup_name IS NULL THEN 0
# 		--WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited', 'High Restricted') OR harvest_lu.rollup_name IS NULL THEN 0
# 	 	---- not in coast
# 	 	--WHEN coast.pgid IS NULL THEN
# 	 	--	CASE 
# 	 	--		WHEN slope_key.val > 50 THEN 0
# 	 	--		WHEN vri.site_index < 7 THEN 0
# 	 	--		ELSE og.tp_falb
# 	 	--	END
# 	 	--ELSE
# 	 	---- coast
# 	 	--	CASE 
# 	 	--		WHEN slope_key.val > 70 THEN 0
# 	 	--		WHEN vri.site_index < 12 then 0
# 	 	ELSE og.tp_falb
# 	 	--	END
# 	 END) as thlb_area
# FROM
# 	ogsr.og_prov_res og
# LEFT JOIN 
# 	whse.rr_restriction_gr_skey harvest_key USING (gr_skey)
# LEFT JOIN
# 	whse.rr_restriction harvest ON harvest.pgid = harvest_key.pgid
# LEFT JOIN
# 	whse.rr_restriction_three_zone harvest_lu ON harvest_lu.rr_restriction = harvest.rr_restriction
# -- LEFT JOIN
# -- 	whse.coast_forest_act_gr_skey coast_key ON coast_key.gr_skey = og.gr_skey
# -- LEFT JOIN
# -- 	whse.coast_forest_act coast ON coast.pgid = coast_key.pgid
# -- LEFT JOIN
# -- 	whse.slope_gr_skey slope_key ON og.gr_skey = slope_key.gr_skey
# -- LEFT JOIN
# -- 	whse.veg_comp_lyr_r1_poly_internal_2022_gr_skey vri_key ON vri_key.gr_skey = og.gr_skey
# -- LEFT JOIN
# -- 	whse.veg_comp_lyr_r1_poly_internal_2022 vri ON vri_key.pgid = vri.pgid;
# 	"
# result1 <- dbGetQuery(conn, query)
# netdown_results <- rbind(netdown_results, result1)


# query <- "SELECT
# 	'High Restricted (scaled by 0.15)' as land_classification
# 	,sum(
# 	CASE 
# 	 	WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited') OR harvest_lu.rollup_name IS NULL THEN 0
# 		WHEN harvest_lu.rollup_name IN ('High Restricted') THEN 0.15
# 	 	---- not in coast
# 	 	--WHEN coast.pgid IS NULL THEN
# 	 	--	CASE 
# 	 	--		WHEN slope_key.val > 50 THEN 0
# 	 	--		WHEN vri.site_index < 7 THEN 0
# 	 	--		ELSE og.tp_falb
# 	 	--	END
# 	 	--ELSE
# 	 	---- coast
# 	 	--	CASE 
# 	 	--		WHEN slope_key.val > 70 THEN 0
# 	 	--		WHEN vri.site_index < 12 then 0
# 	 	ELSE og.tp_falb
# 	 	--	END
# 	 END) as thlb_area
# FROM
# 	ogsr.og_prov_res og
# LEFT JOIN 
# 	whse.rr_restriction_gr_skey harvest_key USING (gr_skey)
# LEFT JOIN
# 	whse.rr_restriction harvest ON harvest.pgid = harvest_key.pgid
# LEFT JOIN
# 	whse.rr_restriction_three_zone harvest_lu ON harvest_lu.rr_restriction = harvest.rr_restriction
# -- LEFT JOIN
# -- 	whse.coast_forest_act_gr_skey coast_key ON coast_key.gr_skey = og.gr_skey
# -- LEFT JOIN
# -- 	whse.coast_forest_act coast ON coast.pgid = coast_key.pgid
# -- LEFT JOIN
# -- 	whse.slope_gr_skey slope_key ON og.gr_skey = slope_key.gr_skey
# -- LEFT JOIN
# -- 	whse.veg_comp_lyr_r1_poly_internal_2022_gr_skey vri_key ON vri_key.gr_skey = og.gr_skey
# -- LEFT JOIN
# -- 	whse.veg_comp_lyr_r1_poly_internal_2022 vri ON vri_key.pgid = vri.pgid;
# 	"
# result1 <- dbGetQuery(conn, query)
# netdown_results <- rbind(netdown_results, result1)


# query <- "SELECT
# 	'Slope > 50 interior, > 70 coast' as land_classification
# 	,sum(
# 	CASE 
# 	 	WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited') OR harvest_lu.rollup_name IS NULL THEN 0
# 		WHEN harvest_lu.rollup_name IN ('High Restricted') THEN 0.15	 	---- not in coast
# 	 	WHEN coast.pgid IS NULL THEN
# 		-- interior
# 	 		CASE 
# 	 			WHEN slope_key.val > 50 THEN 0
# 	 	--		WHEN vri.site_index < 7 THEN 0
# 	 			ELSE og.tp_falb
# 	 		END
# 	 	ELSE
# 	 	-- coast
# 	 		CASE 
# 	 			WHEN slope_key.val > 70 THEN 0
# 	 	--		WHEN vri.site_index < 12 then 0
# 	 			ELSE og.tp_falb
# 			END
# 		END
# 	 ) as thlb_area
# FROM
# 	ogsr.og_prov_res og
# LEFT JOIN 
# 	whse.rr_restriction_gr_skey harvest_key USING (gr_skey)
# LEFT JOIN
# 	whse.rr_restriction harvest ON harvest.pgid = harvest_key.pgid
# LEFT JOIN
# 	whse.rr_restriction_three_zone harvest_lu ON harvest_lu.rr_restriction = harvest.rr_restriction
#  LEFT JOIN
#  	whse.coast_forest_act_gr_skey coast_key ON coast_key.gr_skey = og.gr_skey
#  LEFT JOIN
#  	whse.coast_forest_act coast ON coast.pgid = coast_key.pgid
#  LEFT JOIN
#  	whse.slope_gr_skey slope_key ON og.gr_skey = slope_key.gr_skey
# -- LEFT JOIN
# -- 	whse.veg_comp_lyr_r1_poly_internal_2022_gr_skey vri_key ON vri_key.gr_skey = og.gr_skey
# -- LEFT JOIN
# -- 	whse.veg_comp_lyr_r1_poly_internal_2022 vri ON vri_key.pgid = vri.pgid;
# 	"
# result1 <- dbGetQuery(conn, query)
# netdown_results <- rbind(netdown_results, result1)


# query <- "SELECT
# 	'VRI site index < 7 interior, < 12 coast' as land_classification
# 	,sum(
# 	CASE 
# 	 	WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited') OR harvest_lu.rollup_name IS NULL THEN 0
# 		WHEN harvest_lu.rollup_name IN ('High Restricted') THEN 0.15	 	---- not in coast
# 	 	---- not in coast
# 	 	WHEN coast.pgid IS NULL THEN
# 	 		CASE 
# 	 			WHEN slope_key.val > 50 THEN 0
# 	 			WHEN vri.site_index < 7 THEN 0
# 	 			ELSE og.tp_falb
# 	 		END
# 	 	ELSE
# 	 	-- coast
# 	 		CASE 
# 	 			WHEN slope_key.val > 70 THEN 0
# 	 			WHEN vri.site_index < 12 then 0
# 	 			ELSE og.tp_falb
# 			END
# 		END
# 	 ) as thlb_area
# FROM
# 	ogsr.og_prov_res og
# LEFT JOIN 
# 	whse.rr_restriction_gr_skey harvest_key USING (gr_skey)
# LEFT JOIN
# 	whse.rr_restriction harvest ON harvest.pgid = harvest_key.pgid
# LEFT JOIN
# 	whse.rr_restriction_three_zone harvest_lu ON harvest_lu.rr_restriction = harvest.rr_restriction
#  LEFT JOIN
#  	whse.coast_forest_act_gr_skey coast_key ON coast_key.gr_skey = og.gr_skey
#  LEFT JOIN
#  	whse.coast_forest_act coast ON coast.pgid = coast_key.pgid
#  LEFT JOIN
#  	whse.slope_gr_skey slope_key ON og.gr_skey = slope_key.gr_skey
# LEFT JOIN
# 	whse.veg_comp_lyr_r1_poly_internal_2022_gr_skey vri_key ON vri_key.gr_skey = og.gr_skey
# LEFT JOIN
# 	whse.veg_comp_lyr_r1_poly_internal_2022 vri ON vri_key.pgid = vri.pgid;
# 	"
# result1 <- dbGetQuery(conn, query)
# netdown_results <- rbind(netdown_results, result1)


query <- "
WITH ws_no_cutblocks AS (
	SELECT
		ws.pgid
	FROM
		whse.fwa_assessment_ws_gr_skey ws_key
	LEFT JOIN
		whse.fwa_assessment_ws ws ON ws_key.pgid = ws.pgid
	LEFT JOIN
		whse.veg_consolidated_cut_blocks_sp_gr_skey cutblocks_key ON cutblocks_key.gr_skey = ws_key.gr_skey
	LEFT JOIN
		(SELECT pgid FROM whse.veg_consolidated_cut_blocks_sp WHERE harvest_year >= (SELECT EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '30 years'))) cutblocks ON cutblocks.pgid = cutblocks_key.pgid
	GROUP BY 
		ws.pgid
	HAVING 
		sum(case 
			when cutblocks.pgid is null then 0
			else 1
		end)::real/count(*)::real < 0.02
)
SELECT
	'Assessment watersheds with no cutblocks' as land_classification
	, sum(CASE 
		WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited') OR harvest_lu.rollup_name IS NULL THEN 0
		WHEN harvest_lu.rollup_name IN ('High Restricted') THEN 0.15
		-- interior
		WHEN coast.pgid IS NULL THEN
			CASE 
				WHEN slope_key.val > 50 THEN 0
				WHEN vri.site_index < 7 THEN 0
				WHEN ws_key.pgid IS NOT NULL THEN 0
				ELSE og.tp_falb
			END
		ELSE
		-- coast
			CASE 
				WHEN slope_key.val > 70 THEN 0
				WHEN vri.site_index < 12 then 0
				WHEN ws_key.pgid IS NOT NULL THEN 0
				ELSE og.tp_falb
			END
	END) as thlb_area
FROM
	ogsr.og_prov_res og
LEFT JOIN 
	whse.rr_restriction_gr_skey harvest_key USING (gr_skey)
LEFT JOIN
	whse.rr_restriction harvest ON harvest.pgid = harvest_key.pgid
LEFT JOIN
	whse.rr_restriction_three_zone harvest_lu ON harvest_lu.rr_restriction = harvest.rr_restriction
LEFT JOIN
	whse.coast_forest_act_gr_skey coast_key ON coast_key.gr_skey = og.gr_skey
LEFT JOIN
	whse.coast_forest_act coast ON coast.pgid = coast_key.pgid
LEFT JOIN
	whse.slope_gr_skey slope_key ON og.gr_skey = slope_key.gr_skey
LEFT JOIN
	whse.veg_comp_lyr_r1_poly_internal_2022_gr_skey vri_key ON vri_key.gr_skey = og.gr_skey
LEFT JOIN
	whse.veg_comp_lyr_r1_poly_internal_2022 vri ON vri_key.pgid = vri.pgid
LEFT JOIN
	whse.fwa_assessment_ws_gr_skey ws_key on ws_key.gr_skey = og.gr_skey
LEFT JOIN
	ws_no_cutblocks ON ws_key.pgid = ws_no_cutblocks.pgid
LEFT JOIN
	whse.all_bc_gr_skey geom ON geom.gr_skey = og.gr_skey;"
# result1 <- dbGetQuery(conn, query)
# netdown_results <- rbind(netdown_results, result1)
# today_date <- format(Sys.Date(), "%Y-%m-%d")
# netdown_results_csv <- glue("final/tables/thlb-proxy-netdown-round2-{today_date}.csv")
# write.csv(netdown_results, file = netdown_results_csv, row.names = FALSE)


print('Finished netdown queries, starting man unit query..')
print('Creating THLB..')

query <- "SELECT 
	CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL - ' || manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest - ' ||  manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot - ' || manlic.forest_file_id
		WHEN tfl.forest_file_id IS NOT Null 
			THEN tfl.forest_file_id
		ELSE mu_look.tsa_number_description
	END AS man_unit
	, CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL'
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest'
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot'
		WHEN tfl.forest_file_id IS NOT Null 
			THEN 'TFL'
		ELSE 'TSA'
	END AS tenure_type
	, sum(CASE 
		-- proxy THLB definition: 
		-- WHEN in community forests/woodlot/FNWL areas & you are not a protected/prohibited/high restrictive area (i.e. this IS legally loggable) 
		-- edge CASE: not given a harvest restriction value
		WHEN (LOWER(manlic.forest_file_id) LIKE 'n%%' OR LOWER(manlic.forest_file_id) LIKE 'w%%' OR LOWER(manlic.forest_file_id) LIKE 'k%%')
		  AND (three_zone.rollup_name IS NULL OR three_zone.rollup_name not in ('Protected', 'Prohibited', 'High Restricted')) 
		THEN fmlb.fmlb
		WHEN manlic.forest_file_id IS NULL THEN coalesce(thlb_fact, 0) * fmlb.fmlb -- tsa/tfl
		ELSE 0
		END) AS original_thlb_minus_protected_prohibited
from
whse.fmlb_current fmlb
LEFT JOIN whse.all_thlb_gr_skey thlbo USING (gr_skey)
LEFT JOIN whse.all_thlb allthlb USING (fid)
LEFT JOIN whse.tsa_boundaries_gr_skey tg ON tg.gr_skey = fmlb.gr_skey 
LEFT JOIN whse.tsa_boundaries tsa ON tsa.pgid = tg.pgid 
LEFT JOIN whse.fadm_tfl_all_sp_gr_skey tflg ON tflg.gr_skey = fmlb.gr_skey 
LEFT JOIN whse.fadm_tfl_all_sp tfl ON tfl.pgid = tflg.pgid
LEFT JOIN whse.ften_managed_licence_poly_svw_gr_skey manlico ON manlico.gr_skey = fmlb.gr_skey 
LEFT JOIN whse.ften_managed_licence_poly_svw manlic ON manlic.pgid = manlico.pgid 
LEFT JOIN whse.mu_lookup_table_im mu_look ON tsa.tsa = mu_look.tsa_number
LEFT JOIN whse.rr_restriction_gr_skey rrg ON fmlb.gr_skey = rrg.gr_skey
LEFT JOIN whse.rr_restriction rr ON rrg.pgid = rr.pgid
LEFT JOIN whse.rr_restriction_three_zone three_zone USING (rr_restriction) 
GROUP BY 
	CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL - ' || manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest - ' ||  manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot - ' || manlic.forest_file_id
		WHEN tfl.forest_file_id IS NOT Null 
			THEN tfl.forest_file_id
		ELSE mu_look.tsa_number_description
	END
	, CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL'
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest'
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot'
		WHEN tfl.forest_file_id IS NOT Null 
			THEN 'TFL'
		ELSE 'TSA'
	END"
results1 <- dbGetQuery(conn, query)
man_unit_results <- results1

print('Creating THLB with protected & prohibited removed..')

query <- "SELECT 
	CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL - ' || manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest - ' ||  manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot - ' || manlic.forest_file_id
		WHEN tfl.forest_file_id IS NOT Null 
			THEN tfl.forest_file_id
		ELSE mu_look.tsa_number_description
	END AS man_unit
	, CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL'
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest'
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot'
		WHEN tfl.forest_file_id IS NOT Null 
			THEN 'TFL'
		ELSE 'TSA'
	END AS tenure_type
	, sum(CASE 
	    WHEN three_zone.rollup_name in ('Protected', 'Prohibited') THEN 0
		-- proxy THLB definition: 
		-- WHEN in community forests/woodlot/FNWL areas & you are not a protected/prohibited/high restrictive area (i.e. this IS legally loggable) 
		-- edge CASE: not given a harvest restriction value
		WHEN (LOWER(manlic.forest_file_id) LIKE 'n%%' OR LOWER(manlic.forest_file_id) LIKE 'w%%' OR LOWER(manlic.forest_file_id) LIKE 'k%%')
		  AND (three_zone.rollup_name IS NULL OR three_zone.rollup_name not in ('Protected', 'Prohibited', 'High Restricted')) 
		THEN fmlb.fmlb
		WHEN manlic.forest_file_id IS NULL THEN coalesce(thlb_fact, 0) * fmlb.fmlb -- tsa/tfl
		ELSE 0
		END) AS original_thlb_minus_protected_prohibited
from
whse.fmlb_current fmlb
LEFT JOIN whse.all_thlb_gr_skey thlbo USING (gr_skey)
LEFT JOIN whse.all_thlb allthlb USING (fid)
LEFT JOIN whse.tsa_boundaries_gr_skey tg ON tg.gr_skey = fmlb.gr_skey 
LEFT JOIN whse.tsa_boundaries tsa ON tsa.pgid = tg.pgid 
LEFT JOIN whse.fadm_tfl_all_sp_gr_skey tflg ON tflg.gr_skey = fmlb.gr_skey 
LEFT JOIN whse.fadm_tfl_all_sp tfl ON tfl.pgid = tflg.pgid
LEFT JOIN whse.ften_managed_licence_poly_svw_gr_skey manlico ON manlico.gr_skey = fmlb.gr_skey 
LEFT JOIN whse.ften_managed_licence_poly_svw manlic ON manlic.pgid = manlico.pgid 
LEFT JOIN whse.mu_lookup_table_im mu_look ON tsa.tsa = mu_look.tsa_number
LEFT JOIN whse.rr_restriction_gr_skey rrg ON fmlb.gr_skey = rrg.gr_skey
LEFT JOIN whse.rr_restriction rr ON rrg.pgid = rr.pgid
LEFT JOIN whse.rr_restriction_three_zone three_zone USING (rr_restriction) 
GROUP BY 
	CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL - ' || manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest - ' ||  manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot - ' || manlic.forest_file_id
		WHEN tfl.forest_file_id IS NOT Null 
			THEN tfl.forest_file_id
		ELSE mu_look.tsa_number_description
	END
	, CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL'
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest'
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot'
		WHEN tfl.forest_file_id IS NOT Null 
			THEN 'TFL'
		ELSE 'TSA'
	END"
results1 <- dbGetQuery(conn, query)
man_unit_results <- cbind(man_unit_results, results1[,3])
num_cols <- length(names(man_unit_results))
names(man_unit_results)[num_cols] <- names(results1)[3]

print('Creating THLB with protected & prohibited & high restricted removed..')

query <- "SELECT 
	CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL - ' || manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest - ' ||  manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot - ' || manlic.forest_file_id
		WHEN tfl.forest_file_id IS NOT Null 
			THEN tfl.forest_file_id
		ELSE mu_look.tsa_number_description
	END AS man_unit
	, CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL'
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest'
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot'
		WHEN tfl.forest_file_id IS NOT Null 
			THEN 'TFL'
		ELSE 'TSA'
	END AS tenure_type
	, sum(CASE 
	    WHEN three_zone.rollup_name in ('Protected', 'Prohibited', 'High Restricted') THEN 0
		-- proxy THLB definition: 
		-- WHEN in community forests/woodlot/FNWL areas & you are not a protected/prohibited/high restrictive area (i.e. this IS legally loggable) 
		-- edge CASE: not given a harvest restriction value
		WHEN (LOWER(manlic.forest_file_id) LIKE 'n%%' OR LOWER(manlic.forest_file_id) LIKE 'w%%' OR LOWER(manlic.forest_file_id) LIKE 'k%%')
		  AND (three_zone.rollup_name IS NULL OR three_zone.rollup_name not in ('Protected', 'Prohibited', 'High Restricted')) 
		THEN fmlb.fmlb
		WHEN manlic.forest_file_id IS NULL THEN coalesce(thlb_fact, 0) * fmlb.fmlb -- tsa/tfl
		ELSE 0
		END) AS original_thlb_minus_protected_prohibited_high_res
from
whse.fmlb_current fmlb
LEFT JOIN whse.all_thlb_gr_skey thlbo USING (gr_skey)
LEFT JOIN whse.all_thlb allthlb USING (fid)
LEFT JOIN whse.tsa_boundaries_gr_skey tg ON tg.gr_skey = fmlb.gr_skey 
LEFT JOIN whse.tsa_boundaries tsa ON tsa.pgid = tg.pgid 
LEFT JOIN whse.fadm_tfl_all_sp_gr_skey tflg ON tflg.gr_skey = fmlb.gr_skey 
LEFT JOIN whse.fadm_tfl_all_sp tfl ON tfl.pgid = tflg.pgid
LEFT JOIN whse.ften_managed_licence_poly_svw_gr_skey manlico ON manlico.gr_skey = fmlb.gr_skey 
LEFT JOIN whse.ften_managed_licence_poly_svw manlic ON manlic.pgid = manlico.pgid 
LEFT JOIN whse.mu_lookup_table_im mu_look ON tsa.tsa = mu_look.tsa_number
LEFT JOIN whse.rr_restriction_gr_skey rrg ON fmlb.gr_skey = rrg.gr_skey
LEFT JOIN whse.rr_restriction rr ON rrg.pgid = rr.pgid
LEFT JOIN whse.rr_restriction_three_zone three_zone USING (rr_restriction) 
GROUP BY 
	CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL - ' || manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest - ' ||  manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot - ' || manlic.forest_file_id
		WHEN tfl.forest_file_id IS NOT Null 
			THEN tfl.forest_file_id
		ELSE mu_look.tsa_number_description
	END
	, CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL'
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest'
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot'
		WHEN tfl.forest_file_id IS NOT Null 
			THEN 'TFL'
		ELSE 'TSA'
	END"
results1 <- dbGetQuery(conn, query)
man_unit_results <- cbind(man_unit_results, results1[,3])
num_cols <- length(names(man_unit_results))
names(man_unit_results)[num_cols] <- names(results1)[3]

print('Creating THLB with protected & prohibited & high restricted * 0.15 removed..')

query <- "SELECT 
	CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL - ' || manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest - ' ||  manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot - ' || manlic.forest_file_id
		WHEN tfl.forest_file_id IS NOT Null 
			THEN tfl.forest_file_id
		ELSE mu_look.tsa_number_description
	END AS man_unit
	, CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL'
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest'
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot'
		WHEN tfl.forest_file_id IS NOT Null 
			THEN 'TFL'
		ELSE 'TSA'
	END AS tenure_type
	, sum(CASE 
	    WHEN three_zone.rollup_name in ('Protected', 'Prohibited') THEN 0
		WHEN three_zone.rollup_name in ('High Restricted') THEN 0.15
		-- proxy THLB definition: 
		-- WHEN in community forests/woodlot/FNWL areas & you are not a protected/prohibited/high restrictive area (i.e. this IS legally loggable) 
		-- edge CASE: not given a harvest restriction value
		WHEN (LOWER(manlic.forest_file_id) LIKE 'n%%' OR LOWER(manlic.forest_file_id) LIKE 'w%%' OR LOWER(manlic.forest_file_id) LIKE 'k%%')
		  AND (three_zone.rollup_name IS NULL OR three_zone.rollup_name not in ('Protected', 'Prohibited', 'High Restricted')) 
		THEN fmlb.fmlb
		WHEN manlic.forest_file_id IS NULL THEN coalesce(thlb_fact, 0) * fmlb.fmlb -- tsa/tfl
		ELSE 0
		END) AS original_thlb_minus_protected_prohibited_high_res_15
from
whse.fmlb_current fmlb
LEFT JOIN whse.all_thlb_gr_skey thlbo USING (gr_skey)
LEFT JOIN whse.all_thlb allthlb USING (fid)
LEFT JOIN whse.tsa_boundaries_gr_skey tg ON tg.gr_skey = fmlb.gr_skey 
LEFT JOIN whse.tsa_boundaries tsa ON tsa.pgid = tg.pgid 
LEFT JOIN whse.fadm_tfl_all_sp_gr_skey tflg ON tflg.gr_skey = fmlb.gr_skey 
LEFT JOIN whse.fadm_tfl_all_sp tfl ON tfl.pgid = tflg.pgid
LEFT JOIN whse.ften_managed_licence_poly_svw_gr_skey manlico ON manlico.gr_skey = fmlb.gr_skey 
LEFT JOIN whse.ften_managed_licence_poly_svw manlic ON manlic.pgid = manlico.pgid 
LEFT JOIN whse.mu_lookup_table_im mu_look ON tsa.tsa = mu_look.tsa_number
LEFT JOIN whse.rr_restriction_gr_skey rrg ON fmlb.gr_skey = rrg.gr_skey
LEFT JOIN whse.rr_restriction rr ON rrg.pgid = rr.pgid
LEFT JOIN whse.rr_restriction_three_zone three_zone USING (rr_restriction) 
GROUP BY 
	CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL - ' || manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest - ' ||  manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot - ' || manlic.forest_file_id
		WHEN tfl.forest_file_id IS NOT Null 
			THEN tfl.forest_file_id
		ELSE mu_look.tsa_number_description
	END
	, CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL'
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest'
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot'
		WHEN tfl.forest_file_id IS NOT Null 
			THEN 'TFL'
		ELSE 'TSA'
	END"
results1 <- dbGetQuery(conn, query)
man_unit_results <- cbind(man_unit_results, results1[,3])
num_cols <- length(names(man_unit_results))
names(man_unit_results)[num_cols] <- names(results1)[3]


print('Creating THLB proxy with protected & prohibited & high restricted removed..')
query <- "
WITH thlb_proxy AS (
SELECT
	og.gr_skey
	, CASE 
	 	WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited') OR harvest_lu.rollup_name IS NULL THEN 0
		WHEN harvest_lu.rollup_name IN ('High Restricted') THEN 0.15
	 	---- not in coast
	 	--WHEN coast.pgid IS NULL THEN
	 	--	CASE 
	 	-- 	WHEN slope_key.val > 50 THEN 0
	 	--		--WHEN vri.site_index < 7 THEN 0
	 	--		ELSE og.tp_falb
	 	--	END
	 	--ELSE
	 	-- coast
	 	--	CASE 
	 	--		WHEN slope_key.val > 70 THEN 0
	 			--WHEN vri.site_index < 12 then 0
	 			ELSE og.tp_falb
		--	END
		END
	 as thlb
FROM
	ogsr.og_prov_res og
LEFT JOIN 
	whse.rr_restriction_gr_skey harvest_key USING (gr_skey)
LEFT JOIN
	whse.rr_restriction harvest ON harvest.pgid = harvest_key.pgid
LEFT JOIN
	whse.rr_restriction_three_zone harvest_lu ON harvest_lu.rr_restriction = harvest.rr_restriction
-- LEFT JOIN
-- 	whse.coast_forest_act_gr_skey coast_key ON coast_key.gr_skey = og.gr_skey
-- LEFT JOIN
-- 	whse.coast_forest_act coast ON coast.pgid = coast_key.pgid
-- LEFT JOIN
-- 	whse.slope_gr_skey slope_key ON og.gr_skey = slope_key.gr_skey
-- LEFT JOIN
-- 	whse.veg_comp_lyr_r1_poly_internal_2022_gr_skey vri_key ON vri_key.gr_skey = og.gr_skey
-- LEFT JOIN
--	whse.veg_comp_lyr_r1_poly_internal_2022 vri ON vri_key.pgid = vri.pgid
)
SELECT 
	CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL - ' || manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest - ' ||  manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot - ' || manlic.forest_file_id
		WHEN tfl.forest_file_id IS NOT Null 
			THEN tfl.forest_file_id
		ELSE mu_look.tsa_number_description
	END AS man_unit
	, CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL'
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest'
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot'
		WHEN tfl.forest_file_id IS NOT Null 
			THEN 'TFL'
		ELSE 'TSA'
	END AS tenure_type
	, sum(CASE 
		-- proxy THLB definition: 
		-- WHEN in community forests/woodlot/FNWL areas & you are not a protected/prohibited/high restrictive area (i.e. this IS legally loggable) 
		-- edge CASE: not given a harvest restriction value
		WHEN (LOWER(manlic.forest_file_id) LIKE 'n%%' OR LOWER(manlic.forest_file_id) LIKE 'w%%' OR LOWER(manlic.forest_file_id) LIKE 'k%%')
		  AND (three_zone.rollup_name IS NULL OR three_zone.rollup_name not in ('Protected', 'Prohibited', 'High Restricted')) 
		THEN thlb.thlb
		WHEN manlic.forest_file_id IS NULL THEN thlb -- tsa/tfl
		ELSE 0
		END) AS thlb_proxy_protected_prohibited_high_restr_15
from
thlb_proxy thlb
LEFT JOIN whse.tsa_boundaries_gr_skey tg ON tg.gr_skey = thlb.gr_skey 
LEFT JOIN whse.tsa_boundaries tsa ON tsa.pgid = tg.pgid 
LEFT JOIN whse.fadm_tfl_all_sp_gr_skey tflg ON tflg.gr_skey = thlb.gr_skey 
LEFT JOIN whse.fadm_tfl_all_sp tfl ON tfl.pgid = tflg.pgid
LEFT JOIN whse.ften_managed_licence_poly_svw_gr_skey manlico ON manlico.gr_skey = thlb.gr_skey 
LEFT JOIN whse.ften_managed_licence_poly_svw manlic ON manlic.pgid = manlico.pgid 
LEFT JOIN whse.mu_lookup_table_im mu_look ON tsa.tsa = mu_look.tsa_number
LEFT JOIN whse.rr_restriction_gr_skey rrg ON thlb.gr_skey = rrg.gr_skey
LEFT JOIN whse.rr_restriction rr ON rrg.pgid = rr.pgid
LEFT JOIN whse.rr_restriction_three_zone three_zone USING (rr_restriction)
GROUP BY 
	CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL - ' || manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest - ' ||  manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot - ' || manlic.forest_file_id
		WHEN tfl.forest_file_id IS NOT Null 
			THEN tfl.forest_file_id
		ELSE mu_look.tsa_number_description
	END
	, CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL'
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest'
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot'
		WHEN tfl.forest_file_id IS NOT Null 
			THEN 'TFL'
		ELSE 'TSA'
	END"

results1 <- dbGetQuery(conn, query)
man_unit_results <- cbind(man_unit_results, results1[,3])
num_cols <- length(names(man_unit_results))
names(man_unit_results)[num_cols] <- names(results1)[3]

print('Creating THLB proxy with slope > 50 interior / slope > 70 coast removed..')

query <- "
WITH thlb_proxy AS (
SELECT
	og.gr_skey
	, CASE 
	 	WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited') OR harvest_lu.rollup_name IS NULL THEN 0
		WHEN harvest_lu.rollup_name IN ('High Restricted') THEN 0.15
	 	---- not in coast
	 	WHEN coast.pgid IS NULL THEN
	 		CASE 
	 	 	WHEN slope_key.val > 50 THEN 0
	 	--		--WHEN vri.site_index < 7 THEN 0
	 			ELSE og.tp_falb
	 		END
	 	ELSE
	 	-- coast
	 		CASE 
	 			WHEN slope_key.val > 70 THEN 0
	 			--WHEN vri.site_index < 12 then 0
	 			ELSE og.tp_falb
			END
		END
	 as thlb
FROM
	ogsr.og_prov_res og
LEFT JOIN 
	whse.rr_restriction_gr_skey harvest_key USING (gr_skey)
LEFT JOIN
	whse.rr_restriction harvest ON harvest.pgid = harvest_key.pgid
LEFT JOIN
	whse.rr_restriction_three_zone harvest_lu ON harvest_lu.rr_restriction = harvest.rr_restriction
 LEFT JOIN
 	whse.coast_forest_act_gr_skey coast_key ON coast_key.gr_skey = og.gr_skey
 LEFT JOIN
 	whse.coast_forest_act coast ON coast.pgid = coast_key.pgid
 LEFT JOIN
 	whse.slope_gr_skey slope_key ON og.gr_skey = slope_key.gr_skey
-- LEFT JOIN
-- 	whse.veg_comp_lyr_r1_poly_internal_2022_gr_skey vri_key ON vri_key.gr_skey = og.gr_skey
-- LEFT JOIN
--	whse.veg_comp_lyr_r1_poly_internal_2022 vri ON vri_key.pgid = vri.pgid
)
SELECT 
	CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL - ' || manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest - ' ||  manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot - ' || manlic.forest_file_id
		WHEN tfl.forest_file_id IS NOT Null 
			THEN tfl.forest_file_id
		ELSE mu_look.tsa_number_description
	END AS man_unit
	, CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL'
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest'
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot'
		WHEN tfl.forest_file_id IS NOT Null 
			THEN 'TFL'
		ELSE 'TSA'
	END AS tenure_type
	, sum(CASE 
		-- proxy THLB definition: 
		-- WHEN in community forests/woodlot/FNWL areas & you are not a protected/prohibited/high restrictive area (i.e. this IS legally loggable) 
		-- edge CASE: not given a harvest restriction value
		WHEN (LOWER(manlic.forest_file_id) LIKE 'n%%' OR LOWER(manlic.forest_file_id) LIKE 'w%%' OR LOWER(manlic.forest_file_id) LIKE 'k%%')
		  AND (three_zone.rollup_name IS NULL OR three_zone.rollup_name not in ('Protected', 'Prohibited', 'High Restricted')) 
		THEN thlb.thlb
		WHEN manlic.forest_file_id IS NULL THEN thlb -- tsa/tfl
		ELSE 0
		END) AS thlb_proxy_slope
from
thlb_proxy thlb
LEFT JOIN whse.tsa_boundaries_gr_skey tg ON tg.gr_skey = thlb.gr_skey 
LEFT JOIN whse.tsa_boundaries tsa ON tsa.pgid = tg.pgid 
LEFT JOIN whse.fadm_tfl_all_sp_gr_skey tflg ON tflg.gr_skey = thlb.gr_skey 
LEFT JOIN whse.fadm_tfl_all_sp tfl ON tfl.pgid = tflg.pgid
LEFT JOIN whse.ften_managed_licence_poly_svw_gr_skey manlico ON manlico.gr_skey = thlb.gr_skey 
LEFT JOIN whse.ften_managed_licence_poly_svw manlic ON manlic.pgid = manlico.pgid 
LEFT JOIN whse.mu_lookup_table_im mu_look ON tsa.tsa = mu_look.tsa_number
LEFT JOIN whse.rr_restriction_gr_skey rrg ON thlb.gr_skey = rrg.gr_skey
LEFT JOIN whse.rr_restriction rr ON rrg.pgid = rr.pgid
LEFT JOIN whse.rr_restriction_three_zone three_zone USING (rr_restriction)
GROUP BY 
	CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL - ' || manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest - ' ||  manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot - ' || manlic.forest_file_id
		WHEN tfl.forest_file_id IS NOT Null 
			THEN tfl.forest_file_id
		ELSE mu_look.tsa_number_description
	END
	, CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL'
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest'
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot'
		WHEN tfl.forest_file_id IS NOT Null 
			THEN 'TFL'
		ELSE 'TSA'
	END"

results1 <- dbGetQuery(conn, query)
man_unit_results <- cbind(man_unit_results, results1[,3])
num_cols <- length(names(man_unit_results))
names(man_unit_results)[num_cols] <- names(results1)[3]


print('Creating THLB proxy with vri site index < 7 interior / site index < 12 coast removed..')

query <- "
WITH thlb_proxy AS (
SELECT
	og.gr_skey
	, CASE 
	 	WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited') OR harvest_lu.rollup_name IS NULL THEN 0
		WHEN harvest_lu.rollup_name IN ('High Restricted') THEN 0.15
	 	---- not in coast
	 	WHEN coast.pgid IS NULL THEN
	 		CASE 
	 	 	    WHEN slope_key.val > 50 THEN 0
	 		    WHEN vri.site_index < 7 THEN 0
	 		    ELSE og.tp_falb
	 		END
	 	ELSE
	 	-- coast
	 		CASE 
	 			WHEN slope_key.val > 70 THEN 0
	 			WHEN vri.site_index < 12 then 0
	 			ELSE og.tp_falb
			END
		END
	 as thlb
FROM
	ogsr.og_prov_res og
LEFT JOIN 
	whse.rr_restriction_gr_skey harvest_key USING (gr_skey)
LEFT JOIN
	whse.rr_restriction harvest ON harvest.pgid = harvest_key.pgid
LEFT JOIN
	whse.rr_restriction_three_zone harvest_lu ON harvest_lu.rr_restriction = harvest.rr_restriction
 LEFT JOIN
 	whse.coast_forest_act_gr_skey coast_key ON coast_key.gr_skey = og.gr_skey
 LEFT JOIN
 	whse.coast_forest_act coast ON coast.pgid = coast_key.pgid
 LEFT JOIN
 	whse.slope_gr_skey slope_key ON og.gr_skey = slope_key.gr_skey
 LEFT JOIN
 	whse.veg_comp_lyr_r1_poly_internal_2022_gr_skey vri_key ON vri_key.gr_skey = og.gr_skey
 LEFT JOIN
	whse.veg_comp_lyr_r1_poly_internal_2022 vri ON vri_key.pgid = vri.pgid
)
SELECT 
	CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL - ' || manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest - ' ||  manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot - ' || manlic.forest_file_id
		WHEN tfl.forest_file_id IS NOT Null 
			THEN tfl.forest_file_id
		ELSE mu_look.tsa_number_description
	END AS man_unit
	, CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL'
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest'
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot'
		WHEN tfl.forest_file_id IS NOT Null 
			THEN 'TFL'
		ELSE 'TSA'
	END AS tenure_type
	, sum(CASE 
		-- proxy THLB definition: 
		-- WHEN in community forests/woodlot/FNWL areas & you are not a protected/prohibited/high restrictive area (i.e. this IS legally loggable) 
		-- edge CASE: not given a harvest restriction value
		WHEN (LOWER(manlic.forest_file_id) LIKE 'n%%' OR LOWER(manlic.forest_file_id) LIKE 'w%%' OR LOWER(manlic.forest_file_id) LIKE 'k%%')
		  AND (three_zone.rollup_name IS NULL OR three_zone.rollup_name not in ('Protected', 'Prohibited', 'High Restricted')) 
		THEN thlb.thlb
		WHEN manlic.forest_file_id IS NULL THEN thlb -- tsa/tfl
		ELSE 0
		END) AS thlb_proxy_vri
from
thlb_proxy thlb
LEFT JOIN whse.tsa_boundaries_gr_skey tg ON tg.gr_skey = thlb.gr_skey 
LEFT JOIN whse.tsa_boundaries tsa ON tsa.pgid = tg.pgid 
LEFT JOIN whse.fadm_tfl_all_sp_gr_skey tflg ON tflg.gr_skey = thlb.gr_skey 
LEFT JOIN whse.fadm_tfl_all_sp tfl ON tfl.pgid = tflg.pgid
LEFT JOIN whse.ften_managed_licence_poly_svw_gr_skey manlico ON manlico.gr_skey = thlb.gr_skey 
LEFT JOIN whse.ften_managed_licence_poly_svw manlic ON manlic.pgid = manlico.pgid 
LEFT JOIN whse.mu_lookup_table_im mu_look ON tsa.tsa = mu_look.tsa_number
LEFT JOIN whse.rr_restriction_gr_skey rrg ON thlb.gr_skey = rrg.gr_skey
LEFT JOIN whse.rr_restriction rr ON rrg.pgid = rr.pgid
LEFT JOIN whse.rr_restriction_three_zone three_zone USING (rr_restriction)
GROUP BY 
	CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL - ' || manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest - ' ||  manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot - ' || manlic.forest_file_id
		WHEN tfl.forest_file_id IS NOT Null 
			THEN tfl.forest_file_id
		ELSE mu_look.tsa_number_description
	END
	, CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL'
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest'
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot'
		WHEN tfl.forest_file_id IS NOT Null 
			THEN 'TFL'
		ELSE 'TSA'
	END"

results1 <- dbGetQuery(conn, query)
man_unit_results <- cbind(man_unit_results, results1[,3])
num_cols <- length(names(man_unit_results))
names(man_unit_results)[num_cols] <- names(results1)[3]

print('Creating THLB proxy with assessment watersheds with < 2% cutblocks removed..')

query <- "WITH ws_no_cutblocks AS (
	SELECT
		ws.pgid
	FROM
		whse.fwa_assessment_ws_gr_skey ws_key
	LEFT JOIN
		whse.fwa_assessment_ws ws ON ws_key.pgid = ws.pgid
	LEFT JOIN
		whse.veg_consolidated_cut_blocks_sp_gr_skey cutblocks_key ON cutblocks_key.gr_skey = ws_key.gr_skey
	LEFT JOIN
		(SELECT pgid FROM whse.veg_consolidated_cut_blocks_sp WHERE harvest_year >= (SELECT EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '30 years'))) cutblocks ON cutblocks.pgid = cutblocks_key.pgid
	GROUP BY 
		ws.pgid
	HAVING 
		sum(case 
			when cutblocks.pgid is null then 0
			else 1
		end)::real/count(*)::real < 0.02
), thlb_proxy AS (
SELECT
	og.gr_skey,
	CASE 
		WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited') OR harvest_lu.rollup_name IS NULL THEN 0
		WHEN harvest_lu.rollup_name IN ('High Restricted') THEN 0.15
		-- interior
		WHEN coast.pgid IS NULL THEN
			CASE 
				WHEN slope_key.val > 50 THEN 0
				WHEN vri.site_index < 7 THEN 0
				WHEN ws_key.pgid IS NOT NULL THEN 0
				ELSE og.tp_falb
			END
		ELSE
		-- coast
			CASE 
				WHEN slope_key.val > 70 THEN 0
				WHEN vri.site_index < 12 then 0
				WHEN ws_key.pgid IS NOT NULL THEN 0
				ELSE og.tp_falb
			END
	END as thlb
	-- CASE 
	-- 	WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited') OR harvest_lu.rollup_name IS NULL THEN 'protected/prohibited'
	-- 	WHEN harvest_lu.rollup_name IN ('High Restricted') THEN 'high restricted'
	-- 	-- interior
	-- 	WHEN coast.pgid IS NULL THEN
	-- 		CASE 
	-- 			WHEN slope_key.val > 50 THEN 'interior slope > 50'
	-- 			WHEN vri.site_index < 7 THEN 'interior site index < 7'
	-- 			WHEN ws_key.pgid IS NOT NULL THEN 'interior ws no cutblock'
	-- 			ELSE 'falb'
	-- 		END
	-- 	ELSE
	-- 	-- coast
	-- 		CASE 
	-- 			WHEN slope_key.val > 70 THEN 'coast slope > 70'
	-- 			WHEN vri.site_index < 12 then 'coast site index < 12'
	-- 			WHEN ws_key.pgid IS NOT NULL THEN 'coast ws no cutblock'
	-- 			ELSE  'falb'
	-- 		END
	-- END as data_source
FROM
	ogsr.og_prov_res og
LEFT JOIN 
	whse.rr_restriction_gr_skey harvest_key USING (gr_skey)
LEFT JOIN
	whse.rr_restriction harvest ON harvest.pgid = harvest_key.pgid
LEFT JOIN
	whse.rr_restriction_three_zone harvest_lu ON harvest_lu.rr_restriction = harvest.rr_restriction
LEFT JOIN
	whse.coast_forest_act_gr_skey coast_key ON coast_key.gr_skey = og.gr_skey
LEFT JOIN
	whse.coast_forest_act coast ON coast.pgid = coast_key.pgid
LEFT JOIN
	whse.slope_gr_skey slope_key ON og.gr_skey = slope_key.gr_skey
LEFT JOIN
	whse.veg_comp_lyr_r1_poly_internal_2022_gr_skey vri_key ON vri_key.gr_skey = og.gr_skey
LEFT JOIN
	whse.veg_comp_lyr_r1_poly_internal_2022 vri ON vri_key.pgid = vri.pgid
LEFT JOIN
	whse.fwa_assessment_ws_gr_skey ws_key on ws_key.gr_skey = og.gr_skey
LEFT JOIN
	ws_no_cutblocks ON ws_key.pgid = ws_no_cutblocks.pgid
) 	
SELECT 
	CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL - ' || manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest - ' ||  manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot - ' || manlic.forest_file_id
		WHEN tfl.forest_file_id IS NOT Null 
			THEN tfl.forest_file_id
		ELSE mu_look.tsa_number_description
	END AS man_unit
	, CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL'
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest'
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot'
		WHEN tfl.forest_file_id IS NOT Null 
			THEN 'TFL'
		ELSE 'TSA'
	END AS tenure_type
	, sum(CASE 
		-- proxy THLB definition: 
		-- WHEN in community forests/woodlot/FNWL areas & you are not a protected/prohibited/high restrictive area (i.e. this IS legally loggable) 
		-- edge CASE: not given a harvest restriction value
		WHEN (LOWER(manlic.forest_file_id) LIKE 'n%%' OR LOWER(manlic.forest_file_id) LIKE 'w%%' OR LOWER(manlic.forest_file_id) LIKE 'k%%')
		  AND (three_zone.rollup_name IS NULL OR three_zone.rollup_name not in ('Protected', 'Prohibited', 'High Restricted')) 
		THEN thlb.thlb
		WHEN manlic.forest_file_id IS NULL THEN thlb -- tsa/tfl
		ELSE 0
		END) AS thlb_proxy_no_cutblock_ws
from
thlb_proxy thlb
LEFT JOIN whse.tsa_boundaries_gr_skey tg ON tg.gr_skey = thlb.gr_skey 
LEFT JOIN whse.tsa_boundaries tsa ON tsa.pgid = tg.pgid 
LEFT JOIN whse.fadm_tfl_all_sp_gr_skey tflg ON tflg.gr_skey = thlb.gr_skey 
LEFT JOIN whse.fadm_tfl_all_sp tfl ON tfl.pgid = tflg.pgid
LEFT JOIN whse.ften_managed_licence_poly_svw_gr_skey manlico ON manlico.gr_skey = thlb.gr_skey 
LEFT JOIN whse.ften_managed_licence_poly_svw manlic ON manlic.pgid = manlico.pgid 
LEFT JOIN whse.mu_lookup_table_im mu_look ON tsa.tsa = mu_look.tsa_number
LEFT JOIN whse.rr_restriction_gr_skey rrg ON thlb.gr_skey = rrg.gr_skey
LEFT JOIN whse.rr_restriction rr ON rrg.pgid = rr.pgid
LEFT JOIN whse.rr_restriction_three_zone three_zone USING (rr_restriction)
GROUP BY 
	CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL - ' || manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest - ' ||  manlic.forest_file_id
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot - ' || manlic.forest_file_id
		WHEN tfl.forest_file_id IS NOT Null 
			THEN tfl.forest_file_id
		ELSE mu_look.tsa_number_description
	END
	, CASE 
		WHEN LOWER(manlic.forest_file_id) LIKE 'n%%'
			THEN 'FNWL'
		WHEN LOWER(manlic.forest_file_id) LIKE 'k%%'
			THEN 'Community Forest'
		WHEN LOWER(manlic.forest_file_id) LIKE 'w%%'
			 THEN 'Woodlot'
		WHEN tfl.forest_file_id IS NOT Null 
			THEN 'TFL'
		ELSE 'TSA'
	END"

results1 <- dbGetQuery(conn, query)
man_unit_results <- cbind(man_unit_results, results1[,3])
num_cols <- length(names(man_unit_results))
names(man_unit_results)[num_cols] <- names(results1)[3]

today_date <- format(Sys.Date(), "%Y-%m-%d")
tsa_results <- man_unit_results[which(man_unit_results$tenure_type == 'TSA'),]
tsa_man_unit_results_csv <- glue("final/tables/thlb-proxy-tsa-round2-{today_date}.csv")
write.csv(tsa_results, file = tsa_man_unit_results_csv, row.names = FALSE)

tfl_results <- man_unit_results[which(man_unit_results$tenure_type == 'TFL'),]
tfl_man_unit_results_csv <- glue("final/tables/thlb-proxy-tfl-round2-{today_date}.csv")
write.csv(tfl_results, file = tfl_man_unit_results_csv, row.names = FALSE)
end_time <- Sys.time()
duration <- difftime(end_time, start_time, units = "mins")
print(glue("Script started at {format(end_time, '%Y-%m-%d %I:%M:%S %p')}"))
print(glue("Script duration: {duration} minutes\n"))