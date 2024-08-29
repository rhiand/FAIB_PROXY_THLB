library(RPostgres)
library(glue)
library(dadmtools)

## relies on install_github("bcgov/FAIB_DATA_MANAGEMENT") being installed at some point
conn_list <- dadmtools::get_pg_conn_list()
## relies on the keyring connList being populated
conn <- DBI::dbConnect(conn_list["driver"][[1]],
				host = conn_list["host"][[1]],
				user = conn_list["user"][[1]],
				dbname = conn_list["dbname"][[1]],
				password = conn_list["password"][[1]],
				port = conn_list["port"][[1]])

query <- "SELECT
	'FALB' as land_classification
	,sum(og.tp_falb) as thlb_area
	-- , CASE 
	-- 	WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited', 'High Restricted') OR harvest_lu.rollup_name IS NULL THEN 0
	-- 	-- not in coast
	-- 	WHEN coast.pgid IS NULL THEN
	-- 		CASE 
	-- 			WHEN slope_key.val > 40 THEN 0
	-- 			WHEN vri.site_index < 8 THEN 0
	-- 			ELSE og.tp_falb
	-- 		END
	-- 	ELSE
	-- 	-- coast
	-- 		CASE 
	-- 			WHEN slope_key.val > 60 THEN 0
	-- 			WHEN vri.site_index < 15 then 0
	-- 			ELSE og.tp_falb
	-- 		END
	-- END as thlb
FROM
	ogsr.og_prov_res og
-- LEFT JOIN 
-- 	whse.rr_restriction_gr_skey harvest_key USING (gr_skey)
-- LEFT JOIN
-- 	whse.rr_restriction harvest ON harvest.pgid = harvest_key.pgid
-- LEFT JOIN
-- 	whse.rr_restriction_three_zone harvest_lu ON harvest_lu.rr_restriction = harvest.rr_restriction
-- LEFT JOIN
-- 	whse.coast_forest_act_gr_skey coast_key ON coast_key.gr_skey = og.gr_skey
-- LEFT JOIN
-- 	whse.coast_forest_act coast ON coast.pgid = coast_key.pgid
-- LEFT JOIN
-- 	whse.slope_gr_skey slope_key ON og.gr_skey = slope_key.gr_skey
-- LEFT JOIN
-- 	whse.veg_comp_lyr_r1_poly_internal_2022_gr_skey vri_key ON vri_key.gr_skey = og.gr_skey
-- LEFT JOIN
-- 	whse.veg_comp_lyr_r1_poly_internal_2022 vri ON vri_key.pgid = vri.pgid;
	"
result1 <- dbGetQuery(conn, query)
netdown_results <- result1


query <- "SELECT
	'Protected and Prohibited' as land_classification
	,sum(
	CASE 
	 	WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited') OR harvest_lu.rollup_name IS NULL THEN 0
		--WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited', 'High Restricted') OR harvest_lu.rollup_name IS NULL THEN 0
	 	---- not in coast
	 	--WHEN coast.pgid IS NULL THEN
	 	--	CASE 
	 	--		WHEN slope_key.val > 40 THEN 0
	 	--		WHEN vri.site_index < 8 THEN 0
	 	--		ELSE og.tp_falb
	 	--	END
	 	--ELSE
	 	---- coast
	 	--	CASE 
	 	--		WHEN slope_key.val > 60 THEN 0
	 	--		WHEN vri.site_index < 15 then 0
	 	ELSE og.tp_falb
	 	--	END
	 END) as thlb_area
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
-- 	whse.veg_comp_lyr_r1_poly_internal_2022 vri ON vri_key.pgid = vri.pgid;
	"
result1 <- dbGetQuery(conn, query)
netdown_results <- rbind(netdown_results, result1)


query <- "SELECT
	'Protected, Prohibited, High Restricted' as land_classification
	,sum(
	CASE 
	 	--WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited') OR harvest_lu.rollup_name IS NULL THEN 0
		WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited', 'High Restricted') OR harvest_lu.rollup_name IS NULL THEN 0
	 	---- not in coast
	 	--WHEN coast.pgid IS NULL THEN
	 	--	CASE 
	 	--		WHEN slope_key.val > 40 THEN 0
	 	--		WHEN vri.site_index < 8 THEN 0
	 	--		ELSE og.tp_falb
	 	--	END
	 	--ELSE
	 	---- coast
	 	--	CASE 
	 	--		WHEN slope_key.val > 60 THEN 0
	 	--		WHEN vri.site_index < 15 then 0
	 	ELSE og.tp_falb
	 	--	END
	 END) as thlb_area
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
-- 	whse.veg_comp_lyr_r1_poly_internal_2022 vri ON vri_key.pgid = vri.pgid;
	"
result1 <- dbGetQuery(conn, query)
netdown_results <- rbind(netdown_results, result1)


query <- "SELECT
	'Slope > 40 interior, > 60 coast' as land_classification
	,sum(
	CASE 
	 	--WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited') OR harvest_lu.rollup_name IS NULL THEN 0
		WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited', 'High Restricted') OR harvest_lu.rollup_name IS NULL THEN 0
	 	---- not in coast
	 	WHEN coast.pgid IS NULL THEN
	 		CASE 
	 			WHEN slope_key.val > 40 THEN 0
	 	--		WHEN vri.site_index < 8 THEN 0
	 			ELSE og.tp_falb
	 		END
	 	ELSE
	 	-- coast
	 		CASE 
	 			WHEN slope_key.val > 60 THEN 0
	 	--		WHEN vri.site_index < 15 then 0
	 			ELSE og.tp_falb
			END
		END
	 ) as thlb_area
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
-- 	whse.veg_comp_lyr_r1_poly_internal_2022 vri ON vri_key.pgid = vri.pgid;
	"
result1 <- dbGetQuery(conn, query)
netdown_results <- rbind(netdown_results, result1)


query <- "SELECT
	'VRI site index < 8 interior, < 15 coast' as land_classification
	,sum(
	CASE 
	 	--WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited') OR harvest_lu.rollup_name IS NULL THEN 0
		WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited', 'High Restricted') OR harvest_lu.rollup_name IS NULL THEN 0
	 	---- not in coast
	 	WHEN coast.pgid IS NULL THEN
	 		CASE 
	 			WHEN slope_key.val > 40 THEN 0
	 			WHEN vri.site_index < 8 THEN 0
	 			ELSE og.tp_falb
	 		END
	 	ELSE
	 	-- coast
	 		CASE 
	 			WHEN slope_key.val > 60 THEN 0
	 			WHEN vri.site_index < 15 then 0
	 			ELSE og.tp_falb
			END
		END
	 ) as thlb_area
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
	whse.veg_comp_lyr_r1_poly_internal_2022 vri ON vri_key.pgid = vri.pgid;
	"
result1 <- dbGetQuery(conn, query)
netdown_results <- rbind(netdown_results, result1)

print('Finished netdown queries, starting man unit query..')
query <- "WITH thlb_proxy AS (
SELECT
	og.gr_skey
	, CASE 
		WHEN coast.pgid IS NULL THEN 0
		ELSE 1
	END as coast_bool
	, CASE 
	 	--WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited') OR harvest_lu.rollup_name IS NULL THEN 0
		WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited', 'High Restricted') OR harvest_lu.rollup_name IS NULL THEN 0
	 	---- not in coast
	 	WHEN coast.pgid IS NULL THEN
	 		CASE 
	 			WHEN slope_key.val > 40 THEN 0
	 			WHEN vri.site_index < 8 THEN 0
	 			ELSE og.tp_falb
	 		END
	 	ELSE
	 	-- coast
	 		CASE 
	 			WHEN slope_key.val > 60 THEN 0
	 			WHEN vri.site_index < 15 then 0
	 			ELSE og.tp_falb
			END
	END as thlb
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
), thlb_proxy_mgmt_unit AS (
SELECT 
	(sum(coast_bool)::real/count(*)::real)*100 as pct_coast
	, CASE 
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
		END) AS thlb_fact
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
	END
), thlb_mgmt_unit AS (
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
		THEN fmlb.fmlb
		WHEN manlic.forest_file_id IS NULL THEN coalesce(thlb_fact, 0) * fmlb.fmlb -- tsa/tfl
		ELSE 0
		END) AS thlb_fact
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
	END
)
SELECT 	
	a.man_unit
	, a.tenure_type
	, proxy.pct_coast
	, a.thlb_fact as area
	, proxy.thlb_fact as proxy_area
FROM
	thlb_proxy_mgmt_unit proxy
JOIN
	thlb_mgmt_unit a 
USING 
	(man_unit)
ORDER BY
	tenure_type, man_unit"
man_unit_results <- dbGetQuery(conn, query)
today_date <- format(Sys.Date(), "%Y-%m-%d")
tsa_results <- man_unit_results[which(man_unit_results$tenure_type == 'TSA'),]
tsa_man_unit_results_csv <- glue("final/tables/thlb-proxy-tsa-round1-{today_date}.csv")
write.csv(tsa_results, file = tsa_man_unit_results_csv, row.names = FALSE)

tfl_results <- man_unit_results[which(man_unit_results$tenure_type == 'TFL'),]
tfl_man_unit_results_csv <- glue("final/tables/thlb-proxy-tfl-round1-{today_date}.csv")
write.csv(tfl_results, file = tfl_man_unit_results_csv, row.names = FALSE)

netdown_results_csv <- glue("final/tables/thlb-proxy-netdown-round1-{today_date}.csv")
write.csv(netdown_results, file = netdown_results_csv, row.names = FALSE)
