library(dadmtools)
library(janitor)
library(readr)
library(stringr)
library(tidyr)
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
## Goal is to filter out all vri polygons that have a site index less than 5th percentile within area based used
## that is "merchantability"
## the thresholds would be a good input to have available for everyone next week
query <- glue("DROP TABLE IF EXISTS {dst_schema}.tsa_5p_site_index_cc")
run_sql_r(query, conn_list)

query <- glue("CREATE TABLE {dst_schema}.tsa_5p_site_index_cc AS
WITH tsa_cc AS (
	SELECT
		mu_look.man_unit as man_unit
		, cc.opening_id
	FROM 
	whse.tsa_boundaries_gr_skey tsa_key
	LEFT JOIN whse.tsa_boundaries tsa ON tsa.pgid = tsa_key.pgid 
	LEFT JOIN whse.mu_lookup_table_im mu_look ON tsa.tsa = mu_look.tsa_number
	LEFT JOIN whse.veg_consolidated_cut_blocks_sp_gr_skey cc_key ON cc_key.gr_skey = tsa_key.gr_skey
	LEFT JOIN whse.veg_consolidated_cut_blocks_sp cc ON cc.pgid = cc_key.pgid
	WHERE 
		harvest_year >= 2017 -- 2007 is the first year where we have 30 cutblocks in each TSA
	GROUP BY 
		mu_look.man_unit
		,cc.opening_id
), tsa_cc_30 AS (
	SELECT
		man_unit
		, count(*) AS cc_count
	FROM
		tsa_cc
	GROUP BY 
		man_unit
	HAVING 
		count(*) >= 30
), ge30 AS (
SELECT
	mu_look.man_unit,
	percentile_disc(0.05) WITHIN GROUP (ORDER BY CASE WHEN vri.bclcs_level_1 = 'U' THEN vritfl.site_index ELSE vri.site_index END) AS tfl_integrated_p5,
	cc_count.cc_count,
	count(*) as ha
FROM 
	{dst_schema}.veg_comp_lyr_r1_poly_2016_gr_skey vri_key
	LEFT JOIN {dst_schema}.veg_comp_lyr_r1_poly_2016 vri USING (pgid)
	LEFT JOIN {dst_schema}.tfl_integrated2016_gr_skey vritfl_key ON vritfl_key.gr_skey = vri_key.gr_skey
	LEFT JOIN {dst_schema}.tfl_integrated2016 vritfl ON vritfl_key.pgid = vritfl.pgid
	LEFT JOIN whse.tsa_boundaries_gr_skey tsa_key on tsa_key.gr_skey = vri_key.gr_skey
	LEFT JOIN whse.tsa_boundaries tsa on tsa.pgid = tsa_key.pgid -- length 34
	LEFT JOIN whse.mu_lookup_table_im mu_look ON tsa.tsa = mu_look.tsa_number
	LEFT JOIN whse.veg_consolidated_cut_blocks_sp_gr_skey cc_key ON cc_key.gr_skey = vri_key.gr_skey
	LEFT JOIN whse.veg_consolidated_cut_blocks_sp cc ON cc.pgid = cc_key.pgid
	JOIN tsa_cc_30 cc_count on cc_count.man_unit = mu_look.man_unit
WHERE 
	harvest_year >= 2017
GROUP BY
	mu_look.man_unit, 
	cc_count.cc_count
)
SELECT
	*
FROM
	ge30
UNION ALL
SELECT
	CASE 
		WHEN man_unit = '40 - Fort St. John TSA' THEN '8 - Fort Nelson TSA'
		WHEN man_unit = '43 - Nass TSA' THEN '4 - Cassiar TSA'
	END as man_unit
	, CASE 
		WHEN man_unit = '40 - Fort St. John TSA' THEN tfl_integrated_p5
		WHEN man_unit = '43 - Nass TSA' THEN tfl_integrated_p5
	END as tfl_integrated_p5
	, CASE 
		WHEN man_unit = '40 - Fort St. John TSA' THEN cc_count
		WHEN man_unit = '43 - Nass TSA' THEN cc_count
	END as cc_count
	, CASE 
		WHEN man_unit = '40 - Fort St. John TSA' THEN ha
		WHEN man_unit = '43 - Nass TSA' THEN ha
	END as ha
FROM
	ge30
WHERE
	man_unit IN ('40 - Fort St. John TSA', '43 - Nass TSA')")
run_sql_r(query, conn_list)

query <- glue("DROP TABLE IF EXISTS {dst_schema}.tsa_tfl_abt_5p_site_index_cc")
run_sql_r(query, conn_list)
query <- glue("CREATE TABLE {dst_schema}.tsa_tfl_abt_5p_site_index_cc AS
WITH g30_tfl_abt AS (
(with tfl_cc as (
	SELECT
		mu_look.man_unit as man_unit
		, cc.opening_id
	FROM 
	whse.fadm_tfl_all_sp_gr_skey tfl_key
	LEFT JOIN whse.fadm_tfl_all_sp tfl ON tfl.pgid = tfl_key.pgid 
	LEFT JOIN whse.mu_lookup_table_im mu_look ON tfl.forest_file_id = mu_look.forest_file_id
	LEFT JOIN whse.veg_consolidated_cut_blocks_sp_gr_skey cc_key ON cc_key.gr_skey = tfl_key.gr_skey
	LEFT JOIN whse.veg_consolidated_cut_blocks_sp cc ON cc.pgid = cc_key.pgid
	WHERE 
		harvest_year >= 2017 -- 2007 is the first year where we have 30 cutblocks in each TSA
	GROUP BY 
		mu_look.man_unit
		,cc.opening_id
), tsa_cc_30 AS (
	SELECT
		man_unit
		, count(*) AS cc_count
	FROM
		tfl_cc
	GROUP BY 
		man_unit
	HAVING 
		count(*) >= 30
)
SELECT
	mu_look.man_unit,
	percentile_disc(0.05) WITHIN GROUP (ORDER BY CASE WHEN vri.bclcs_level_1 = 'U' THEN vritfl.site_index ELSE vri.site_index END) AS tfl_integrated_p5,
	cc_count.cc_count,
	count(*) as ha
FROM 
	{dst_schema}.veg_comp_lyr_r1_poly_2016_gr_skey vri_key
	LEFT JOIN {dst_schema}.veg_comp_lyr_r1_poly_2016 vri USING (pgid)
	LEFT JOIN {dst_schema}.tfl_integrated2016_gr_skey vritfl_key ON vritfl_key.gr_skey = vri_key.gr_skey
	LEFT JOIN {dst_schema}.tfl_integrated2016 vritfl ON vritfl_key.pgid = vritfl.pgid
	LEFT JOIN whse.fadm_tfl_all_sp_gr_skey tfl_key on tfl_key.gr_skey = vri_key.gr_skey
	LEFT JOIN whse.fadm_tfl_all_sp tfl on tfl.pgid = tfl_key.pgid -- length 34
	LEFT JOIN whse.mu_lookup_table_im mu_look ON tfl.forest_file_id = mu_look.forest_file_id
	LEFT JOIN whse.veg_consolidated_cut_blocks_sp_gr_skey cc_key ON cc_key.gr_skey = vri_key.gr_skey
	LEFT JOIN whse.veg_consolidated_cut_blocks_sp cc ON cc.pgid = cc_key.pgid
	JOIN tsa_cc_30 cc_count on cc_count.man_unit = mu_look.man_unit
WHERE 
	harvest_year >= 2017
GROUP BY
	mu_look.man_unit, 
	cc_count.cc_count
)
UNION ALL
-- area based tenures
(WITH abt_cc as (
	SELECT
		mu_look.man_unit as man_unit
		, cc.opening_id
	FROM 
	whse.ften_managed_licence_poly_svw_gr_skey tfl_key
	LEFT JOIN whse.ften_managed_licence_poly_svw tfl ON tfl.pgid = tfl_key.pgid 
	LEFT JOIN whse.mu_lookup_table_im mu_look ON tfl.forest_file_id = mu_look.forest_file_id
	LEFT JOIN whse.veg_consolidated_cut_blocks_sp_gr_skey cc_key ON cc_key.gr_skey = tfl_key.gr_skey
	LEFT JOIN whse.veg_consolidated_cut_blocks_sp cc ON cc.pgid = cc_key.pgid
	WHERE 
		harvest_year >= 2017 -- 2007 is the first year where we have 30 cutblocks in each TSA
	GROUP BY 
		mu_look.man_unit
		,cc.opening_id
), abt_cc_30 AS (
	SELECT
		man_unit
		, count(*) AS cc_count
	FROM
		abt_cc
	GROUP BY 
		man_unit
	HAVING 
		count(*) >= 30
)
SELECT
	mu_look.man_unit,
	percentile_disc(0.05) WITHIN GROUP (ORDER BY CASE WHEN vri.bclcs_level_1 = 'U' THEN vritfl.site_index ELSE vri.site_index END) AS tfl_integrated_p5,
	cc_count.cc_count,
	count(*) as ha
FROM 
	{dst_schema}.veg_comp_lyr_r1_poly_2016_gr_skey vri_key
	LEFT JOIN {dst_schema}.veg_comp_lyr_r1_poly_2016 vri USING (pgid)
	LEFT JOIN {dst_schema}.tfl_integrated2016_gr_skey vritfl_key ON vritfl_key.gr_skey = vri_key.gr_skey
	LEFT JOIN {dst_schema}.tfl_integrated2016 vritfl ON vritfl_key.pgid = vritfl.pgid
	LEFT JOIN whse.ften_managed_licence_poly_svw_gr_skey tfl_key on tfl_key.gr_skey = vri_key.gr_skey
	LEFT JOIN whse.ften_managed_licence_poly_svw tfl on tfl.pgid = tfl_key.pgid -- length 34
	LEFT JOIN whse.mu_lookup_table_im mu_look ON tfl.forest_file_id = mu_look.forest_file_id
	LEFT JOIN whse.veg_consolidated_cut_blocks_sp_gr_skey cc_key ON cc_key.gr_skey = vri_key.gr_skey
	LEFT JOIN whse.veg_consolidated_cut_blocks_sp cc ON cc.pgid = cc_key.pgid
	JOIN abt_cc_30 cc_count on cc_count.man_unit = mu_look.man_unit
WHERE 
	harvest_year >= 2017
GROUP BY
	mu_look.man_unit, 
	cc_count.cc_count
)
), missing_tfl_abt AS (
SELECT
	man_unit,
	tsa,
	tfl_integrated_p5,
	cc_count,
	ha
FROM
	{dst_schema}.tsa_link_tfl_manlic
LEFT JOIN g30_tfl_abt using (man_unit)
	)
SELECT 
	a.man_unit,
	a.tsa,
	CASE WHEN a.tfl_integrated_p5 IS NULL THEN tsa_5p.tfl_integrated_p5 ELSE a.tfl_integrated_p5 END as tfl_integrated_p5,
	a.cc_count,
	a.ha
FROM
missing_tfl_abt a
JOIN
{dst_schema}.tsa_5p_site_index_cc tsa_5p ON tsa_5p.man_unit = a.tsa
UNION ALL
SELECT
	man_unit,
	man_unit,
	tfl_integrated_p5,
	cc_count,
	ha
FROM
	{dst_schema}.tsa_5p_site_index_cc")
run_sql_r(query, conn_list)
query <- glue("DROP TABLE IF EXISTS {dst_schema}.tsa_5p_site_index_cc")
run_sql_r(query, conn_list)


## first go at merchantable query
query <- "SELECT
CASE 
	WHEN si_manlic.man_unit IS NOT null THEN si_manlic.man_unit
	WHEN si_tfl.man_unit IS NOT NULL THEN si_tfl.man_unit
	WHEN si_tsa.man_unit IS NOT NULL THEN si_tsa.man_unit
END,
CASE 
	WHEN si_manlic.man_unit IS NOT null THEN 
		CASE WHEN vri.bclcs_level_1 = 'U' THEN vritfl.site_index ELSE vri.site_index END >= si_manlic.tfl_integreated_p5 THEN 1 ELSE 0 END
	WHEN si_tfl.man_unit IS NOT NULL THEN si_tfl.man_unit
	WHEN si_tsa.man_unit IS NOT NULL THEN si_tsa.man_unit
END,
count(*)

FROM
thlb_proxy.veg_comp_lyr_r1_poly_2016_gr_skey vri_key
LEFT JOIN thlb_proxy.veg_comp_lyr_r1_poly_2016 vri USING (pgid)
LEFT JOIN thlb_proxy.tfl_integrated2016_gr_skey vritfl_key ON vritfl_key.gr_skey = vri_key.gr_skey
LEFT JOIN thlb_proxy.tfl_integrated2016 vritfl ON vritfl_key.pgid = vritfl.pgid
LEFT JOIN whse.tsa_boundaries_gr_skey tsa_key on tsa_key.gr_skey = vri_key.gr_skey
LEFT JOIN whse.tsa_boundaries tsa on tsa.pgid = tsa_key.pgid -- length 34
LEFT JOIN whse.mu_lookup_table_im mu_tsa ON tsa.tsa = mu_tsa.tsa_number
LEFT JOIN thlb_proxy.tsa_tfl_abt_5p_site_index_cc si_tsa ON mu_tsa.man_unit = si_tsa.man_unit
LEFT JOIN whse.fadm_tfl_all_sp_gr_skey tfl_key ON tfl_key.gr_skey = vri_key.gr_skey
LEFT JOIN whse.fadm_tfl_all_sp tfl ON tfl.pgid = tfl_key.pgid 
LEFT JOIN whse.mu_lookup_table_im mu_tfl ON tfl.forest_file_id = mu_tfl.forest_file_id
LEFT JOIN thlb_proxy.tsa_tfl_abt_5p_site_index_cc si_tfl ON mu_tfl.man_unit = si_tfl.man_unit
LEFT JOIN whse.ften_managed_licence_poly_svw_gr_skey manlic_key ON manlic_key.gr_skey = vri_key.gr_skey
LEFT JOIN whse.ften_managed_licence_poly_svw manlic ON manlic.pgid = manlic_key.pgid 
LEFT JOIN whse.mu_lookup_table_im mu_manlic ON manlic.forest_file_id = mu_manlic.forest_file_id
LEFT JOIN thlb_proxy.tsa_tfl_abt_5p_site_index_cc si_manlic ON mu_manlic.man_unit = si_manlic.man_unit
GROUP BY
CASE 
	WHEN si_manlic.man_unit IS NOT null THEN si_manlic.man_unit
	WHEN si_tfl.man_unit IS NOT NULL THEN si_tfl.man_unit
	WHEN si_tsa.man_unit IS NOT NULL THEN si_tsa.man_unit
END

-- select * from thlb_proxy.tsa_tfl_abt_5p_site_index_cc -- man_unit W0326 - Woodlot
-- select * from whse.ften_managed_licence_poly_svw -- forest_file_id W0025
-- select * from whse.mu_lookup_table_im -- forest_file_id W0025 || man_unit W0326 - Woodlot"
