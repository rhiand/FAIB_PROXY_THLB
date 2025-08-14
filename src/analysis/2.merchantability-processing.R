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
dst_schema <- "whse"

## Goal is to filter out all vri polygons that have a site index less than 5th percentile within mgmt unit that has >= 30 cutblocks
## if the mgmt unit has < 30 cutblocks, use the TSA it overlaps with most
print(glue("Running query to recreate {dst_schema}.tsa_5p_site_index_cc"))
query <- glue("DROP TABLE IF EXISTS {dst_schema}.tsa_5p_site_index_cc")
run_sql_r(query, conn_list)

## create a table of the TSA 5th percentile of TFL integreated site_index from VRI 2016
query <- glue("CREATE TABLE {dst_schema}.tsa_5p_site_index_cc AS
WITH tsa_cc AS (
    -- calculate how many opening_id cutblocks >= 2017 exist within each TSA
	SELECT
		mu_look.man_unit as man_unit
		, tsa.tsa_number as forest_file_id
		, cc.opening_id
		, count(*)
		, COUNT(*) FILTER (WHERE (CASE WHEN vri.bclcs_level_1 = 'U' THEN vritfl.site_index ELSE vri.site_index END) IS NOT NULL) AS not_null_count
		, round(((COUNT(*) FILTER (WHERE (CASE WHEN vri.bclcs_level_1 = 'U' THEN vritfl.site_index ELSE vri.site_index END) IS NOT NULL))::numeric / count(*)::numeric) * 100, 1) as pct_not_null
	FROM
	{dst_schema}.tsa_boundaries_gr_skey tsa_key
	LEFT JOIN {dst_schema}.tsa_boundaries tsa ON tsa.pgid = tsa_key.pgid
	LEFT JOIN {dst_schema}.mu_lookup_table_im mu_look ON tsa.tsa = mu_look.tsa_number
	LEFT JOIN {dst_schema}.veg_consolidated_cut_blocks_sp_gr_skey cc_key ON cc_key.gr_skey = tsa_key.gr_skey
	LEFT JOIN {dst_schema}.veg_consolidated_cut_blocks_sp cc ON cc.pgid = cc_key.pgid
	LEFT JOIN {dst_schema}.veg_comp_lyr_r1_poly_2016_gr_skey vri_key on vri_key.gr_skey = tsa_key.gr_skey
	LEFT JOIN (SELECT CASE WHEN site_index = 0 THEN NULL ELSE site_index END AS site_index, bclcs_level_1, pgid FROM {dst_schema}.veg_comp_lyr_r1_poly_2016) vri ON vri.pgid = vri_key.pgid
	LEFT JOIN {dst_schema}.tfl_integrated2016_gr_skey vritfl_key ON vritfl_key.gr_skey = vri_key.gr_skey
	LEFT JOIN (SELECT CASE WHEN site_index = 0 THEN NULL ELSE site_index END AS site_index, pgid FROM {dst_schema}.tfl_integrated2016) vritfl ON vritfl_key.pgid = vritfl.pgid
	WHERE
		cc.harvest_start_year_calendar >= 2017
    GROUP BY
		mu_look.man_unit
		, tsa.tsa_number
		, cc.opening_id
), tsa_cc_30 AS (
	-- identify tsas that have >= 30 cutblocks
-- 	-- percent site index not null must be >= 75%
	SELECT
		man_unit
		, count(*) AS cc_count
	FROM
		tsa_cc
	WHERE
		pct_not_null >= 75
	GROUP BY
		man_unit
	HAVING
		count(*) >= 30
), ge30 AS (
	-- merge the 2016 VRI with the TFL integreated vri from 2016
	-- calculate the 5th percentile of the merged VRI site index for each TSA that has >= 30 cutblocks
	SELECT
		mu_look.man_unit,
		tsa.tsa_number as forest_file_id,
		percentile_disc(0.05) WITHIN GROUP (ORDER BY CASE WHEN vri.bclcs_level_1 = 'U' THEN vritfl.site_index ELSE vri.site_index END) AS tfl_integrated_p5,
		cc_count.cc_count
	FROM
		{dst_schema}.tsa_boundaries_gr_skey tsa_key
		LEFT JOIN {dst_schema}.tsa_boundaries tsa ON tsa.pgid = tsa_key.pgid
		LEFT JOIN {dst_schema}.mu_lookup_table_im mu_look ON tsa.tsa = mu_look.tsa_number
		LEFT JOIN {dst_schema}.veg_consolidated_cut_blocks_sp_gr_skey cc_key ON cc_key.gr_skey = tsa_key.gr_skey
		LEFT JOIN {dst_schema}.veg_consolidated_cut_blocks_sp cc ON cc.pgid = cc_key.pgid
		LEFT JOIN {dst_schema}.veg_comp_lyr_r1_poly_2016_gr_skey vri_key on vri_key.gr_skey = tsa_key.gr_skey
		LEFT JOIN (SELECT CASE WHEN site_index = 0 THEN NULL ELSE site_index END AS site_index, bclcs_level_1, pgid FROM {dst_schema}.veg_comp_lyr_r1_poly_2016) vri ON vri.pgid = vri_key.pgid
		LEFT JOIN {dst_schema}.tfl_integrated2016_gr_skey vritfl_key ON vritfl_key.gr_skey = vri_key.gr_skey
		LEFT JOIN (SELECT CASE WHEN site_index = 0 THEN NULL ELSE site_index END AS site_index, pgid FROM {dst_schema}.tfl_integrated2016) vritfl ON vritfl_key.pgid = vritfl.pgid
		JOIN tsa_cc ON tsa_cc.opening_id = cc.opening_id
		JOIN tsa_cc_30 cc_count on cc_count.man_unit = mu_look.man_unit
	WHERE
		cc.harvest_start_year_calendar >= 2017 
	AND 
		tsa_cc.pct_not_null >= 75
	GROUP BY
		mu_look.man_unit,
		tsa.tsa_number,
		cc_count.cc_count
), final_tbl as (
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
	,       CASE
		WHEN man_unit = '40 - Fort St. John TSA' THEN '08'
		WHEN man_unit = '43 - Nass TSA' THEN '04'
	END as forest_file_id
	, CASE
		WHEN man_unit = '40 - Fort St. John TSA' THEN tfl_integrated_p5
		WHEN man_unit = '43 - Nass TSA' THEN tfl_integrated_p5
	END as tfl_integrated_p5
	, CASE
		WHEN man_unit = '40 - Fort St. John TSA' THEN cc_count
		WHEN man_unit = '43 - Nass TSA' THEN cc_count
	END as cc_count
FROM
	ge30
WHERE
	man_unit IN ('40 - Fort St. John TSA', '43 - Nass TSA')
)SELECT * FROM final_tbl order by man_unit")
run_sql_r(query, conn_list)

print(glue("Running query to recreate {dst_schema}.tsa_tfl_abt_5p_site_index_cc"))
query <- glue("DROP TABLE IF EXISTS {dst_schema}.tsa_tfl_abt_5p_site_index_cc")
run_sql_r(query, conn_list)
query <- glue("CREATE TABLE {dst_schema}.tsa_tfl_abt_5p_site_index_cc AS
WITH tfl_cc AS (
  -- calculate how many opening_id cutblocks >= 2017 exist within each TFL
  SELECT
    mu_look.man_unit as man_unit
    , tfl.forest_file_id as forest_file_id
    , cc.opening_id
    , count(*)
    , COUNT(*) FILTER (WHERE (CASE WHEN vri.bclcs_level_1 = 'U' THEN vritfl.site_index ELSE vri.site_index END) IS NOT NULL) AS not_null_count
    , round(((COUNT(*) FILTER (WHERE (CASE WHEN vri.bclcs_level_1 = 'U' THEN vritfl.site_index ELSE vri.site_index END) IS NOT NULL))::numeric / count(*)::numeric) * 100, 1) as pct_not_null

  FROM
  {dst_schema}.fadm_tfl_all_sp_gr_skey tfl_key
  LEFT JOIN {dst_schema}.fadm_tfl_all_sp tfl ON tfl.pgid = tfl_key.pgid 
  LEFT JOIN {dst_schema}.mu_lookup_table_im mu_look ON tfl.forest_file_id = mu_look.forest_file_id
  LEFT JOIN {dst_schema}.veg_consolidated_cut_blocks_sp_gr_skey cc_key ON cc_key.gr_skey = tfl_key.gr_skey
  LEFT JOIN {dst_schema}.veg_consolidated_cut_blocks_sp cc ON cc.pgid = cc_key.pgid
  LEFT JOIN {dst_schema}.veg_comp_lyr_r1_poly_2016_gr_skey vri_key on vri_key.gr_skey = tfl_key.gr_skey
  LEFT JOIN (SELECT CASE WHEN site_index = 0 THEN NULL ELSE site_index END AS site_index, bclcs_level_1, pgid FROM {dst_schema}.veg_comp_lyr_r1_poly_2016) vri ON vri.pgid = vri_key.pgid
  LEFT JOIN {dst_schema}.tfl_integrated2016_gr_skey vritfl_key ON vritfl_key.gr_skey = vri_key.gr_skey
  LEFT JOIN (SELECT CASE WHEN site_index = 0 THEN NULL ELSE site_index END AS site_index, pgid FROM {dst_schema}.tfl_integrated2016) vritfl ON vritfl_key.pgid = vritfl.pgid
  WHERE
    cc.harvest_start_year_calendar >= 2017
  GROUP BY
    mu_look.man_unit
    , tfl.forest_file_id
    , cc.opening_id
), tfl_cc_30 AS (
  -- identify tfls that have >= 30 cutblocks
  SELECT
    man_unit
    , count(*) AS cc_count
  FROM
    tfl_cc
  WHERE
    pct_not_null >= 75
  GROUP BY
    man_unit
  HAVING
    count(*) >= 30
), g30_tfl_abt AS (
  -- merge the 2016 VRI with the TFL integreated vri from 2016
  -- calculate the 5th percentile of the merged VRI site index for each TFL that has >= 30 cutblocks
  SELECT
    mu_look.man_unit,
    tfl.forest_file_id,
    percentile_disc(0.05) WITHIN GROUP (ORDER BY CASE WHEN vri.bclcs_level_1 = 'U' THEN vritfl.site_index ELSE vri.site_index END) AS tfl_integrated_p5,
    cc_count.cc_count,
    count(*) as ha
FROM
  {dst_schema}.veg_comp_lyr_r1_poly_2016_gr_skey vri_key
  LEFT JOIN (SELECT CASE WHEN site_index = 0 THEN NULL ELSE site_index END as site_index, bclcs_level_1, pgid FROM {dst_schema}.veg_comp_lyr_r1_poly_2016) vri USING (pgid)
  LEFT JOIN {dst_schema}.tfl_integrated2016_gr_skey vritfl_key ON vritfl_key.gr_skey = vri_key.gr_skey
  LEFT JOIN (SELECT CASE WHEN site_index = 0 THEN NULL ELSE site_index END as site_index, pgid FROM {dst_schema}.tfl_integrated2016) vritfl ON vritfl_key.pgid = vritfl.pgid
  LEFT JOIN {dst_schema}.fadm_tfl_all_sp_gr_skey tfl_key on tfl_key.gr_skey = vri_key.gr_skey
  LEFT JOIN {dst_schema}.fadm_tfl_all_sp tfl on tfl.pgid = tfl_key.pgid 
  LEFT JOIN {dst_schema}.mu_lookup_table_im mu_look ON tfl.forest_file_id = mu_look.forest_file_id
  LEFT JOIN {dst_schema}.veg_consolidated_cut_blocks_sp_gr_skey cc_key ON cc_key.gr_skey = vri_key.gr_skey
  LEFT JOIN {dst_schema}.veg_consolidated_cut_blocks_sp cc ON cc.pgid = cc_key.pgid
  JOIN tfl_cc_30 cc_count on cc_count.man_unit = mu_look.man_unit
  JOIN tfl_cc ON tfl_cc.opening_id = cc.opening_id
  WHERE
    cc.harvest_start_year_calendar >= 2017
AND
    tfl_cc.pct_not_null >= 75
GROUP BY
    mu_look.man_unit,
    tfl.forest_file_id,
    cc_count.cc_count
), missing_tfl_abt AS (
        SELECT
      man_unit,
      forest_file_id,
      tsa,
      tfl_integrated_p5,
      cc_count
        FROM
      {dst_schema}.tsa_link_tfl_manlic
        LEFT JOIN g30_tfl_abt using (man_unit)
    WHERE
      man_unit not ilike '%Community Forest' AND man_unit not ilike '%FNWL' AND man_unit not ilike '%Woodlot'
), final_tbl as (
SELECT
  a.man_unit,
  CASE WHEN a.forest_file_id IS NULL THEN mu.forest_file_id ELSE a.forest_file_id END as forest_file_id,
  a.tsa,
  CASE WHEN a.tfl_integrated_p5 IS NULL THEN tsa_5p.tfl_integrated_p5 ELSE a.tfl_integrated_p5 END as tfl_integrated_p5,
  a.cc_count,
  CASE WHEN a.forest_file_id IS NULL THEN 'TSA Threshold Used' ELSE 'Own Threshold Used' END as notes
FROM
missing_tfl_abt a
JOIN
{dst_schema}.mu_lookup_table_im mu ON a.man_unit = mu.man_unit
JOIN
{dst_schema}.tsa_5p_site_index_cc tsa_5p ON tsa_5p.man_unit = a.tsa
UNION ALL
SELECT
        man_unit,
        forest_file_id,
        man_unit,
        tfl_integrated_p5,
        cc_count,
    	'Own Threshold Used' AS notes

FROM
        {dst_schema}.tsa_5p_site_index_cc
) 
SELECT * FROM final_tbl order by man_unit")
run_sql_r(query, conn_list)

query <- glue("DROP TABLE IF EXISTS {dst_schema}.tsa_5p_site_index_cc")
run_sql_r(query, conn_list)

query <- glue("UPDATE {dst_schema}.tsa_tfl_abt_5p_site_index_cc SET notes = 'Used adjacent 40 - Fort St. John TSA threshold', tsa = '40 - Fort St. John TSA'
WHERE man_unit = '8 - Fort Nelson TSA'")
run_sql_r(query, conn_list)


query <- glue("UPDATE {dst_schema}.tsa_tfl_abt_5p_site_index_cc SET notes = 'Used adjacent 43 - Nass TSA threshold', tsa = '43 - Nass TSA'
WHERE man_unit = '4 - Cassiar TSA'")
run_sql_r(query, conn_list)