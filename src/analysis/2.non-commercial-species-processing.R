library(dadmtools)
source('src/utils/functions.R')

conn_list <- dadmtools::get_pg_conn_list()
dst_schema <- "thlb_proxy"

query <- "DROP TABLE IF EXISTS thlb_proxy.tsa_species_1_cc_ha;"
run_sql_r(query, conn_list)

query <- "CREATE TABLE thlb_proxy.tsa_species_1_cc_ha AS
SELECT
-- area.label as area_name
tsa_rank1
, CASE 
	WHEN vri.bclcs_level_1 = 'U' THEN vritfl.species_cd_1 
	ELSE vri.species_cd_1
  END AS species_cd_1
 , count(*) as species_cd_1_ha
, sum(CASE WHEN cc.harvest_start_year_calendar >= 2017 THEN 1 ELSE 0 END) as harvested_ha

FROM
whse.all_bc_gr_skey bc
LEFT JOIN whse.veg_consolidated_cut_blocks_sp_2024_gr_skey cc_key ON cc_key.gr_skey = bc.gr_skey
LEFT JOIN whse.veg_consolidated_cut_blocks_sp_2024 cc ON cc.pgid = cc_key.pgid
LEFT JOIN thlb_proxy.veg_comp_lyr_r1_poly_2016_gr_skey vri_key on vri_key.gr_skey = bc.gr_skey
LEFT JOIN thlb_proxy.veg_comp_lyr_r1_poly_2016 vri ON vri.pgid = vri_key.pgid
LEFT JOIN thlb_proxy.tfl_integrated2016_gr_skey vritfl_key ON vritfl_key.gr_skey = vri_key.gr_skey
LEFT JOIN thlb_proxy.tfl_integrated2016 vritfl ON vritfl_key.pgid = vritfl.pgid
LEFT JOIN whse.man_unit_gr_skey man_unit on man_unit.gr_skey = bc.gr_skey
LEFT JOIN thlb_proxy.seral_2023_tap_method fmlb on fmlb.gr_skey = bc.gr_skey
-- LEFT JOIN whse.north_south_coast_gr_skey area_key on area_key.gr_skey = bc.gr_skey
-- LEFT JOIN whse.north_south_coast area on area.fid = area_key.fid
WHERE
	fmlb.fmlb_adj = 1
GROUP BY
	-- area.label
	tsa_rank1
	, CASE 
		WHEN vri.bclcs_level_1 = 'U' THEN vritfl.species_cd_1 
		ELSE vri.species_cd_1
	 END;"
run_sql_r(query, conn_list)