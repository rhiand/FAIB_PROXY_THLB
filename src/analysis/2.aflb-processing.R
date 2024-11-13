library(RPostgres)
library(glue)
library(devtools)
# install_github("bcgov/FAIB_DADMTOOLS")
library(dadmtools)
source('src/utils/functions.R')
## relies on install_github("bcgov/FAIB_DATA_MANAGEMENT") being installed at some point
conn_list <- dadmtools::get_pg_conn_list()

query <- "DROP TABLE IF EXISTS thlb_proxy.prov_netdown;"
run_sql_r(query, conn_list)

query <- "CREATE TABLE thlb_proxy.prov_netdown AS
SELECT
	bc_gr_skey.gr_skey,
	1::int as bc_land,
	fown.own_sched || ' - ' || fown.ownership_description as n01_ownership,
	CASE
		-- ALPINE
		WHEN vri.bclcs_level_1 = 'N' AND vri.bclcs_level_3 = 'A' THEN 'non_vegetated_alpine_lcs' -- Alpine Rock and Ice
		WHEN vri.bclcs_level_2 = 'N' AND vri.bclcs_level_3 = 'A' THEN 'non_treed_alpine_lcs' -- Shrub/Lichen
		WHEN bec.bgc_label IN ('BAFAun', 'IMA') THEN 'alpine_bec' -- # BEC sourced Alpine,
		-- RIPARIAN
		WHEN vri.bclcs_level_1 = 'N' AND vri.bclcs_level_5 = 'LA' THEN 'lake_lcs' -- lakes
		WHEN vri.bclcs_level_1 = 'N' AND vri.bclcs_level_5 = 'RE' THEN 'reservoir_lcs' -- reservoir
		WHEN vri.bclcs_level_1 = 'N' AND vri.bclcs_level_5 = 'RI' THEN 'riparian_lcs' -- riparian
		WHEN vri.bclcs_level_2 = 'T' AND vri.bclcs_level_3 = 'W' THEN 'treed_wetland_lcs' -- treed wetland
		WHEN vri.bclcs_level_2 = 'N' AND vri.bclcs_level_3 = 'W' THEN 'non_treed_wetland_lcs'
		WHEN vri.bclcs_level_3 = 'W' THEN 'wetlands_lcs' -- nontreed wetland
		WHEN wet.waterbody_type = 'W' THEN 'wetland_fwa' -- FWA classification
		--  NonVegetated/low productivity
		WHEN vri.bclcs_level_1 = 'N' AND cc.harvest_year is null THEN 'non_vegetated_lcs' -- nonvegetated and not an opening
		WHEN vri.bclcs_level_2 = 'N' AND vri.bclcs_level_4 NOT IN ('ST', 'SL') AND cc.harvest_year is null THEN 'non_treed_herb_lcs' -- nontreed but not in a cutblock
		WHEN vri.bclcs_level_4 IN ('ST', 'SL') AND cc.harvest_year is null THEN 'non_treed_shrub_lcs'
		WHEN vri.site_index < 5 AND cc.harvest_year is null THEN 'non_productive_si_vri' -- low productivity stands
		WHEN vri.non_productive_descriptor_cd is not null AND cc.harvest_year is null THEN 'FC1_' || vri.non_productive_descriptor_cd -- stand classified in the FC1 as nonproductive
		WHEN vri.bclcs_level_1 || vri.bclcs_level_2 = 'VT' AND vri.species_cd_1 is null THEN 'no_species_cd_1' -- species label
		WHEN vri.bclcs_level_1 = 'U' OR vri.bclcs_level_1 is null THEN 'unclassified'
		ELSE NULL
	END as n02_nonfor,
	lin.fact as p03_linear_features,
	rip.fact as p04_riparian,
	inop.inop_fact as p05_phys_inop,
	merch.merchantability as n06_merchantability
FROM
whse.all_bc_gr_skey bc_gr_skey
LEFT JOIN whse.f_own_gr_skey fown_key ON bc_gr_skey.gr_skey = fown_key.gr_skey
LEFT JOIN (SELECT own || schedule as own_sched, own, schedule, ownership_description, pgid FROM whse.f_own WHERE own||schedule in ('40N', '41N', '52N', '54N', '80N')) fown USING (pgid)
LEFT JOIN whse.veg_comp_lyr_r1_poly_internal_2023_gr_skey vri_key on vri_key.gr_skey = bc_gr_skey.gr_skey 
LEFT JOIN whse.veg_comp_lyr_r1_poly_internal_2023 vri on vri.pgid = vri_key.pgid
LEFT JOIN whse.veg_consolidated_cut_blocks_sp_gr_skey ccg on ccg.gr_skey = bc_gr_skey.gr_skey 
LEFT JOIN whse.veg_consolidated_cut_blocks_sp cc on cc.pgid = ccg.pgid 
LEFT JOIN whse.bec_biogeoclimatic_poly_gr_skey bec_key on bec_key.gr_skey = bc_gr_skey.gr_skey
LEFT JOIN whse.bec_biogeoclimatic_poly bec ON bec.pgid = bec_key.pgid
LEFT JOIN whse.fwa_wetlands_gr_skey wet_key ON wet_key.gr_skey = bc_gr_skey.gr_skey
LEFT JOIN whse.fwa_wetlands wet ON wet.pgid = wet_key.pgid
LEFT JOIN thlb_proxy.bc_linear_features lin ON lin.gr_skey = bc_gr_skey.gr_skey
LEFT JOIN thlb_proxy.bc_riparian_buffers rip ON rip.gr_skey = bc_gr_skey.gr_skey
LEFT JOIN thlb_proxy.bc_inoperable_gr_skey inop ON inop.gr_skey = bc_gr_skey.gr_skey
LEFT JOIN thlb_proxy.bc_merchantability_gr_skey merch ON merch.gr_skey = bc_gr_skey.gr_skey"
run_sql_r(query, conn_list)

# query <- "WITH a AS (
# select 
# 	bc_land as bc_land
# 	, CASE 
# 		-- if ownership doesn't exist, then its still in THLB
# 		WHEN n01_ownership IS NULL THEN 1 
# 		-- otherwise, remove from THLB
# 		ELSE 0 
# 	END AS n01_ownership_thlb
# 	, CASE 
# 		-- if it 'Non forested' category is null, then its still in THLB
# 		WHEN n02_nonfor IS NULL THEN 1
# 		-- otherwise, it is non forested, thus remove from THLB
# 		ELSE 0
# 	END AS n02_nonfor_thlb
	
# from 
# thlb_proxy.prov_netdown proxy
# JOIN (select gr_skey from public.tsa02_netdown2024) tsa02 ON proxy.gr_skey = tsa02.gr_skey
# )
# SELECT
# 	sum(bc_land) as bc_land,
# 	sum(1-n01_ownership_thlb) as n01_ownership,
# 	sum(bc_land * n01_ownership_thlb) as n01_ownership_thlb,
# 	sum(1-n02_nonfor_thlb) as n02_nonfor,
# 	sum(bc_land * n01_ownership_thlb * n02_nonfor_thlb) as n02_nonfor_thlb
# FROM a"

