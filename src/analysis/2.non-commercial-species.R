library(dadmtools)
source('src/utils/functions.R')

conn_list <- dadmtools::get_pg_conn_list()
dst_schema <- "thlb_proxy"

query <- "DROP TABLE IF EXISTS thlb_proxy.tsa_species_1_cc_ha;"
run_sql_r(query, conn_list)

query <- "CREATE TABLE thlb_proxy.tsa_species_1_cc_ha AS
WITH species_per_cutblock AS (
	SELECT 
		man_unit.tsa_rank1,
		vri.feature_id, 
		vri.species_cd_1,
		vri.species_pct_1,
		count(*) * vri.species_pct_1/100  AS species_cd_1_ha,
		sum(CASE WHEN cc.pgid is not null then 1 else 0 end) * vri.species_pct_1/100 AS species_cd_1_per_cc_ha
	FROM
	whse.veg_comp_lyr_r1_poly_internal_2023 vri
	LEFT JOIN whse.veg_comp_lyr_r1_poly_internal_2023_gr_skey vri_key using (pgid)
	LEFT JOIN whse.veg_consolidated_cut_blocks_sp_2024_gr_skey cc_key ON cc_key.gr_skey = vri_key.gr_skey
	LEFT JOIN whse.veg_consolidated_cut_blocks_sp_2024 cc on cc.pgid = cc_key.pgid
	LEFT JOIN whse.man_unit_gr_skey man_unit on man_unit.gr_skey = vri_key.gr_skey
-- 	WHERE feature_id in (8710553, 8710566, 8710679, 8710718, 8710798)
	GROUP BY  
		man_unit.tsa_rank1,
		vri.feature_id, 
		vri.species_cd_1,
		vri.species_pct_1	
)
SELECT
	tsa_rank1,
	species_cd_1,
	sum(species_cd_1_ha) AS species_cd_1_ha,
	sum(species_cd_1_per_cc_ha) AS species_cd_1_per_cc_ha
FROM
species_per_cutblock
-- WHERE
-- 	sum(species_cd_1_per_cc_ha), 0) > 0 and tsa_rank1 is not null and species_cd_1 is not null
GROUP BY 
	tsa_rank1,
	species_cd_1;"
run_sql_r(query, conn_list)