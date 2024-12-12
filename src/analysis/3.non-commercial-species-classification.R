library(dadmtools)
source('src/utils/functions.R')

conn_list <- dadmtools::get_pg_conn_list()
dst_schema <- "thlb_proxy"

query <- "WITH vri_species_cd_datadict AS (
	SELECT
		CASE 
			WHEN upper(species_cd) IN ('AC','ACB','ACT','AD','AX','DG', 'E','EA','EB','EP', 'ES', 'EW','M','MB','MN', 'MV', 'QG', 'RA', 'VB', 'W', 'WS', 'XH', 'ZH') then 'decid'
			WHEN upper(species_cd) IN ('AT', 'SS', 'SB', 'PA') THEN species_full_name
			WHEN upper(species_cd) IN ('D', 'DR') THEN 'alder'
			WHEN upper(species_cd) LIKE 'F%' THEN 'fir'
			WHEN upper(species_cd) LIKE 'C%' then 'cedar'
			WHEN upper(species_cd) LIKE 'H%' then 'hemlock'
			WHEN upper(species_cd) LIKE 'S%' then 'spruce'
			WHEN upper(species_cd) LIKE 'B%' then 'balsam'
			WHEN upper(species_cd) IN ('PF','PW') then 'white_pine'
			WHEN upper(species_cd) IN ('P','PL','PLI','PLC', 'PJ') then 'pine'
			WHEN upper(species_cd) IN ('PY') then 'yellow_pine'
			WHEN upper(species_cd)  LIKE 'L%' then 'larch'
			WHEN upper(species_cd) IN ('YC') then 'cypress'
			ELSE 'other'
		END as species_grouping,
		species_cd,
		species_full_name,
		type
	FROM
		thlb_proxy.vri_species_cd_datadict			   
), species_grouping AS(
SELECT 
	tsa_rank1,
	species_grouping,
	sum(species_cd_1_ha) as species_ha,
	sum(harvested_ha) as harvested_ha
FROM
thlb_proxy.tsa_species_1_cc_ha tbl
JOIN vri_species_cd_datadict dict on tbl.species_cd_1 = dict.species_cd
GROUP BY 
	tsa_rank1,
	species_grouping
)
SELECT
	tsa_rank1,
	species_grouping,
	species_ha,
	harvested_ha,
	round((species_ha / sum(species_ha) OVER (partition by tsa_rank1)) * 100, 1) as pct_occurrence_in_landbase_fmlb,
	round((harvested_ha / sum(harvested_ha) OVER (partition by tsa_rank1)) * 100, 1) as pct_occurrence_in_harvested_fmlb,
	CASE 
		WHEN species_grouping IN ('other', 'decid', 'Whitebark Pine', 'Black Spruce') THEN 'non-commercial'
		ELSE 'commercial'
	END AS commercial,
	sum(species_ha) OVER (partition by tsa_rank1) as landbase_fmlb_ha,
	sum(harvested_ha) OVER (partition by tsa_rank1) as harvested_fmlb_ha
FROM
	species_grouping
ORDER BY
	tsa_rank1,
	round((species_ha / sum(species_ha) OVER (partition by tsa_rank1)) * 100, 1) DESC,
	round((harvested_ha / sum(harvested_ha) OVER (partition by tsa_rank1)) * 100, 1) DESC"
sql_to_df(query, conn_list)