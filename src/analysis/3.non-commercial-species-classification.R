library(ggplot2)
library(dplyr)
library(tidyr)
library(cowplot) # For arranging plots in a grid
source('src/utils/functions.R')

conn_list <- dadmtools::get_pg_conn_list()
dst_schema <- "whse"
vector_schema <- "whse_vector"

query <- glue("DROP TABLE IF EXISTS {dst_schema}.bc_non_commercial_gr_skey;")
run_sql_r(query, conn_list)


## Dec 13, 2024
## the table created here is not currently being used - instead its rebuilt in 3.netdown-create-table.R
query <- glue("CREATE TABLE {dst_schema}.bc_non_commercial_gr_skey AS
WITH vri_species_cd_datadict AS (
	SELECT
		CASE 
			WHEN upper(species_cd) IN ('AC','ACB','ACT','AD','AX','DG', 'E','EA','EB','EP', 'ES', 'EW','M','MB','MN', 'MV', 'QG', 'RA', 'VB', 'W', 'WS', 'XH', 'ZH') then 'decid'
			WHEN upper(species_cd) IN ('AT', 'SS', 'SB', 'PA') THEN lower(species_full_name)
			WHEN upper(species_cd) IN ('D', 'DR') THEN 'alder'
			WHEN upper(species_cd) LIKE 'F%' THEN 'fir'
			WHEN upper(species_cd) LIKE 'C%' then 'cedar'
			WHEN upper(species_cd) LIKE 'H%' then 'hemlock'
			WHEN upper(species_cd) LIKE 'S%' then 'spruce'
			WHEN upper(species_cd) LIKE 'B%' then 'balsam'
			WHEN upper(species_cd) IN ('PF','PW') then 'white pine'
			WHEN upper(species_cd) IN ('P','PL','PLI','PLC', 'PJ') then 'pine'
			WHEN upper(species_cd) IN ('PY') then 'yellow pine'
			WHEN upper(species_cd)  LIKE 'L%' then 'larch'
			WHEN upper(species_cd) IN ('YC') then 'cypress'
			ELSE 'other'
		END as species_grouping,
		species_cd,
		species_full_name,
		type
	FROM
		{dst_schema}.vri_species_cd_datadict			   
)
SELECT
	vri_key.gr_skey,
	vri.species_cd_1,
	non_com.species_grouping,
	non_com.non_commercial,
	man_unit.man_unit
FROM
{dst_schema}.veg_comp_lyr_r1_poly_internal_gr_skey vri_key
LEFT JOIN {dst_schema}.veg_comp_lyr_r1_poly_internal vri USING (pgid)
LEFT JOIN vri_species_cd_datadict on vri.species_cd_1 = vri_species_cd_datadict.species_cd
LEFT JOIN {dst_schema}.man_unit_gr_skey man_unit on man_unit.gr_skey = vri_key.gr_skey
LEFT JOIN {dst_schema}.non_commercial_lu_table non_com on non_com.tsa = man_unit.tsa_rank1 and vri_species_cd_datadict.species_grouping = non_com.species_grouping;")
run_sql_r(query, conn_list)