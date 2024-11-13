library(RPostgres)
library(glue)
library(devtools)
# install_github("bcgov/FAIB_DADMTOOLS")
library(dadmtools)
source('src/utils/functions.R')
## relies on install_github("bcgov/FAIB_DATA_MANAGEMENT") being installed at some point
conn_list <- dadmtools::get_pg_conn_list()

query <- "DROP TABLE IF EXISTS thlb_proxy.seral_2023_tap_method;"
run_sql_r(query, conn_list)


#####  FALB is a compilation of the following datasets: 
#####  bec: bec_biogeoclimatic_poly
#####  vri: veg_comp_lyr_r1_poly
#####  consolidated cutblocks: veg_consolidated_cut_blocks_sp
#####  btm: btm_present_land_use_v1_svw
#####  f_own: F_OWN (Forest ownership)
#####  
#####  FALB uses the VRI field: for_mgmt_land_base_ind as its base with some updates. Note the for_mgmt_land_base_ind field is a text field which contains either 'Y' or 'N'.
#####  
#####  Updates:
#####  + 'N' if f_own.own in (40, 41, 52, 80)
#####  + 'N' if BEC.natural_disturbance = 'NDT5'
#####  + 'Y' if harvest IS TRUE, (harvest is defined as: harvest is TRUE if vri.harvest_date is not null OR consolidated cutblocks exist OR vri.opening_id or vri.opening_number IS NOT NULL or '0' otherwise FALSE)
#####  + 'Y' if (vri.bclcs_level_1 IN ('U', '') AND btm.present_land_use_label in ('Old Forest', 'Recently Logged', 'Selectively Logged', 'Young Forest'))
#####  + 'N' if bclcs_level_1 = 'V' AND bclcs_level_2 <> 'T' and vri.project NOT LIKE 'FIRE_UPDATE%'
#####  + 'N' if URBAN is true AND vri.for_mgmt_land_base_ind = 'Y', (URBAN defined as: URBAN IS TRUE if vri.non_productive_descriptor_cd = 'U' OR vri.bclcs_level_5 = 'UR' OR vri.land_cover_class_cd_1 = 'UR' OR vri.land_cover_class_cd_2 = 'UR' vri.OR land_cover_class_cd_3 = 'UR')
#####  OTHERWISE vri.for_mgmt_land_base_ind

query <- "CREATE TABLE thlb_proxy.seral_2023_tap_method AS
WITH r1 AS (
SELECT
	g.gr_skey, 
	v.feature_id, 
	v.pgid, 
	v.inventory_standard_cd AS inv_std_cd,
	v.for_mgmt_land_base_ind AS fmlb_orig, 
	v.non_productive_cd AS np_cd,
	v.non_productive_descriptor_cd AS np_desc,
	v.opening_number,
	v.opening_id, 
-- 	line_7b_disturbance_history AS l7b, 
	v.project, 
	v.bclcs_level_1 AS bclcs_1, 
	v.bclcs_level_2 AS bclcs_2,
	v.bclcs_level_3 AS bclcs_3,
	v.bclcs_level_4 AS bclcs_4,
	v.bclcs_level_5 AS bclcs_5, 
	v.crown_closure AS cc, 
	v.site_index AS si, 
	v.est_site_index AS est_si,
	v.land_cover_class_cd_1, 
	v.land_cover_class_cd_2,
	v.land_cover_class_cd_3,
	v.non_veg_cover_type_1,
	v.earliest_nonlogging_dist_type,
	v.species_cd_1 AS spc_cd_1, 
	v.stand_percentage_dead,
	v.proj_age_1, 
	v.reference_year, 
	extract(year from v.attribution_base_date) AS attribution_year, 
	--bec
	----------------------------------------------------------
	bec.bgc_label AS bgc_label,
	bec.zone AS bec_zone, 
	bec.subzone AS bec_subzone, 
	bec.variant AS bec_variant, 
	bec.natural_disturbance AS ndt,
	btm.present_land_use_label AS btm,
	--------------------------------------------------------
	---- BURN
	---- fire_year description: This is the fire_year from veg_burn_severity_sp, veg_burn_severity_same_yr_sp, respectively, if either severity rating is either HIGH or MEDIUM, otherwise, NULL
	---- burn_severity_rating description: This is the burn_severity from veg_burn_severity_sp, veg_burn_severity_same_yr_sp, respectively, if either severity rating is either HIGH or MEDIUM, otherwise, NULL

	---- FIRE
	---- fire_year description: This is the fire_year from fire_severity_compilation (manually compiled fire data from 2007-2023), if either severity rating is either HIGH or MEDIUM, otherwise, NULL
	---- burn_severity_rating description: This is the burn_severity from fire_severity_compilation (manually compiled fire data from 2007-2023), if either severity rating is either HIGH or MEDIUM, otherwise, NULL

	coalesce(burn.fire_year, fire.fire_year) AS fire_year, 
	coalesce(burn.burn_severity_rating, fire.burn_severity) AS burn_severity_rating,
	--------------------------------------------------------
	----  HARVEST boolean field description: harvest is TRUE if vri.harvest_date is not null OR consolidated cutblocks exist OR vri.opening_id or vri.opening_number IS NOT NULL or '0' otherwise FALSE
	(nullif(v.opening_id::text,'0') is not null or nullif(v.opening_number::text,'0') is not null or v.harvest_date is not null or ccg.gr_skey is not null) AS harvest, 
	CASE 
		WHEN nullif(v.opening_id::text,'0') is not null THEN 'harvest: vri opening_id present'
		WHEN nullif(v.opening_number::text,'0') is not null THEN 'harvest: vri opening_number present'
		WHEN v.harvest_date is not null THEN 'harvest: vri harvest date present'
		WHEN ccg.gr_skey is not null THEN 'harvest: cutblock present'
	END AS harvest_desc, 
	cc.harvest_year
FROM 
whse.all_bc_gr_skey g 
LEFT JOIN whse.veg_comp_lyr_r1_poly_internal_2023_gr_skey vg on vg.gr_skey = g.gr_skey 
LEFT JOIN whse.veg_comp_lyr_r1_poly_internal_2023 v on v.pgid = vg.pgid
LEFT JOIN thlb_proxy.btm_present_land_use_v1_svw_gr_skey btmg on btmg.gr_skey = g.gr_skey 
LEFT JOIN thlb_proxy.btm_present_land_use_v1_svw btm on btm.pgid = btmg.pgid
LEFT JOIN whse.veg_consolidated_cut_blocks_sp_gr_skey ccg on ccg.gr_skey = g.gr_skey 
LEFT JOIN whse.veg_consolidated_cut_blocks_sp cc on cc.pgid = ccg.pgid 
LEFT JOIN whse.bec_biogeoclimatic_poly_gr_skey bec_key on bec_key.gr_skey = g.gr_skey
LEFT JOIN whse.bec_biogeoclimatic_poly bec ON bec.pgid = bec_key.pgid
--LEFT JOIN (select gr_skey, true AS fwa_wetland from whse.fwa_wetlands_poly_gr_skey) fwa on fwa.gr_skey = g.gr_skey
LEFT JOIN 
	(
		SELECT 
			g.gr_skey,
			COALESCE(CASE	
						WHEN upper(by.burn_severity_rating) NOT IN ('HIGH', 'MEDIUM') THEN null 
						ELSE by.burn_severity_rating 
					END, 
					CASE	
						WHEN upper(b.burn_severity_rating) NOT IN ('HIGH', 'MEDIUM') THEN null 
						ELSE b.burn_severity_rating 
					END) AS burn_severity_rating,
			COALESCE(CASE
						WHEN upper(by.burn_severity_rating) NOT IN ('HIGH', 'MEDIUM') THEN null 
						ELSE by.fire_year
					END,
					CASE 
						WHEN upper(b.burn_severity_rating) NOT IN ('HIGH', 'MEDIUM') THEN null 
						ELSE b.fire_year 
					END) AS fire_year
		--coalesce(by.burn_severity_rating, b.burn_severity_rating) AS burn_severity_rating,
		--coalesce(by.fire_year, b.fire_year) AS fire_year
		FROM whse.all_bc_gr_skey g
		LEFT JOIN thlb_proxy.veg_burn_severity_sp_gr_skey bg on bg.gr_skey = g.gr_skey
		LEFT JOIN thlb_proxy.veg_burn_severity_sp b on b.pgid = bg.pgid 
		LEFT JOIN thlb_proxy.veg_burn_severity_same_yr_sp_gr_skey byg on byg.gr_skey = g.gr_skey 
		LEFT JOIN thlb_proxy.veg_burn_severity_same_yr_sp by on by.pgid = byg.pgid
	) burn ON burn.gr_skey = g.gr_skey 
LEFT JOIN thlb_proxy.fire_severity_compilation_gr_skey fireg on fireg.gr_skey = g.gr_skey 
LEFT JOIN 
	(
		SELECT 
			pgid, 
			CASE 
				WHEN upper(burn_severity) NOT IN ('HIGH', 'MEDIUM') THEN null 
				ELSE burn_severity 
			END AS burn_severity,
			CASE
				WHEN upper(burn_severity) NOT IN ('HIGH', 'MEDIUM') THEN null 
				ELSE fire_year 
			END AS fire_year
		FROM
		thlb_proxy.fire_severity_compilation
	) fire on fire.pgid = fireg.pgid
), r2 AS (
SELECT 
	r1.gr_skey, 
	bec_zone, 
	ndt, 
	btm,
	bclcs_1,
	fmlb_orig, 
	proj_age_1, 
	harvest, 
	fire_year, 
	burn_severity_rating,
	stand_percentage_dead, 
	reference_year, 
	attribution_year, 
	harvest_year, 
	---- fmlb_adj description
	CASE 
		WHEN ndt = 'NDT5' THEN 'N: bec natural disturbance = ndt5'
		WHEN harvest THEN 'Y: '|| harvest_desc
		WHEN (coalesce(bclcs_1,'') IN ('U', '') AND btm IN ('Old Forest', 'Recently Logged', 'Selectively Logged', 'Young Forest')) THEN 'Y: vri lcs unclassified and btm in Old Forest, Recently Logged, Selectively Logged, Young Forest'
		--WHEN upper(bec_zone || bec_subzone) IN ('SWBMKS', 'SWBUNS', 'SWBVKS') then 'N'
		WHEN bclcs_1 = 'V' AND coalesce(bclcs_2,'') <> 'T' AND project NOT LIKE 'FIRE_UPDATE%' THEN 'Y: bclcs_1 = V and bclcs_2 != T and project not FIRE_UPDATE%'
		WHEN ((trim(np_desc) = 'U' OR bclcs_5 = 'UR' OR land_cover_class_cd_1 = 'UR' OR land_cover_class_cd_2 = 'UR' OR land_cover_class_cd_3 = 'UR' OR non_veg_cover_type_1 = 'UR')) AND (coalesce(fmlb_orig,'Y') = 'Y') THEN 'N: URBAN and fmlb_orig = Y'
	ELSE fmlb_orig || '- vri.for_mgmt_land_base_ind'
	END AS fmlb_adj_desc, 
	CASE 
		WHEN ndt = 'NDT5' THEN 'N'
		WHEN harvest THEN 'Y'
		WHEN (coalesce(bclcs_1,'') IN ('U', '') AND btm IN ('Old Forest', 'Recently Logged', 'Selectively Logged', 'Young Forest')) THEN 'Y'
		--WHEN upper(bec_zone || bec_subzone) IN ('SWBMKS', 'SWBUNS', 'SWBVKS') then 'N'
		WHEN bclcs_1 = 'V' AND coalesce(bclcs_2,'') <> 'T' AND project NOT LIKE 'FIRE_UPDATE%' THEN 'N'	
		WHEN ((trim(np_desc) = 'U' OR bclcs_5 = 'UR' OR land_cover_class_cd_1 = 'UR' OR land_cover_class_cd_2 = 'UR' OR land_cover_class_cd_3 = 'UR' OR non_veg_cover_type_1 = 'UR')) AND (coalesce(fmlb_orig,'Y') = 'Y') THEN 'N'
	ELSE fmlb_orig
	END AS fmlb_adj, 
	-------------------------------------------------------------------------------------------------------------------
	/*case 
		when upper(burn_severity_rating_bs) = 'HIGH' and fire_year_bs >= 2017 then  2020 - fire_year_bs  --Age in the recent high severity fires
		when bclcs_level_1 is not null and bclcs_level_1 <> 'U' and harvest_year_cc >= 2000 then (greatest((2020 - harvest_year_cc)-1, 1)) --Reporting standards changed in 2012.  Cut in Consolidate Cutblocks after that date
		when (bclcs_level_1 is null or bclcs_level_1 = 'U') and harvest_year_cc >= 0 then (greatest((2020 - harvest_year_cc)-1, 1)) --For areas outside of VRI (BTM) apply all Consolidated Cutblock ages
		WHEN harvest_date IS NOT NULL THEN (greatest((2020 - EXTRACT(YEAR FROM harvest_DATE))-1, 1))::integer  --Updating ages for forest cover polys with results harvest age data
		else r.proj_age_1
	end AS age_adj,*/
	-------------------------------------------------------------------------------------------------------------------
	CASE 
		WHEN upper(burn_severity_rating) IN ('HIGH', 'MEDIUM') THEN coalesce(extract(year from now()) - fire_year, 1)
		WHEN stand_percentage_dead > 70 THEN greatest(extract(year from now()) - reference_year, 1)
		--when coalesce(harvest_year,0) < attribution_year then coalesce(proj_age_1,1)
		WHEN coalesce(harvest_year,0) >= attribution_year THEN greatest(extract(year from now()) - harvest_year, 1) 
		ELSE proj_age_1 
	END AS age_adj
FROM
	r1
)
SELECT 
    r.gr_skey,
    r.bec_zone,
    r.ndt,
    r.btm,
    r.bclcs_1,
    r.fmlb_orig,
    r.proj_age_1,
    r.harvest,
    r.fire_year,
    r.burn_severity_rating,
    r.stand_percentage_dead,
    r.reference_year,
    r.attribution_year,
    r.harvest_year,
    r.fmlb_adj::boolean::int as fmlb_adj,
	CASE 
		-- when ownership is included in falb definition, use FMLB boolean
		WHEN fown_lut.falb then r.fmlb_adj::boolean::int
		WHEN NOT fown_lut.falb THEN 0
		ELSE r.fmlb_adj::boolean::int
	END falb_adj,
	CASE 
		-- when ownership is included in falb definition, use FMLB boolean
		WHEN fown_lut.falb then r.fmlb_adj_desc
		WHEN NOT fown_lut.falb THEN 'N: ' || fown.own || fown.schedule || ' - ' || fown.ownership_description
		ELSE r.fmlb_adj_desc
	END falb_adj_desc,
	r.age_adj,
	thlb_proxy.faib_seral_bdg_ndt3_140(
	fmlb_adj::text,
	age_adj::int4,
	ndt::text,
	bec_zone::text,
	bclcs_1::text,
	btm::text) AS seral
FROM
	r2 r
LEFT JOIN whse.f_own_gr_skey fown_key on r.gr_skey = fown_key.gr_skey
LEFT JOIN whse.f_own fown USING (pgid)
LEFT JOIN thlb_proxy.f_own_falb_lut fown_lut ON fown_lut.own = fown.own;"
run_sql_r(query, conn_list)

query <- "analyze thlb_proxy.seral_2023_tap_method;"
run_sql_r(query, conn_list)


