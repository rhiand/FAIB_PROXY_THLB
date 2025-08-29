##  age_adj snippet snagged from: \\spatialfiles2.bcgov\archive\FOR\VIC\HTS\DAM\Staff_Workarea\mwfowler\Projects\Old_Growth_Analysis\Reporting\Seral_2020_2023\Code\FAIB_FMLB_TAP_2020_Method.sql
library(dadmtools)
conn_list <- dadmtools::get_pg_conn_list()
## Note: the function: import_bcgw_to_pg relies on he oracle foreign server: oradb existing. If working on a fresh db that doesn't have the oracle foreign server set up yet - be sure to import layers using dadmtools first as it will set up the oracle foreign server

dst_schema <- "whse"
vector_schema <- "whse_vector"


query <- glue("drop table if exists {dst_schema}.fmlb;")
run_sql_r(query, conn_list)

query <- glue("CREATE TABLE {dst_schema}.fmlb AS
SELECT
	g.gr_skey,
	CASE 
		-- knock out BEC NDT5 — Alpine Tundra and Subalpine Parkland
		WHEN bec.natural_disturbance = 'NDT5' THEN 'N'
		-- keep in any RESULTS openings or consolidated cutblocks
		WHEN (nullif(vri.opening_id::text,'0') is not null or cc.harvest_start_year_calendar is not null) THEN 'Y'
		-- knock out spruce, willow, birch shrublands
		WHEN upper(bec.zone || bec.subzone) IN ('SWBMKS', 'SWBUNS', 'SWBVKS') then 'N'
		-- knock out all bec bunch grass
		WHEN bec.zone = 'BG' THEN 'N'
		-- current & historical lidar
		-- include in FMLB if >= 10% treed & napercent < 25%
		-- exclude from FMLB if < 10% treed percent & na percent < 25%
		WHEN current_lidar.treedpercent >= 10 and current_lidar.napercent < 25 then 'Y'
		WHEN current_lidar.treedpercent < 10 and current_lidar.napercent < 25 then 'N'
		WHEN hx_lidar.treedpercent >= 10 and hx_lidar.napercent < 25 then 'Y'
		WHEN hx_lidar.treedpercent < 10 and hx_lidar.napercent < 25 then 'N'
		-- keep in where TFL 19, btm forest and inv != F or orig FMLB is null
		-- known old growth in TFL19 where old inventory
		WHEN 
			tfl.forest_file_id ='TFL19' and 
			btm.present_land_use_label in ('Old Forest', 'Recently Logged', 'Selectively Logged', 'Young Forest') AND
			((vri.for_mgmt_land_base_ind = 'N' and vri.inventory_standard_cd = 'F') OR (vri.for_mgmt_land_base_ind IS NULL))
		THEN 'Y'
		-- keep in known areas of forest in strathcona park 
		WHEN 
			park.protected_lands_name = 'STRATHCONA PARK' AND 
			vri.bclcs_level_1 ='V' AND 
			vri.bclcs_level_2 = 'T' AND 
			vri.site_index > 4 AND
			vri.for_mgmt_land_base_ind ='N' 
		THEN 'Y'
		-- keep in where VRI is unreported but btm present land use is forested
		WHEN 
			(coalesce(vri.bclcs_level_1,'U') = 'U' AND 
			btm.present_land_use_label IN ('Old Forest', 'Recently Logged', 'Selectively Logged', 'Young Forest')) 
		THEN 'Y'
		-- knock out where original for_mgmt_land_base_ind = 'Y' but other fields denote unreported
		WHEN 
			((trim(vri.non_productive_descriptor_cd) = 'U' OR 
			vri.bclcs_level_5 = 'UR' OR 
			vri.land_cover_class_cd_1 = 'UR' OR 
			vri.land_cover_class_cd_2 = 'UR' OR 
			vri.land_cover_class_cd_3 = 'UR' OR 
			vri.non_veg_cover_type_1 = 'UR')) AND 
			(coalesce(vri.for_mgmt_land_base_ind,'Y') = 'Y') 
		THEN 'N'
		-- knock out where bclcs_level_1 is Non Vegetated and not beetle kill or wildfire
		-- don't want to switch something to N when it is beetle kill or wildfire
		WHEN 
			coalesce(vri.bclcs_level_1, 'U') = 'N' AND 
			COALESCE(earliest_nonlogging_dist_type, 'ZZ') not in ('IBM', 'B', 'NB')  
		THEN 'N'
		-- keep in veg / non treed / CC < 10 / land cover class IS NULL / not beetle kill (IBM) or wildfire (B, NB) / FMLB original = 'Y' / BTM forested
		WHEN 
			vri.BCLCS_LEVEL_1 = 'V' AND 
			COALESCE(vri.BCLCS_LEVEL_2, '') <> 'T' AND 
			vri.FOR_MGMT_LAND_BASE_IND = 'Y' AND 
			vri.CROWN_CLOSURE < 10 AND 
			vri.LAND_COVER_CLASS_CD_1 IS NULL AND
			btm.present_land_use_label in ('Old Forest', 'Recently Logged', 'Selectively Logged', 'Young Forest') AND
			COALESCE(earliest_nonlogging_dist_type, 'ZZ') not in ('IBM', 'B', 'NB')
		THEN 'Y'
		-- knock out veg / non treed / CC < 10 / land cover class not tree classes / not beetle kill (IBM) or wildfire (B, NB) / FMLB original = 'Y'
		WHEN 
			vri.BCLCS_LEVEL_1 = 'V' AND 
			COALESCE(vri.BCLCS_LEVEL_2, '') <> 'T' AND 
			vri.FOR_MGMT_LAND_BASE_IND = 'Y' AND 
			vri.CROWN_CLOSURE < 10 AND 
			coalesce(vri.LAND_COVER_CLASS_CD_1, '') NOT IN ('TB', 'TC', 'TM') AND 
			COALESCE(earliest_nonlogging_dist_type, 'ZZ') not in ('IBM', 'B', 'NB') 
		THEN 'N'
		ELSE vri.for_mgmt_land_base_ind
	END AS fmlb_adj,
		CASE 
			-- knock out BEC NDT5 — Alpine Tundra and Subalpine Parkland
			WHEN bec.natural_disturbance = 'NDT5' THEN 'N: NDT5'
			-- keep in any RESULTS openings or consolidated cutblocks
			WHEN (nullif(vri.opening_id::text,'0') is not null or cc.harvest_start_year_calendar is not null) THEN 'Y: vri opening_id or cc harvest year exists'
			-- knock out spruce, willow, birch shrublands
			WHEN upper(bec.zone || bec.subzone) IN ('SWBMKS', 'SWBUNS', 'SWBVKS') then 'N: SWB scrublands'
			-- knock out all BEC bunch grass
			WHEN bec.zone = 'BG' THEN 'N: bunchgrass'
			-- current & historical lidar
			-- include in FMLB if >= 10% treed & napercent < 25%
			-- exclude from FMLB if < 10% treed percent & na percent < 25%
			WHEN current_lidar.treedpercent >= 10 and current_lidar.napercent < 25 then 'Y: Lidar >= 10% treed%'
			WHEN current_lidar.treedpercent < 10 and current_lidar.napercent < 25 then 'N: Lidar < 10% treed %'
			WHEN hx_lidar.treedpercent >= 10 and hx_lidar.napercent < 25 then 'Y: Lidar >= 10% treed%'
			WHEN hx_lidar.treedpercent < 10 and hx_lidar.napercent < 25 then 'N: Lidar < 10% treed %'
			-- keep in where TFL 19, btm forest and inv != F or orig FMLB is null
			-- known old growth in TFL19 where old inventory
			WHEN 
				tfl.forest_file_id ='TFL19' and 
				((vri.for_mgmt_land_base_ind = 'N' and vri.inventory_standard_cd = 'F') OR (vri.for_mgmt_land_base_ind IS NULL)) and 
				btm.present_land_use_label in ('Old Forest', 'Recently Logged', 'Selectively Logged', 'Young Forest') 
			THEN 'Y: TFL19, orig FMLB = N, inv code = F, btm forested'
			-- keep in known areas of forest in strathcona park 
			WHEN 
				park.protected_lands_name = 'STRATHCONA PARK' AND 
				vri.bclcs_level_1 ='V' AND 
				vri.bclcs_level_2 = 'T' AND 
				vri.site_index > 4 AND
				vri.for_mgmt_land_base_ind ='N' 
			THEN 'Y: strathcona'
			-- keep in where VRI is unreported but btm present land use is forested
			WHEN 
				(coalesce(vri.bclcs_level_1,'U') = 'U' AND 
				btm.present_land_use_label IN ('Old Forest', 'Recently Logged', 'Selectively Logged', 'Young Forest')) 
			THEN 'Y: BTM forest, bclcs unreported'
			-- knock out where original for_mgmt_land_base_ind = 'Y' but other fields denote unreported
			WHEN 
				((trim(vri.non_productive_descriptor_cd) = 'U' OR 
				vri.bclcs_level_5 = 'UR' OR 
				vri.land_cover_class_cd_1 = 'UR' OR 
				vri.land_cover_class_cd_2 = 'UR' OR 
				vri.land_cover_class_cd_3 = 'UR' OR 
				vri.non_veg_cover_type_1 = 'UR')) AND 
				(coalesce(vri.for_mgmt_land_base_ind,'Y') = 'Y') 
			THEN 'N: urban etc'
			-- knock out where bclcs_level_1 is Non Vegetated and not beetle kill or wildfire
			-- don't want to switch something to N when it is beetle kill or wildfire
			WHEN 
				coalesce(vri.bclcs_level_1, 'U') = 'N' AND 
				COALESCE(earliest_nonlogging_dist_type, 'ZZ') not in ('IBM', 'B', 'NB')  
			THEN 'N: bclcs nonveg, not beetle kill or burn'
			-- keep in veg / non treed / CC < 10 / land cover class IS NULL / not beetle kill (IBM) or wildfire (B, NB) / FMLB original = 'Y' / forested according to feds
			WHEN 
				vri.BCLCS_LEVEL_1 = 'V' AND 
				COALESCE(vri.BCLCS_LEVEL_2, '') <> 'T' AND 
				vri.FOR_MGMT_LAND_BASE_IND = 'Y' AND 
				vri.CROWN_CLOSURE < 10 AND 
				vri.LAND_COVER_CLASS_CD_1 IS NULL AND
				btm.present_land_use_label in ('Old Forest', 'Recently Logged', 'Selectively Logged', 'Young Forest') AND
				COALESCE(earliest_nonlogging_dist_type, 'ZZ') not in ('IBM', 'B', 'NB')
			THEN 'Y: veg, non treed, BTM forested'
			-- knock out veg / non treed / CC < 10 / land cover class not tree classes / not beetle kill (IBM) or wildfire (B, NB) / FMLB original = 'Y'
			-- https://archive.ipcc.ch/ipccreports/sres/land_use/index.php?idp=124
			WHEN 
				vri.BCLCS_LEVEL_1 = 'V' AND 
				COALESCE(vri.BCLCS_LEVEL_2, '') <> 'T' AND 
				vri.FOR_MGMT_LAND_BASE_IND = 'Y' AND 
				vri.CROWN_CLOSURE < 10 AND 
				coalesce(vri.LAND_COVER_CLASS_CD_1, '') NOT IN ('TB', 'TC', 'TM') AND 
				COALESCE(earliest_nonlogging_dist_type, 'ZZ') not in ('IBM', 'B', 'NB')
			THEN 'N: veg, nontreed etc'
			ELSE vri.for_mgmt_land_base_ind || ': for_mgmt_land_base_ind'
		END AS fmlb_adj_desc,
	CASE 
		-- knock out BEC NDT5 — Alpine Tundra and Subalpine Parkland
		WHEN bec.natural_disturbance = 'NDT5' THEN 'BEC NDT5'
		-- keep in any RESULTS openings or consolidated cutblocks
		WHEN (nullif(vri.opening_id::text,'0') is not null or cc.harvest_start_year_calendar is not null) THEN NULL
		-- knock out spruce, willow, birch shrublands
		WHEN upper(bec.zone || bec.subzone) IN ('SWBMKS', 'SWBUNS', 'SWBVKS') then 'BEC spruce-willow-birch shrublands'
		-- knock out all bec bunch grass
		WHEN bec.zone = 'BG' THEN 'BEC bunchgrass'
		-- current & historical lidar
		-- include in FMLB if >= 10% treed & napercent < 25%
		-- exclude from FMLB if < 10% treed percent & na percent < 25%
		WHEN current_lidar.treedpercent >= 10 and current_lidar.napercent < 25 then NULL
		WHEN current_lidar.treedpercent < 10 and current_lidar.napercent < 25 then 'Lidar < 10% treed %'
		WHEN hx_lidar.treedpercent >= 10 and hx_lidar.napercent < 25 then NULL
		WHEN hx_lidar.treedpercent < 10 and hx_lidar.napercent < 25 then 'Lidar < 10% treed %'
		-- keep in where VRI is unreported but btm present land use is forested
		-- keep in where TFL 19, btm forest and inv != F or orig FMLB is null
		-- known old growth in TFL19 where old inventory
		WHEN 
			tfl.forest_file_id ='TFL19' and 
			((vri.for_mgmt_land_base_ind = 'N' and vri.inventory_standard_cd = 'F') OR (vri.for_mgmt_land_base_ind IS NULL)) and 
			btm.present_land_use_label in ('Old Forest', 'Recently Logged', 'Selectively Logged', 'Young Forest') 
		THEN NULL
		-- keep in known areas of forest in strathcona park 
		WHEN 
			park.protected_lands_name = 'STRATHCONA PARK' AND 
			vri.bclcs_level_1 ='V' AND 
			vri.bclcs_level_2 = 'T' AND 
			vri.site_index > 4 AND
			vri.for_mgmt_land_base_ind ='N' 
		THEN NULL
		-- keep in where VRI is unreported but btm present land use is forested
		WHEN 
			(coalesce(vri.bclcs_level_1,'U') = 'U' AND 
			btm.present_land_use_label IN ('Old Forest', 'Recently Logged', 'Selectively Logged', 'Young Forest')) 
		THEN NULL
		-- knock out where original for_mgmt_land_base_ind = 'Y' but other fields denote unreported
		WHEN 
			((trim(vri.non_productive_descriptor_cd) = 'U' OR 
			vri.bclcs_level_5 = 'UR' OR 
			vri.land_cover_class_cd_1 = 'UR' OR 
			vri.land_cover_class_cd_2 = 'UR' OR 
			vri.land_cover_class_cd_3 = 'UR' OR 
			vri.non_veg_cover_type_1 = 'UR')) AND 
			(coalesce(vri.for_mgmt_land_base_ind,'Y') = 'Y') 
		THEN 'unreported/urban'
		-- knock out where bclcs_level_1 is Non Vegetated and not beetle kill or wildfire
		-- don't want to switch something to N when it is beetle kill or wildfire
		WHEN 
			coalesce(vri.bclcs_level_1, 'U') = 'N' AND 
			COALESCE(earliest_nonlogging_dist_type, 'ZZ') not in ('IBM', 'B', 'NB')  
		THEN 'bclcs nonveg, not beetle kill or burn'
		-- keep in veg / non treed / CC < 10 / land cover class IS NULL / not beetle kill (IBM) or wildfire (B, NB) / FMLB original = 'Y' / forested according to feds
		WHEN 
			vri.BCLCS_LEVEL_1 = 'V' AND 
			COALESCE(vri.BCLCS_LEVEL_2, '') <> 'T' AND 
			vri.FOR_MGMT_LAND_BASE_IND = 'Y' AND 
			vri.CROWN_CLOSURE < 10 AND 
			vri.LAND_COVER_CLASS_CD_1 IS NULL AND
			btm.present_land_use_label in ('Old Forest', 'Recently Logged', 'Selectively Logged', 'Young Forest') AND
			COALESCE(earliest_nonlogging_dist_type, 'ZZ') not in ('IBM', 'B', 'NB')
		THEN NULL
		-- knock out veg / non treed / CC < 10 / land cover class not tree classes / not beetle kill (IBM) or wildfire (B, NB) / FMLB original = 'Y'
		-- https://archive.ipcc.ch/ipccreports/sres/land_use/index.php?idp=124
		WHEN 
			vri.BCLCS_LEVEL_1 = 'V' AND 
			COALESCE(vri.BCLCS_LEVEL_2, '') <> 'T' AND 
			vri.FOR_MGMT_LAND_BASE_IND = 'Y' AND 
			vri.CROWN_CLOSURE < 10 AND 
			coalesce(vri.LAND_COVER_CLASS_CD_1, '') NOT IN ('TB', 'TC', 'TM') AND 
			COALESCE(earliest_nonlogging_dist_type, 'ZZ') not in ('IBM', 'B', 'NB')
		THEN 'veg, nontreed etc'
		WHEN vri.for_mgmt_land_base_ind  = 'N' then 'for_mgmt_land_base_ind = N'
		WHEN vri.for_mgmt_land_base_ind  = 'Y' then NULL
		END AS netdown_fmlb
	FROM
	{dst_schema}.all_bc_gr_skey g 
	JOIN {dst_schema}.veg_comp_lyr_r1_poly_internal_gr_skey vri_key on vri_key.gr_skey = g.gr_skey
	LEFT JOIN {dst_schema}.veg_comp_lyr_r1_poly_internal vri on vri.pgid = vri_key.pgid
	LEFT JOIN {dst_schema}.veg_consolidated_cut_blocks_sp_gr_skey cc_key on cc_key.gr_skey = g.gr_skey
	LEFT JOIN {dst_schema}.veg_consolidated_cut_blocks_sp cc on cc.pgid = cc_key.pgid	
	LEFT JOIN {dst_schema}.bec_biogeoclimatic_poly_gr_skey bec_key on bec_key.gr_skey = g.gr_skey
	LEFT JOIN {dst_schema}.bec_biogeoclimatic_poly bec on bec.pgid = bec_key.pgid
	LEFT JOIN {dst_schema}.btm_present_land_use_v1_svw_gr_skey btm_key on btm_key.gr_skey = g.gr_skey
	LEFT JOIN {dst_schema}.btm_present_land_use_v1_svw btm on btm.pgid = btm_key.pgid
	LEFT JOIN {dst_schema}.fadm_tfl_all_sp_gr_skey tfl_key on tfl_key.gr_skey = g.gr_skey
	LEFT JOIN {dst_schema}.fadm_tfl_all_sp tfl on tfl.pgid = tfl_key.pgid
	LEFT JOIN {dst_schema}.ta_park_ecores_pa_svw_gr_skey park_key on park_key.gr_skey = g.gr_skey
	LEFT JOIN {dst_schema}.ta_park_ecores_pa_svw park on park.pgid = park_key.pgid
	LEFT JOIN {dst_schema}.gaps_current_noncurrent_lidar_treedpercent current_lidar on current_lidar.feature_id = vri.feature_id
	LEFT JOIN {dst_schema}.gaps_lidarprogam_contained_treedpercent hx_lidar on hx_lidar.feature_id = vri.feature_id")

	run_sql_r(query, conn_list)