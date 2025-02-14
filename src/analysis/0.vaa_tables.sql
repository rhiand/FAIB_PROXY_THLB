DROP TABLE IF EXISTS whse.man_unit_gr_skey;
CREATE TABLE whse.man_unit_gr_skey AS
	SELECT
	 	a.gr_skey,
		CASE 
			WHEN LOWER(manlic.forest_file_id) like 'n%'	then 'FNWL - ' || manlic.forest_file_id
			WHEN LOWER(manlic.forest_file_id) like 'k%'	then 'Community Forest - ' ||  manlic.forest_file_id
			WHEN LOWER(manlic.forest_file_id) like 'w%' then 'Woodlot - ' || manlic.forest_file_id
			WHEN tfl.forest_file_id IS NOT NULL then 'TFL ' || replace(tfl.forest_file_id, 'TFL', '')
			ELSE mu_look.tsa_number_description
		END AS man_unit,
		CASE 
			WHEN LOWER(manlic.forest_file_id) like 'n%'	then 'FNWL'
			WHEN LOWER(manlic.forest_file_id) like 'k%'	then 'Community Forest'
			WHEN LOWER(manlic.forest_file_id) like 'w%' then 'Woodlot'
			WHEN tfl.forest_file_id IS NOT NULL then 'TFL'
			ELSE 'TSA'
		END AS tenure_type,
		mu_look.tsa_number_description as tsa_rank1,
		'TFL ' || replace(tfl.forest_file_id, 'TFL', '') as tfl_rank1,
		CASE
			WHEN LOWER(manlic.forest_file_id) like 'n%'	then 'FNWL - ' || manlic.forest_file_id
			WHEN LOWER(manlic.forest_file_id) like 'k%'	then 'Community Forest - ' ||  manlic.forest_file_id
			WHEN LOWER(manlic.forest_file_id) like 'w%' then 'Woodlot - ' || manlic.forest_file_id
			ELSE NULL
		END AS manlic_rank1,
		di.district_name AS district_name,
		LEFT(reg.region_name, -24 ) as region_name,
		nsc.name
	FROM
	whse.all_bc_gr_skey a 
	left join whse.adm_nr_regions_sp_gr_skey regg on a.gr_skey = regg.gr_skey 
	left join whse.adm_nr_regions_sp reg on regg.pgid = reg.pgid

	left join whse.north_south_coast_gr_skey nscg on a.gr_skey = nscg.gr_skey
	left join whse.north_south_coast nsc on nscg.fid = nsc.fid

	left join whse.adm_nr_districts_sp_gr_skey dig on a.gr_skey = dig.gr_skey 
	left join whse.adm_nr_districts_sp di on dig.pgid = di.pgid

	left join whse.tsa_boundaries_gr_skey tg on tg.gr_skey = a.gr_skey 
	left join whse.tsa_boundaries tsa on tsa.pgid = tg.pgid 

	left join whse.fadm_tfl_all_sp_gr_skey tflg on tflg.gr_skey = a.gr_skey 
	left join whse.fadm_tfl_all_sp tfl on tfl.pgid = tflg.pgid

	left join whse.ften_managed_licence_poly_svw_gr_skey manlico on manlico.gr_skey = a.gr_skey 
	left join whse.ften_managed_licence_poly_svw manlic on manlic.pgid = manlico.pgid 

	left join whse.mu_lookup_table_im mu_look on tsa.tsa_number::integer = mu_look.tsa_number::integer;

ALTER TABLE whse.man_unit_gr_skey ADD PRIMARY KEY (gr_skey);

COMMENT ON TABLE whse.man_unit_gr_skey
    IS 'Last ran 2025-02-06';