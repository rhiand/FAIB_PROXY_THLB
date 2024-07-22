library(RPostgres)
library(glue)
library(faibDataManagement)
start_time <- Sys.time()
print(glue("Script started at {format(start_time, '%Y-%m-%d %I:%M:%S %p')}"))
## relies on install_github("bcgov/FAIB_DATA_MANAGEMENT") being installed at some point
conn_list <- faibDataManagement::get_pg_conn_list()
## relies on the keyring connList being populated
conn <- DBI::dbConnect(conn_list["driver"][[1]],
				host = conn_list["host"][[1]],
								user = conn_list["user"][[1]],
								dbname = conn_list["dbname"][[1]],
								password = conn_list["password"][[1]],
								port = conn_list["port"][[1]])


## to be created into a tif
## vri site index
query <- "select site_index::int as raster_value, st_buffer(geom, 50, 'endcap=square') from whse.veg_comp_lyr_r1_poly_internal_2022_gr_skey vri_key LEFT JOIN whse.veg_comp_lyr_r1_poly_internal_2022 vri ON vri_key.pgid = vri.pgid LEFT JOIN whse.all_bc_gr_skey geom ON geom.gr_skey = vri_key.gr_skey "
## slope
query <- "select val::int as raster_value, st_buffer(geom, 50, 'endcap=square') from whse.slope_gr_skey slope_key LEFT JOIN whse.all_bc_gr_skey geom ON geom.gr_skey = slope_key.gr_skey"
## thlb current minus protected and prohibited
query <- "select CASE WHEN rr.rr_restriction_rollup in ('01_Protected', '02_Prohibited') THEN 0 ELSE thlb.thlb_fact END AS raster_value, st_buffer(geom, 50, 'endcap=square') from whse.all_thlb thlb left join whse.all_thlb_gr_skey thlb_key using (pgid) left join whse.all_bc_gr_skey g on g.gr_skey = thlb_key.gr_skey LEFT JOIN whse.rr_restriction_gr_skey rr_key on thlb_key.gr_skey = rr_key.gr_skey LEFT JOIN whse.rr_restriction rr on rr_key.pgid = rr.pgid"
## proxy THLB minus protected and prohibited and high restricted * 0.15
query <- "SELECT CASE  WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited') OR harvest_lu.rollup_name IS NULL THEN 0 WHEN harvest_lu.rollup_name IN ('High Restricted') THEN 0.15 WHEN coast.pgid IS NULL THEN CASE  WHEN slope_key.val > 50 THEN 0 ELSE og.tp_falb END ELSE CASE  WHEN slope_key.val > 70 THEN 0 ELSE og.tp_falb END END as raster_value, st_buffer(geom, 50, 'endcap=square')  FROM ogsr.og_prov_res og LEFT JOIN  whse.rr_restriction_gr_skey harvest_key USING (gr_skey) LEFT JOIN whse.rr_restriction harvest ON harvest.pgid = harvest_key.pgid LEFT JOIN whse.rr_restriction_three_zone harvest_lu ON harvest_lu.rr_restriction = harvest.rr_restriction LEFT JOIN whse.coast_forest_act_gr_skey coast_key ON coast_key.gr_skey = og.gr_skey LEFT JOIN whse.coast_forest_act coast ON coast.pgid = coast_key.pgid LEFT JOIN whse.slope_gr_skey slope_key ON og.gr_skey = slope_key.gr_skey LEFT JOIN whse.all_bc_gr_skey geom ON geom.gr_skey = og.gr_skey"
## proxy THLB minus slope
query <- "SELECT CASE  WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited') OR harvest_lu.rollup_name IS NULL THEN 0 WHEN harvest_lu.rollup_name IN ('High Restricted') THEN 0.15 WHEN coast.pgid IS NULL THEN CASE  WHEN slope_key.val > 50 THEN 0 ELSE og.tp_falb END ELSE CASE  WHEN slope_key.val > 70 THEN 0 ELSE og.tp_falb END END as raster_value, st_buffer(geom, 50, 'endcap=square')  FROM ogsr.og_prov_res og LEFT JOIN  whse.rr_restriction_gr_skey harvest_key USING (gr_skey) LEFT JOIN whse.rr_restriction harvest ON harvest.pgid = harvest_key.pgid LEFT JOIN whse.rr_restriction_three_zone harvest_lu ON harvest_lu.rr_restriction = harvest.rr_restriction LEFT JOIN whse.coast_forest_act_gr_skey coast_key ON coast_key.gr_skey = og.gr_skey LEFT JOIN whse.coast_forest_act coast ON coast.pgid = coast_key.pgid LEFT JOIN whse.slope_gr_skey slope_key ON og.gr_skey = slope_key.gr_skey LEFT JOIN whse.all_bc_gr_skey geom ON geom.gr_skey = og.gr_skey"
## proxy THLB minus vri
query <- "SELECT CASE WHEN harvest_lu.rollup_name IN ('Protected', 'Prohibited') OR harvest_lu.rollup_name IS NULL THEN 0 WHEN harvest_lu.rollup_name IN ('High Restricted') THEN 0.15 WHEN coast.pgid IS NULL THEN CASE WHEN slope_key.val > 50 THEN 0 WHEN vri.site_index < 7 THEN 0 ELSE og.tp_falb END ELSE CASE WHEN slope_key.val > 70 THEN 0 WHEN vri.site_index < 12 then 0 ELSE og.tp_falb END END as raster_value, st_buffer(geom, 50, 'endcap=square') FROM ogsr.og_prov_res og LEFT JOIN whse.rr_restriction_gr_skey harvest_key USING (gr_skey) LEFT JOIN whse.rr_restriction harvest ON harvest.pgid = harvest_key.pgid LEFT JOIN whse.rr_restriction_three_zone harvest_lu ON harvest_lu.rr_restriction = harvest.rr_restriction LEFT JOIN whse.coast_forest_act_gr_skey coast_key ON coast_key.gr_skey = og.gr_skey LEFT JOIN whse.coast_forest_act coast ON coast.pgid = coast_key.pgid LEFT JOIN whse.slope_gr_skey slope_key ON og.gr_skey = slope_key.gr_skey LEFT JOIN whse.veg_comp_lyr_r1_poly_internal_2022_gr_skey vri_key ON vri_key.gr_skey = og.gr_skey LEFT JOIN whse.veg_comp_lyr_r1_poly_internal_2022 vri ON vri_key.pgid = vri.pgid LEFT JOIN whse.all_bc_gr_skey geom ON geom.gr_skey = og.gr_skey"
