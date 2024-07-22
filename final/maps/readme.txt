April, 25, 2024
Hailey Eckstrand, FAIB

THLB tif resultant from this query from Hailey's db:

select thlb.thlb_fact as raster_value, st_buffer(geom, 50, 'endcap=square') from whse.all_thlb thlb left join whse.all_thlb_gr_skey thlb_key using (pgid) left join whse.all_bc_gr_skey g on g.gr_skey = thlb_key.gr_skey