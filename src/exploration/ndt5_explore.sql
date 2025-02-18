select
n01_fmlb,
subzone_name,
zone_name,
count(*)
from
thlb_proxy.prov_netdown tsa
LEFT JOIN whse.veg_consolidated_cut_blocks_sp_2024_gr_skey cc_key on cc_key.gr_skey = tsa.gr_skey
LEFT JOIN whse.veg_consolidated_cut_blocks_sp_2024 cc on cc.pgid = cc_key.pgid
LEFT JOIN whse.all_bc_gr_skey g on g.gr_skey = tsa.gr_skey
LEFT JOIN whse.bec_biogeoclimatic_poly_gr_skey bec_key on bec_key.gr_skey = tsa.gr_skey
LEFT JOIN whse.bec_biogeoclimatic_poly bec on bec.pgid = bec_key.pgid
WHERE 
n04_nonfor IS NULL -- forested
and
n01_fmlb is not null -- fmlb: non forested
and cc.pgid is not null -- 2845/998340
group by n01_fmlb,
subzone_name,
zone_name
order by count(*) desc