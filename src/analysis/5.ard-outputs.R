library(dadmtools)
library(dplyr)
source('src/utils/functions.R')
## one time runs are commented out to help with rerunning code
conn_list <- dadmtools::get_pg_conn_list()

## raster exports:
source('src/utils/GenerateTIFFS.R')

## vector outputs
## riparian
output_dir <- "S:\\FOR\\VIC\\HTS\\FAIB_DATA_FOR_DISTRIBUTION\\THLB\\THLB_Proxy"
system(glue("ogr2ogr -overwrite -f \"FileGDB\" {output_dir}\\provincial_riparian_buffers.gdb PG:\"dbname='{conn_list$dbname}' host='{conn_list$host}' user='{conn_list$user}' password='{conn_list$password}'\"  -sql \"SELECT * FROM whse_vector.riparian_buffers\" -nlt MULTIPOLYGON -nln riparian_buffers"))

query <- "CREATE TABLE public.provincial_pthlb AS
WITH first_dissolve AS (
SELECT
	ST_Union(st_buffer(b.geom, 50, 'endcap=square')) as geom,
	p.pthlb_net as pthlb_fact,
	p.version,
	p.man_unit
FROM
	whse.thlb_proxy_netdown p 
JOIN
	whse.all_bc_gr_skey b USING (gr_skey)
GROUP BY 
	p.pthlb_net,
	p.version,
	p.man_unit
	)
SELECT
	(ST_Dump(geom)).geom as geom,
	pthlb_fact,
	version,
	man_unit
FROM
	first_dissolve"

run_sql_r(query, conn_list)

query <- "CREATE TABLE public.provincial_paflb AS
WITH first_dissolve AS (
SELECT
	ST_Union(st_buffer(b.geom, 50, 'endcap=square')) as geom,
	p.paflb as paflb_fact,
	p.version,
	p.man_unit
FROM
	whse.thlb_proxy_netdown p 
JOIN
	whse.all_bc_gr_skey b USING (gr_skey)
GROUP BY 
	p.paflb,
	p.version,
	p.man_unit
	)
SELECT
	(ST_Dump(geom)).geom as geom,
	paflb,
	version,
	man_unit
FROM
	first_dissolve"

run_sql_r(query, conn_list)

query <- "CREATE TABLE public.provincial_fmlb AS
WITH first_dissolve AS (
SELECT
	ST_Union(st_buffer(b.geom, 50, 'endcap=square')) as geom,
	p.fmlb,
	p.version,
	p.man_unit
FROM
	whse.thlb_proxy_netdown p 
JOIN
	whse.all_bc_gr_skey b USING (gr_skey)
GROUP BY 
	p.fmlb,
	p.version,
	p.man_unit
	)
SELECT
	(ST_Dump(geom)).geom as geom,
	fmlb,
	version,
	man_unit
FROM
	first_dissolve"

run_sql_r(query, conn_list)