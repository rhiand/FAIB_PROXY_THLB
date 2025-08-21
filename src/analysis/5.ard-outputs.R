library(dadmtools)
library(dplyr)
source('src/utils/functions.R')
conn_list <- dadmtools::get_pg_conn_list()

## export the required raster exports for the ARD:
source('src/utils/GenerateTIFFS.R')

## export the required vector exports for the ARD:
## riparian
output_dir <- "S:\\FOR\\VIC\\HTS\\FAIB_DATA_FOR_DISTRIBUTION\\THLB\\THLB_Proxy"
system(glue("ogr2ogr -overwrite -f \"FileGDB\" {output_dir}\\provincial_riparian_buffers.gdb PG:\"dbname='{conn_list$dbname}' host='{conn_list$host}' user='{conn_list$user}' password='{conn_list$password}'\"  -sql \"SELECT * FROM whse_vector.riparian_buffers\" -nlt MULTIPOLYGON -nln riparian_buffers"))

## pthlb
query <- "DROP TABLE IF EXISTS public.provincial_pthlb;"
run_sql_r(query, conn_list)

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

## export to FAIB_DATA_FOR_DISTRIBUTION
system(glue("ogr2ogr -overwrite -f \"FileGDB\" {output_dir}\\provincial_pthlb.gdb PG:\"dbname='{conn_list$dbname}' host='{conn_list$host}' user='{conn_list$user}' password='{conn_list$password}'\"  -sql \"SELECT * FROM public.provincial_pthlb\" -nlt MULTIPOLYGON -nln provincial_pthlb"))

## paflb
query <- "DROP TABLE IF EXISTS public.provincial_paflb;"
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
	paflb_fact,
	version,
	man_unit
FROM
	first_dissolve"
run_sql_r(query, conn_list)

## export to FAIB_DATA_FOR_DISTRIBUTION
system(glue("ogr2ogr -overwrite -f \"FileGDB\" {output_dir}\\provincial_paflb.gdb PG:\"dbname='{conn_list$dbname}' host='{conn_list$host}' user='{conn_list$user}' password='{conn_list$password}'\"  -sql \"SELECT * FROM public.provincial_paflb\" -nlt MULTIPOLYGON -nln provincial_paflb"))


## fmlb
query <- "DROP TABLE IF EXISTS public.provincial_fmlb;"
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
## export to FAIB_DATA_FOR_DISTRIBUTION
system(glue("ogr2ogr -overwrite -f \"FileGDB\" {output_dir}\\provincial_fmlb.gdb PG:\"dbname='{conn_list$dbname}' host='{conn_list$host}' user='{conn_list$user}' password='{conn_list$password}'\"  -sql \"SELECT * FROM public.provincial_fmlb\" -nlt MULTIPOLYGON -nln provincial_fmlb"))
