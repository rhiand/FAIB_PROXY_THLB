library(dadmtools)
source('src/utils/functions.R')
conn_list <- dadmtools::get_pg_conn_list()
## Note: the function: import_bcgw_to_pg relies on he oracle foreign server: oradb existing. If working on a fresh db that doesn't have the oracle foreign server set up yet - be sure to import layers using dadmtools first as it will set up the oracle foreign server

repo_path <- 'C:/projects/FAIB_PROXY_THLB'
setwd(repo_path)

dst_schema <- "whse"

## import the current and historical lidar data within the VRI gaps
src_path <- 'data/input/final_gaps_current_noncurrent_lidar_treedPercent.sqlite'
src_lyr <- 'gaps_current_noncurrent_lidar_treedpercent'
ogr_cmd <- glue('ogr2ogr -overwrite -a_srs EPSG:3005 -nln {src_lyr} -lco SCHEMA={dst_schema} -nlt NONE -lco OVERWRITE=YES --config PG_USE_COPY YES -f PostgreSQL PG:dbname={conn_list$dbname} {src_path}')
system(ogr_cmd)

src_path <- 'data/input/final_gaps_lidarProgam_contained_treedpercent.sqlite'
src_lyr <- 'gaps_lidarprogam_contained_treedpercent'
ogr_cmd <- glue('ogr2ogr -overwrite -a_srs EPSG:3005 -nln {src_lyr} -lco SCHEMA={dst_schema} -nlt NONE -lco OVERWRITE=YES --config PG_USE_COPY YES -f PostgreSQL PG:dbname={conn_list$dbname} {src_path}')
system(ogr_cmd)

## recalculate the NA percent as it is currently a combo or proportion & percent
query <- glue("update {dst_schema}.gaps_current_noncurrent_lidar_treedpercent set napercent = round(((totaln-(treedn+ntreedn))/totaln*100)::numeric, 2);")
run_sql_r(query, conn_list)

query <- glue("update {dst_schema}.gaps_current_noncurrent_lidar_treedpercent set treedpercent = round((treedn/(treedn+ntreedn)*100)::numeric, 2);")
run_sql_r(query, conn_list)
