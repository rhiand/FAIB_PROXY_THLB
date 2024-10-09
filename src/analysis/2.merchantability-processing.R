library(dadmtools)
library(janitor)
library(readr)
library(stringr)
library(tidyr)
source('src/utils/functions.R')

conn_list <- dadmtools::get_pg_conn_list()
db <- DBI::dbConnect(conn_list["driver"][[1]],
				host = conn_list["host"][[1]],
				user = conn_list["user"][[1]],
				dbname = conn_list["dbname"][[1]],
				password = conn_list["password"][[1]],
				port = conn_list["port"][[1]])
start_time <- Sys.time()
print(glue("Script started at {format(start_time, '%Y-%m-%d %I:%M:%S %p')}"))

# query <- "DROP TABLE IF EXISTS thlb_proxy.inoperable_gr_skey"
# run_sql_r(query, conn_list)
# query <- "DROP TABLE IF EXISTS thlb_proxy.inoperable_thresholds"
# run_sql_r(query, conn_list)

ecas_interior_path_2016 <- "data\\input\\InteriorStoneQuery2016.csv"
ecas_interior_path_2023 <- "data\\input\\InteriorStoneQuery2023.csv"
ecas_coast_path_2016 <- "data\\input\\CoastStoneQuery2016.csv"
ecas_coast_path_2023 <- "data\\input\\CoastStoneQuery2023.csv"

mhv <- get_ecas(ecas_interior_path_2023, ecas_interior_path_2016)
mhv01 <- mhv[[1]]
ecas_df <- as.data.frame(mhv[['tcas_df']])
## stalled out there as dont have vdyp table.
mhv_df<-get_mhv_df(db,vdyp_query,spc1_query,mhv01)





## as TSA 04 is too large, a new set of boundaries were created
query <- "DROP TABLE IF EXISTS thlb_proxy.tsa_boundaries_2020_inoperable"
run_sql_r(query, conn_list)
query <- "CREATE TABLE thlb_proxy.tsa_boundaries_2020_inoperable AS 
SELECT tsa_number, geom FROM thlb_proxy.tsa_boundaries_2020 WHERE tsa_number != '04'
UNION ALL
(WITH pts AS (
	SELECT
		(ST_Dump(ST_GeneratePoints(geom, 2000))).geom AS geom
	from
		thlb_proxy.tsa_boundaries_2020
	where 
		tsa_number = '04'
), pts_clustered AS (
	select
		geom, ST_ClusterKMeans(geom, 3) over () AS cluster
	from
		pts
), centers AS (
  SELECT
	cluster, ST_Centroid(ST_collect(geom)) AS geom
  FROM 
		pts_clustered
	GROUP BY 
		cluster
), veronoi_polys AS (
SELECT
	(ST_Dump(ST_VoronoiPolygons(ST_collect(geom)))).geom AS geom
FROM
	centers
)
SELECT
	'04-' || row_number() OVER() as tsa_number,
	ST_Intersection(a.geom, b.geom) AS geom
FROM
	veronoi_polys b
CROSS JOIN
	(SELECT geom FROM thlb_proxy.tsa_boundaries_2020 WHERE tsa_number = '04') a
)"
run_sql_r(query, conn_list)

## Loop over the TSA numbers
query <- "SELECT tsa_number FROM thlb_proxy.tsa_boundaries_2020_inoperable WHERE tsa_number ilike '04%' GROUP BY tsa_number"
tsa_numbers <- sql_to_df(query, conn_list)$tsa_number

for (tsa_number in tsa_numbers) {
	start_time <- Sys.time()
	print(glue("TSA #: {tsa_number}, started at: {format(start_time, '%Y-%m-%d %I:%M:%S %p')}"))
	## define data driven queries for cutblocks, tsa's, stability

	mgmt_unit_query <- glue("SELECT 
							ST_Union(geom) as geom
						FROM 
							thlb_proxy.tsa_boundaries_2020_inoperable 
						WHERE 
							tsa_number = '{tsa_number}'")
	
	message('Convert queries to SpatVector')
	mgmt_unit_vect <- create_sampler(db, mgmt_unit_query)

	# writeRaster(stability_clipped, "data\\analysis\\stability_clipped.tif", overwrite=TRUE)
	message('Aggregating slope, stability and elev')
	df_to_pg(Id(schema = 'thlb_proxy', table = glue('inoperable_gr_skey')), phy_ops_df, conn_list, overwrite=FALSE, append=TRUE)
	end_time <- Sys.time()
	duration <- difftime(end_time, start_time, units = "mins")
	print(glue("TSA #: {tsa_number}, took: {duration} minutes\n"))
}



