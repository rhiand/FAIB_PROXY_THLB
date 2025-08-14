library(dadmtools)
source('src/utils/functions.R')

conn_list <- dadmtools::get_pg_conn_list()
db <- DBI::dbConnect(conn_list["driver"][[1]],
				host = conn_list["host"][[1]],
				user = conn_list["user"][[1]],
				dbname = conn_list["dbname"][[1]],
				password = conn_list["password"][[1]],
				port = conn_list["port"][[1]])

dst_schema <- "whse"
vector_schema <- "whse_vector"

## WHSE_FOREST_VEGETATION.RSLT_OPENING_SVW
import_bcgw_to_pg(src_schema     = "WHSE_FOREST_VEGETATION",
                  src_layer      = "RSLT_OPENING_SVW",
                  fdw_schema     = "load",
                  dst_schema     = vector_schema,
                  dst_layer      = "RSLT_OPENING_SVW",
                  fields_to_keep = "OPENING_ID, DISTURBANCE_START_DATE, DISTURBANCE_END_DATE, OPENING_GROSS_AREA, CUT_BLOCK_ID, TIMBER_MARK, opening_category_code",
                  geometry_name  = "geometry",
                  geometry_type  = "MultiPolygon",
                  grouping_name  = NULL,
                  pg_conn_list   = conn_list)

query <- glue("ALTER TABLE {vector_schema}.rslt_opening_svw ADD COLUMN centroid geometry(Point, 3005);")
run_sql_r(query, conn_list)

query <- glue("UPDATE {vector_schema}.rslt_opening_svw set centroid = ST_Centroid(geom);")
run_sql_r(query, conn_list)

query <- glue("ALTER TABLE {vector_schema}.rslt_opening_svw ADD PRIMARY KEY (opening_id);")
run_sql_r(query, conn_list)               

query <- glue("CREATE INDEX rslt_opening_svw_centroid_geom_idx ON {vector_schema}.rslt_opening_svw USING gist(geom);")
run_sql_r(query, conn_list)

query <- glue("ANALYZE {vector_schema}.rslt_opening_svw;")
run_sql_r(query, conn_list)

## WHSE_FOREST_VEGETATION.RSLT_FOREST_COVER_INV_SVW
import_bcgw_to_pg(src_schema     = "WHSE_FOREST_VEGETATION",
                  src_layer      = "RSLT_FOREST_COVER_INV_SVW",
                  fdw_schema     = "load",
                  dst_schema     = vector_schema,
                  dst_layer      = "RSLT_FOREST_COVER_INV_SVW",
                  fields_to_keep = "OPENING_ID, SILV_RESERVE_CODE, SILV_RESERVE_OBJECTIVE_CODE, SILV_POLYGON_AREA, FOREST_COVER_WHEN_UPDATED",
                  geometry_name  = "geometry",
                  geometry_type  = "MultiPolygon",
                  grouping_name  = NULL,
                  pg_conn_list   = conn_list)


## https://www.for.gov.bc.ca/his/results/webhelp/index.htm
## Code Table -> Reserve Objectives
## 
## BIO: "Biodiversity"
## BOT: "Botanical Forest Products"
## CHR: "Cultural Heritage Resource"
## CWD: "Coarse Woody Debris"
##  FH: "Forest health"
## FUE: "Fuel Management"
## MSM: "MSMA Treated Area"
## OTH: "Other"
## REC: "Recreation Access"
## RMA: "Riparian Management Area"
## SEN: "Sensitive Site"
## TER: "Terrain Stability"
## TIM: "Timber Objective"
## VIS: "Visual"
## WTR: "Wildlife Tree Retention Goals"


## Based on guidance from Dan Turner, the Results Forest Cover Inventory layer (i.e., WHSE_FOREST_VEGETATION.RSLT_FOREST_COVER_INV_SVW) was imported instead of the Results Forest Cover Reserve layer (i.e., WHSE_FOREST_VEGETATION.RSLT_FOREST_COVER_RESERVE_SVW), in addition to importing the Results Opening layer (WHSE_FOREST_VEGETATION.RSLT_OPENING_SVW).

## This ensures that when calculating the opening area, the sum of the silv_polygon_area is used, rather than the opening_gross_area from the WHSE_FOREST_VEGETATION.RSLT_OPENING_SVW layer. Dan noted that opening_gross_area is entered manually and can become outdated, as it is completed only at the start of harvest and does not account for unplanned changes during harvesting. In contrast, the WHSE_FOREST_VEGETATION.RSLT_FOREST_COVER_INV_SVW layer is updated at the end of a disturbance to reflect the actual outcome.

## In addition, the silv_reserve_code = 'G' was introduced in 2011 - but only started to be enforced in 2012 - so upped the disturbance_start_date filter from 2011 to 2012

query <- glue("DROP TABLE IF EXISTS {dst_schema}.retention_data_explore")
run_sql_r(query, conn_list)

## calculate the sum of opening_area
query <- glue("CREATE TABLE {dst_schema}.retention_data_explore AS
	SELECT 
		mu_look.man_unit,
		opening.opening_id, 
		sum(res.silv_polygon_area) over (partition by res.opening_id) as opening_area,
		res.silv_reserve_code,
		res.silv_reserve_objective_code,
		res.silv_polygon_area,
		opening.opening_category_code,
		opening.DISTURBANCE_START_DATE,
		res.forest_cover_when_updated
	FROM
	{vector_schema}.rslt_opening_svw opening 
	LEFT JOIN {vector_schema}.tsa_boundaries_2020 tsa on ST_Intersects(tsa.geom, opening.centroid)
	LEFT JOIN {vector_schema}.rslt_forest_cover_inv_svw res USING (opening_id)
	LEFT JOIN {dst_schema}.mu_lookup_table_im mu_look on tsa.tsa_number::integer = mu_look.tsa_number::integer
	WHERE
		opening.DISTURBANCE_START_DATE > '2012-01-01'
	AND
		opening.timber_mark is not null
	AND 
		res.forest_cover_when_updated > '2012-01-01'")
run_sql_r(query, conn_list)