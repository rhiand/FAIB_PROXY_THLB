library(dadmtools)
library(bcdata)
library(tidyverse)
source('src/utils/functions.R')
conn_list <- dadmtools::get_pg_conn_list()
db <- DBI::dbConnect(conn_list["driver"][[1]],
				host = conn_list["host"][[1]],
				user = conn_list["user"][[1]],
				dbname = conn_list["dbname"][[1]],
				password = conn_list["password"][[1]],
				port = conn_list["port"][[1]])


## WHSE_FOREST_VEGETATION.RSLT_OPENING_SVW
import_bcgw_to_pg(src_schema     = "WHSE_FOREST_VEGETATION",
                  src_layer      = "RSLT_OPENING_SVW",
                  fdw_schema     = "load",
                  dst_schema     = "thlb_proxy",
                  dst_layer      = "RSLT_OPENING_SVW",
                  fields_to_keep = "OPENING_ID, DISTURBANCE_START_DATE, DISTURBANCE_END_DATE, OPENING_GROSS_AREA, CUT_BLOCK_ID, TIMBER_MARK",
                  geometry_name  = "geometry",
                  geometry_type  = "MultiPolygon",
                  grouping_name  = NULL,
                  pg_conn_list   = conn_list)

query <- "ALTER TABLE thlb_proxy.rslt_opening_svw ADD PRIMARY KEY (opening_id)"
run_sql_r(query, conn_list)

## WHSE_FOREST_VEGETATION.RSLT_FOREST_COVER_RESERVE_SVW
import_bcgw_to_pg(src_schema     = "WHSE_FOREST_VEGETATION",
                  src_layer      = "RSLT_FOREST_COVER_RESERVE_SVW",
                  fdw_schema     = "load",
                  dst_schema     = "thlb_proxy",
                  dst_layer      = "RSLT_FOREST_COVER_RESERVE_SVW",
                  fields_to_keep = "OPENING_ID, CUT_BLOCK_ID, SILV_RESERVE_CODE, SILV_RESERVE_OBJECTIVE_CODE, SILV_POLYGON_AREA",
                  geometry_name  = NULL,
                  geometry_type  = "MultiPolygon",
                  grouping_name  = NULL,
                  pg_conn_list   = conn_list)


query <- "CREATE TABLE thlb_proxy.retention_data_explore AS
with a as (
	SELECT 
		man_unit,
		opening.opening_id, 
		opening.disturbance_start_date, 
		opening.disturbance_end_date, 
		opening.opening_gross_area, 
		opening.cut_block_id, 
		opening.timber_mark, 
		opening.opening_gross_area - (st_area(opening.geom)/10000),
		for_cov.silv_reserve_code,
		for_cov.SILV_RESERVE_OBJECTIVE_CODE,
		for_cov.SILV_POLYGON_AREA,
		for_cov.SILV_POLYGON_AREA/opening.opening_gross_area as prop_retention
	FROM
	thlb_proxy.rslt_opening_svw opening 
	LEFT JOIN thlb_proxy.tsa_boundaries_2020 tsa on ST_Intersects(tsa.geom, opening.geom)
	LEFT JOIN thlb_proxy.RSLT_FOREST_COVER_RESERVE_SVW for_cov USING (opening_id)
	WHERE
		DISTURBANCE_START_DATE > '2011-01-01' 
	AND 
		DISTURBANCE_START_DATE < '2024-01-01'
	AND
		timber_mark is not null
	AND
		for_cov.SILV_RESERVE_OBJECTIVE_CODE not in ('TIM', 'RMA')
	AND 
		for_cov.SILV_RESERVE_CODE = 'G'
	), b as (
	select
		man_unit,
		opening_id,
		sum(SILV_POLYGON_AREA) as retention_area,
		avg(opening_gross_area),
		sum(SILV_POLYGON_AREA)/avg(opening_gross_area) as prop_retention
	from
		a
	group by 
		man_unit,opening_id
)
select 
	man_unit,
	PERCENTILE_CONT(0.00) WITHIN GROUP (ORDER BY prop_retention) AS percentile_01,
	PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY prop_retention) AS percentile_01,
	PERCENTILE_CONT(0.02) WITHIN GROUP (ORDER BY prop_retention) AS percentile_02,
	PERCENTILE_CONT(0.03) WITHIN GROUP (ORDER BY prop_retention) AS percentile_03,
	PERCENTILE_CONT(0.04) WITHIN GROUP (ORDER BY prop_retention) AS percentile_04,
	PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY prop_retention) AS percentile_05,
	PERCENTILE_CONT(0.06) WITHIN GROUP (ORDER BY prop_retention) AS percentile_06,
	PERCENTILE_CONT(0.07) WITHIN GROUP (ORDER BY prop_retention) AS percentile_07,
	PERCENTILE_CONT(0.08) WITHIN GROUP (ORDER BY prop_retention) AS percentile_08,
	PERCENTILE_CONT(0.09) WITHIN GROUP (ORDER BY prop_retention) AS percentile_09,
	PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY prop_retention) AS percentile_10,
	PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY prop_retention) AS percentile_90,
	PERCENTILE_CONT(0.91) WITHIN GROUP (ORDER BY prop_retention) AS percentile_91,
	PERCENTILE_CONT(0.92) WITHIN GROUP (ORDER BY prop_retention) AS percentile_92,
	PERCENTILE_CONT(0.93) WITHIN GROUP (ORDER BY prop_retention) AS percentile_93,
	PERCENTILE_CONT(0.94) WITHIN GROUP (ORDER BY prop_retention) AS percentile_94,
	PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY prop_retention) AS percentile_95,
	PERCENTILE_CONT(0.96) WITHIN GROUP (ORDER BY prop_retention) AS percentile_96,
	PERCENTILE_CONT(0.97) WITHIN GROUP (ORDER BY prop_retention) AS percentile_97,
	PERCENTILE_CONT(0.98) WITHIN GROUP (ORDER BY prop_retention) AS percentile_98,
	PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY prop_retention) AS percentile_99,
	PERCENTILE_CONT(1.0) WITHIN GROUP (ORDER BY prop_retention) AS percentile_100
FROM
	b
group by
	man_unit;"
run_sql_r(query, conn_list)


mgmt_unit_query <- glue("SELECT 
						geom as geom
					FROM 
						thlb_proxy.tsa_boundaries_2020
					WHERE 
						tsa_number = '02'")
bnd <- st_read(db, query = mgmt_unit_query) 
# Don't need to evaluate unless a new query is needed. jump to next section and load data from postgres.

# load results data directly from BCGW

# Opening_ID table - includes disturbance dates, gross block area, etc.
# OPENING_ID table from RESULTS. 
RES_opening <- bcdc_query_geodata("WHSE_FOREST_VEGETATION.RSLT_OPENING_SVW") %>% 
  filter(INTERSECTS(bnd)) %>% 
  collect()



query <- "SELECT 
  opening_id, 
  disturbance_start_date, 
  disturbance_end_date, 
  opening_gross_area, cut_block_id, timber_mark, st_area(geom)/10000
FROM
thlb_proxy.rslt_opening_svw"
# reduce table to variables of interest:
RES_opening_selection <- RES_opening %>% 
  # select variables of interest:
  select(OPENING_ID, DISTURBANCE_START_DATE, DISTURBANCE_END_DATE, OPENING_GROSS_AREA, CUT_BLOCK_ID, TIMBER_MARK) %>% 
  # calculate area and compare to gross area
  mutate(area_calc = as.numeric(st_area(geometry))/10000,
         area_diff = OPENING_GROSS_AREA - area_calc) %>% 
  # filter to disturbance after Jan 1, 2011:
  filter(DISTURBANCE_START_DATE > "2011-01-01" & DISTURBANCE_START_DATE < "2024-01-01")

# reserve polygons with Opening_ID
RES_reserve <- bcdc_query_geodata("WHSE_FOREST_VEGETATION.RSLT_FOREST_COVER_RESERVE_SVW") %>% 
  filter(INTERSECTS(bnd)) %>% 
  collect() 

# with the understanding that each opening id corresponds to a block, join opening information to reserve information:
# DO NOT add gross block area - it is repeated for each record of reserve within an opening id
RES_reserve_opening <- RES_reserve %>% 
  left_join(RES_opening_selection %>% 
              st_drop_geometry() %>% 
              as_tibble() %>% 
              select(-TIMBER_MARK, -CUT_BLOCK_ID, -area_calc, -area_diff), 
            by = "OPENING_ID") %>% 
  # exclude opening_ids not in the OPENING info table (it has been filtered to disturbance date after January 2011). None of the 'NA" timber marks were brought in.
  filter(!is.na(DISTURBANCE_START_DATE)) %>% 
  # only keep grouped retention
  filter(SILV_RESERVE_CODE %in% "G") %>% 
  # TIM objective is for short-term; exclude from sample:
  filter(!(SILV_RESERVE_OBJECTIVE_CODE %in% "TIM")) %>% 
  mutate(prop_retention = SILV_POLYGON_AREA/OPENING_GROSS_AREA) 
  

# SAVED BASED ON QUERY ON DECEMBER 3, 2024:
#if (dbExistsTable(db, "tsa02_results2024_att")) {
#    dbRemoveTable(db, "tsa02_results2024_att")
#  }
dbWriteTable(db, "tsa02_results2024_att", RES_reserve_opening, row.names= FALSE, overwrite = TRUE)


# query data from postgres:
RES_reserve_opening <- st_read(db, query = "select * from tsa02_results2024_att;")

# chart the proportion of reserve area by reserve objective code 
RES_reserve_opening %>% 
  st_drop_geometry() %>% 
# create plot:
 ggplot() +
  aes(x = SILV_RESERVE_OBJECTIVE_CODE, y = prop_retention) +
  geom_jitter(width = 0.39, alpha = 0.2, color = "darkseagreen4") +
  geom_boxplot(fill = NA, color = "grey20", outlier.shape = NA) +
  theme_light() +
  labs(title = "RESULTS Stand-Level Retention", 
       subtitle = "Proportion of retained area  within gross cutblock area grouped by reserve objective",
       y = "Proportion",
       x = "Silviculture Objective Code")

# summarize in tabular form:
RES_reserve_opening %>% 
  st_drop_geometry() %>% 
  as_tibble() %>% 
  group_by(SILV_RESERVE_OBJECTIVE_CODE) %>% 
  #ungroup() %>% 
  summarize('Retention Area' = sum(SILV_POLYGON_AREA),
            'No. of retention polygons' = n(),
            'Mean proportion retained' = round(mean(prop_retention),3)) %>% 
  mutate(Description = case_when(
              SILV_RESERVE_OBJECTIVE_CODE == "WTR" ~ "Wildlife Tree Retention Goals",
              SILV_RESERVE_OBJECTIVE_CODE == "BIO" ~ "Biodiversity",
              SILV_RESERVE_OBJECTIVE_CODE == "OTH" ~ "Other",
              SILV_RESERVE_OBJECTIVE_CODE == "SEN" ~ "Sensitive Site",
              SILV_RESERVE_OBJECTIVE_CODE == "TER" ~ "Terrain Stability",
              SILV_RESERVE_OBJECTIVE_CODE == "RMA" ~ "Riparian Management Area",
              SILV_RESERVE_OBJECTIVE_CODE == "VIS" ~ "Visual"
            )) %>% 
  rename('Reserve Objective Code' = SILV_RESERVE_OBJECTIVE_CODE) %>% 
  pretty_table()


rslts_opening_retention_prop <- RES_reserve_opening %>% 
  st_drop_geometry() %>% 
  as_tibble() %>% 
  group_by(OPENING_ID, OPENING_GROSS_AREA, DISTURBANCE_START_DATE) %>%
  summarise(retention_area = sum(SILV_POLYGON_AREA)) %>% 
  mutate(proportion_retained = retention_area/OPENING_GROSS_AREA)

rslts_weightedmean <- rslts_opening_retention_prop %>% 
  ungroup() %>% 
  summarise(weighted_mean = weighted.mean(proportion_retained, OPENING_GROSS_AREA)) %>% 
  pull() #december 3rd: 0.0954


# look at proportion retained since 2011 in RESULTS and FREP DATA:
results <- data.frame(distribution = "RESULTS", proportion = rslts_opening_retention_prop$proportion_retained)
frep_dist <- data.frame(distribution = "FREP", proportion = frep$PCT_TOTAL_RETENTION/100)
retention <- as.data.frame(rbind(results, frep_dist))

ggplot(retention) +
  aes(x = distribution, y = proportion) +
  geom_jitter(width = 0.39, alpha = 0.2, shape = "circle", size = 1.5, colour = "darkseagreen4", alpha = 0.6) +
  geom_boxplot(outlier.shape = NA,fill = NA) +
  stat_summary(fun.y="mean",color="deepskyblue4")+
  theme_light()+
  labs(title = "Stand-Level Retention", 
       subtitle = "Proportion of retained area within gross cutblock area",
       y = "Proportion",
       x = " ")
	   
rslts_mean_pct_retained <- data.frame(data_source = "RESULTS",
                                      mean_prop_retained = rslts_opening_retention_prop %>% 
  ungroup() %>% 
  summarise(weighted_mean = weighted.mean(proportion_retained, OPENING_GROSS_AREA)) %>% pull())

frep_mean_pct_retained <- data.frame(data_source = "FREP",
                                     mean_prop_retained = frep %>% summarize(mean = weighted.mean(PCT_TOTAL_RETENTION,GROSS_AREA)/100) %>% pull())

pretty_table(rbind(rslts_mean_pct_retained, frep_mean_pct_retained) %>%
               mutate(mean_prop_retained = round(mean_prop_retained, 4)) %>% 
               rename('Data Source' = data_source,
                      'Mean proportion retained' = mean_prop_retained),
             caption = "Proportion retained based on FREP and RESULTS data") %>% 
  kable_styling(full_width = FALSE)

