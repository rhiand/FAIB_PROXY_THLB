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

query <- "DROP TABLE IF EXISTS thlb_proxy.retention_data_explore"
run_sql_r(query, conn_list)


## BIO: "Biodiversity"
## BOT: 
## CHR: 
## CWD: 
##  FH: 
## FUE: 
## MSM: 
## OTH: "Other"
## REC: 
## RMA: "Riparian Management Area"
## SEN: "Sensitive Site"
## TER: "Terrain Stability"
## TIM: "Timber Objective"
## VIS: "Visual"
## WTR: "Wildlife Tree Retention Goals"



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
		opening.opening_gross_area - (st_area(opening.geom)/10000) as area_diff,
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
		CASE
      WHEN avg(opening_gross_area) = 0 THEN 0 ELSE
      sum(SILV_POLYGON_AREA)/avg(opening_gross_area)
    END AS prop_retention
	from
		a
	group by 
		man_unit,opening_id
)
select 
	man_unit,
	PERCENTILE_CONT(0.00) WITHIN GROUP (ORDER BY prop_retention) AS percentile_00,
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
	PERCENTILE_CONT(1.00) WITHIN GROUP (ORDER BY prop_retention) AS percentile_100
FROM
	b
group by
	man_unit;"
run_sql_r(query, conn_list)



query <- "select * from thlb_proxy.retention_data_explore"
data <- sql_to_df(query, conn_list)

library(tidyr)
library(ggplot2)

# Reshape the data from wide to long
data_long <- data %>%
  pivot_longer(cols = starts_with("percentile_"), 
               names_to = "percentile", 
               values_to = "value") %>%
  mutate(percentile = as.numeric(gsub("percentile_", "", percentile)))

# Subset for 0-10th and 90-100th percentiles
data_filtered <- data_long %>%
  filter((percentile >= 0 & percentile <= 10) | (percentile >= 90 & percentile <= 100))

# Create the line plot
ggplot(data_filtered, aes(x = percentile, y = value, color = man_unit, group = man_unit)) +
  geom_line() +
  labs(
    title = "Percentile Line Plot",
    x = "Percentile",
    y = "Value"
  ) +
  theme_minimal()