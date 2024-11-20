library(dadmtools)
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
dst_schema <- "thlb_proxy"


query <- "DROP TABLE IF EXISTS thlb_proxy.bc_inoperable_gr_skey;"
run_sql_r(query, conn_list)

query <- "CREATE TABLE thlb_proxy.bc_inoperable_gr_skey AS
SELECT 
	distinct on (inop.gr_skey)
	inop.gr_skey,
	split_part(inop.man_unit, '-sub-', 1) as man_unit,
	CASE 
		WHEN inop.class2 > 250 THEN inop.class2::numeric/1000
		ELSE 0
	END AS inop_fact,
	inop.class2,
	inop_cc.number_of_cutblocks,
	elev_threshold.elev_99th,
	slope_threshold.slope_99th
	
FROM 
thlb_proxy.inoperable_gr_skey inop 
JOIN thlb_proxy.inoperable_cutblock_summary inop_cc on split_part(inop.man_unit, '-sub-', 1) = inop_cc.mgmt_unit
JOIN
	(
		SELECT 
			DISTINCT ON (cutblock_percentile_99, split_part(inop.man_unit, '-sub-', 1))			 
			cutblock_percentile_99 as elev_99th,
			split_part(inop.man_unit, '-sub-', 1) as man_unit
		FROM 
			thlb_proxy.inoperable_thresholds inop
		WHERE grid = 'elevation'
		ORDER BY
			cutblock_percentile_99,
			split_part(inop.man_unit, '-sub-', 1)
	) elev_threshold
ON split_part(inop.man_unit, '-sub-', 1) = elev_threshold.man_unit
JOIN
	(
		SELECT 
			DISTINCT ON (cutblock_percentile_99, split_part(inop.man_unit, '-sub-', 1))			 
			cutblock_percentile_99 as slope_99th,
			split_part(inop.man_unit, '-sub-', 1) as man_unit
		FROM 
		thlb_proxy.inoperable_thresholds inop
		WHERE grid = 'slope'
		ORDER BY
			cutblock_percentile_99,
			split_part(inop.man_unit, '-sub-', 1)
	) slope_threshold
ON split_part(inop.man_unit, '-sub-', 1) = slope_threshold.man_unit
WHERE 
	inop_cc.number_of_cutblocks >= 30
AND
	inop.man_unit ilike 'TFL%'
ORDER BY inop.gr_skey, class2 DESC"
run_sql_r(query, conn_list)

	
query <- "INSERT INTO thlb_proxy.bc_inoperable_gr_skey
SELECT 
	distinct on (inop.gr_skey)
	inop.gr_skey,
	split_part(inop.man_unit, '-sub-', 1) as man_unit,
	CASE 
		WHEN inop.class2 > 250 THEN inop.class2::numeric/1000
		ELSE 0
	END AS inop_fact,
	inop.class2,
	inop_cc.number_of_cutblocks,
	elev_threshold.elev_99th,
	slope_threshold.slope_99th
FROM 
thlb_proxy.inoperable_gr_skey inop 
JOIN thlb_proxy.inoperable_cutblock_summary inop_cc on split_part(inop.man_unit, '-sub-', 1) = inop_cc.mgmt_unit
JOIN
	(
		SELECT 
			DISTINCT ON (cutblock_percentile_99, split_part(inop.man_unit, '-sub-', 1))			 
			cutblock_percentile_99 as elev_99th,
			split_part(inop.man_unit, '-sub-', 1) as man_unit
		FROM 
			thlb_proxy.inoperable_thresholds inop
		WHERE grid = 'elevation'
		ORDER BY
			cutblock_percentile_99,
			split_part(inop.man_unit, '-sub-', 1)
	) elev_threshold
ON split_part(inop.man_unit, '-sub-', 1) = elev_threshold.man_unit
JOIN
	(
		SELECT 
			DISTINCT ON (cutblock_percentile_99, split_part(inop.man_unit, '-sub-', 1))			 
			cutblock_percentile_99 as slope_99th,
			split_part(inop.man_unit, '-sub-', 1) as man_unit
		FROM 
		thlb_proxy.inoperable_thresholds inop
		WHERE grid = 'slope'
		ORDER BY
			cutblock_percentile_99,
			split_part(inop.man_unit, '-sub-', 1)
	) slope_threshold
ON split_part(inop.man_unit, '-sub-', 1) = slope_threshold.man_unit
LEFT JOIN thlb_proxy.bc_inoperable_gr_skey inop_exist ON inop_exist.gr_skey = inop.gr_skey
WHERE 
	inop_cc.number_of_cutblocks >= 30
AND 
	-- don't retrieve any gr_skey that already exists in inop table from managed licences & TFLs
	inop_exist.gr_skey IS NULL
ORDER BY inop.gr_skey, class2 DESC"
run_sql_r(query, conn_list)

end_time <- Sys.time()
duration <- round(difftime(end_time, start_time, units = "mins"), 2)
print(glue('Script finished. Duration: {duration} minutes.'))


