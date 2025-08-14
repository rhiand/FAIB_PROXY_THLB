## Identify streams that have upstream catchments extending beyond the borders of 
## British Columbia (BC). The reason for this is that channel width is determined 
## by the size of the upstream watershed. Since the upstream watershed area is
## calculated based on the BC border, any stream with a catchment area outside 
## BC will yield inaccurate measurements.

library(dadmtools)
library(keyring)

## commented out as only need to run once
# keyring_create("localfwapg")
# key_set("dbuser", keyring = "localfwapg", prompt = 'Postgres keyring dbuser:')
# key_set("dbpass", keyring = "localfwapg", prompt = 'Postgres keyring password:')
# key_set("dbhost", keyring = "localfwapg", prompt = 'Postgres keyring host:')
# key_set("dbname", keyring = "localfwapg", prompt = 'Postgres keyring dbname:')


fwapg_conn <- get_pg_conn_list(host=key_get("dbhost", keyring = "localfwapg"),
                 user=key_get("dbuser", keyring = "localfwapg"),
                 dbname=key_get("dbname", keyring = "localfwapg"),
                 password=key_get("dbpass", keyring = "localfwapg")
                 )

query <- "DROP TABLE IF EXISTS whse_basemapping.fwa_upstreambordercrossings_output"
run_sql_r(query, fwapg_conn)
query <- "CREATE TABLE whse_basemapping.fwa_upstreambordercrossings_output (
blue_line_key integer NOT NULL,
fwa_upstreambordercrossings text)"
run_sql_r(query, fwapg_conn)

query <- "SELECT blue_line_key FROM whse_basemapping.fwa_stream_networks_sp GROUP BY blue_line_key"
bl_keys <- sql_to_df(query, fwapg_conn)

for (i in 1:nrow(bl_keys)){
  tryCatch({
    print(glue('On blue_line_key {i}/{nrow(bl_keys)}'))
    bl_key <- bl_keys[i,1]
    query <- glue("INSERT INTO whse_basemapping.fwa_upstreambordercrossings_output (blue_line_key, fwa_upstreambordercrossings) SELECT {bl_key}, fwa_upstreambordercrossings FROM whse_basemapping.fwa_upstreambordercrossings({bl_key}, 100)")
    run_sql_r(query, fwapg_conn)
  }, error = function(e) {
    # Print the error message and continue with the next iteration
    query <- glue("INSERT INTO whse_basemapping.fwa_upstreambordercrossings_output (blue_line_key, fwa_upstreambordercrossings) VALUES ({bl_key}, 'error')")
    run_sql_r(query, fwapg_conn)
  })
}

## Manual review
## There were 28 stream reaches that had a processing ERROR - they were manually reviewed it doesn't look like their drainage were outside BC
## The output table was imported into prov_data database in the public schema via pg_dump | psql

conn_list <- dadmtools::get_pg_conn_list()
## During manually review, it was found that Hay River was missed so it was inserted
query <- "INSERT INTO public.fwa_upstreambordercrossings_output VALUES (359571420, 'AB_120')"
run_sql_r(query, conn_list)

## upon manual review - the following stream reaches were removed as the majority of their contributing area was within BC
query <- "DROP TABLE IF EXISTS public.streams_outside_bc;"
run_sql_r(query, conn_list)

query <- "CREATE TABLE public.streams_outside_bc AS
SELECT
fishy.linear_feature_id,
fishy.blue_line_key,
fishy.channel_width,
fishy.riparian_class,
fishy.gnis_name,
fishy.fwa_watershed_code,
fwa.local_watershed_code,
fishy.geom,
bad_data.fwa_upstreambordercrossings
FROM
whse_sp.modelled_habitat_potential fishy
JOIN public.fwa_upstreambordercrossings_output bad_data ON bad_data.blue_line_key = fishy.blue_line_key
WHERE
bad_data.fwa_upstreambordercrossings in ('AB_120', 'USA_49', 'YTNWT_60')
AND
fishy.gnis_name not in ('Elk River', 'Gold Creek', 'Bloom Creek', 'Caven Creek', 'Fort Nelson River', 'Linklater Creek', 'Etthithun River', 'Salmo River', 'Ring Reid Creek', 'Cautley Creek', 'Boundary Creek', 'Fraser River', 'Saxon Creek', 'Belcourt Creek', 'Huguenot Creek', 'Beatton River', 'Kahntah River');"
run_sql_r(query, conn_list)

query <- "DELETE FROM public.streams_outside_bc WHERE gnis_name = 'Kootenay River' AND local_watershed_code >= '300-625474-554084-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000';"

query <- "CREATE TABLE public.stream_reaches_with_contributing_areas_outside_bc AS SELECT linear_feature_id FROM public.streams_outside_bc"
run_sql_r(query, conn_list)

## pg_dump -d prov_data -U postgres -h 142.36.123.95 -t public.stream_reaches_with_contributing_areas_outside_bc > stream_reaches_with_contributing_areas_outside_bc.sql
## the above was edited to include: DROP TABLE IF EXISTS public.stream_reaches_with_contributing_areas_outside_bc; at the top of the file

## post processing bcts riparian class field
query <- "ALTER TABLE whse_sp.bcts_field_streams ADD COLUMN IF NOT EXISTS class_clean text;"
run_sql_r(query, conn_list)

## TODO finish up string tidying
query <- "select 
	fid,
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(btrim(class), '(', ''), ')', ''), '-B', ''), '-A', ''), 'A', ''), '-S', '')
from 
	whse_sp.bcts_field_streams"



