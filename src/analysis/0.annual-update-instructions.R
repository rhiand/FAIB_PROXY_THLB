library(dadmtools)

## handover document:
## https://bcgov-my.sharepoint.com/:w:/r/personal/hailey_eckstrand_gov_bc_ca/Documents/THLB%20Proxy%20Handover%20Instructions.docx?d=wb836983afbb14745a119ab3ef8db6df4&csf=1&web=1&e=GMS6Vr


## check out the git repo:
## https://github.com/bcgov/FAIB_PROXY_THLB
## update the repo_path path below to your local path
repo_path <- 'C:/projects/FAIB_PROXY_THLB'
setwd(repo_path)

## ensure the follow keyrings are setup and referring to Hailey's thlb_proxy db
library(keyring)
keyring_create("localpsql")
key_set("dbuser", keyring = "localpsql", prompt = 'Postgres keyring dbuser:')
key_set("dbpass", keyring = "localpsql", prompt = 'Postgres keyring password:')
## change host to Hailey's ip address
key_set("dbhost", keyring = "localpsql", prompt = 'Postgres keyring host:')
## change dbname to thlb_proxy
key_set("dbname", keyring = "localpsql", prompt = 'Postgres keyring dbname:')

## connect to oracle using your (Ie. Rhian's) login credentials
keyring_create("oracle")
key_set("dbuser", keyring = "oracle", prompt = 'Oracle keyring dbuser:')
key_set("dbpass", keyring = "oracle", prompt = 'Oracle keyring password:')
key_set("dbhost", keyring = "oracle", prompt = 'Oracle keyring host:')
key_set("dbservicename", keyring = "oracle", prompt = 'Oracle keyring serviceName:')
key_set("dbserver", keyring = "oracle", prompt = 'Oracle keyring server:')

conn_list <- dadmtools::get_pg_conn_list()

test_query <- "select man_unit from whse.thlb_proxy_netdown LIMIT 10"
df <- sql_to_df(test_query, conn_list)
## as per instructions in the Handover document (Step 5)
## Open up {repo_path}/config_parameters.csv in excel
## Go through the config_parameters.csv and update as per the handover document
## Once that is done, then run the import using the dadmtools.


batch_import_to_pg_gr_skey(
  in_csv            = glue('{repo_path}/data/input/config_parameters.csv'),
  pg_conn_param     = dadmtools::get_pg_conn_list(),
  ora_conn_param    = dadmtools::get_ora_conn_list(),
  crop_extent       = c(273287.5,1870587.5,367787.5,1735787.5),
  gr_skey_tbl       = 'whse.all_bc_gr_skey',
  raster_schema     = 'raster',
  template_tif      = "//spatialfiles2.bcgov/archive/FOR/VIC/HTS/ANA/workarea/PROVINCIAL/bc_01ha_gr_skey.tif",
  mask_tif          = "//spatialfiles2.bcgov/archive/FOR/VIC/HTS/ANA/workarea/PROVINCIAL/BC_Boundary_Terrestrial.tif",
  data_src_tbl      = 'whse.data_sources',
  out_tif_path      = glue('{repo_path}/data/output/'),
  import_rast_to_pg = FALSE
)


## after you've successfully imported all the needed data sources, run the following scripts:

## The following creates a value added attributes table (vaa) tables needed for later analysis called whse.man_unit_gr_skey
run_sql_psql(sql_var=NULL, sql_file = 'src/analysis/0.vaa_tables.sql', 'thlb_proxy')

## Update the FMLB to use the latest VRI & cutblocks
source('src/analysis/2.fmlb-creation.R')

## recreate the main THLB proxy netdown table, takes about 200 minutes when I run locally, ie. 3.5 hrs
## if f_own has changed table & attributes names, you'll need to modify this script to use the correct layer & attributes
## row numbers that reference f_own & attributes: 172, 243, 244, 269, 276, 277, 298, 299
source('src/analysis/3.netdown-create-table.R')

## open up 4.netdown.Rmd in RStudio and run with knit on - it will recreate the file: 4.netdown.html. Review this html for sanity
