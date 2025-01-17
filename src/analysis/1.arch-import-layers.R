library(RPostgres)
library(glue)
library(devtools)
# install_github("bcgov/FAIB_DADMTOOLS")
library(dadmtools)
source('src/utils/functions.R')
## relies on install_github("bcgov/FAIB_DATA_MANAGEMENT") being installed at some point
conn_list <- dadmtools::get_pg_conn_list()

## import WHSE_ARCHAEOLOGY.RAAD_TFM_SITES_SVW using the dadmtools
## ran with src\analysis\dadm-import-layers.R