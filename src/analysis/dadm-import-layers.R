library(devtools)
library(dadmtools)
library(RPostgres)
library(glue)
library(terra)
library(keyring)
library(sf)

import_gr_skey_tif_to_pg_rast(
  template_tif      = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
  mask_tif          = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
  crop_extent       = c(273287.5,1870587.5,367787.5,1735787.5), ## c(xmin,xmax,ymin,ymax)
  out_crop_tif_name = 'data\\raw\\bc_01ha_gr_skey.tif',
  pg_conn_param     = dadmtools::get_pg_conn_list(),
  dst_tbl           = 'whse.all_bc_gr_skey'
)


batch_import_to_pg_gr_skey(
  in_csv            = 'data\\input\\config_parameters.csv',
  pg_conn_param     = dadmtools::get_pg_conn_list(),
  ora_conn_param    = dadmtools::get_ora_conn_list(),
  crop_extent       = c(273287.5,1870587.5,367787.5,1735787.5), ## c(xmin,xmax,ymin,ymax)
  gr_skey_tbl       = 'whse.all_bc_gr_skey',
  raster_schema     = 'raster',
  template_tif      = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
  mask_tif          = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
  data_src_tbl      = 'whse.data_sources',
  out_tif_path      = 'data\\output\\',
  import_rast_to_pg = FALSE
)
