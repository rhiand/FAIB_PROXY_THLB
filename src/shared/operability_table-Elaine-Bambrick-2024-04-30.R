## ----setup, message=FALSE,warning=FALSE---------------------------------------------------------------------------------
#librarys
library(DBI)
library(RPostgreSQL)
library(tidyverse)
library(sf)
library(tidyterra)
library(terra)
library(R.utils)
library(bcdata)
library(janitor)
library(scales)
library(tidytext)
library(landscapemetrics)
library(landscapetools)
library(gstat)

rm(list = ls())

########################Operability Table components###########################################
#Physical Operability = slope/elevation/terrain stability rasters sampled by blocks to determine thesholds then aggregated
#Merchantability = maxvol from yield curves tested against ECAS, Problem Timber Types
#Economic Distance and isolation: isolated areas with no historic practice, minimum fragment size relative to distance 
#to nearest neighbor,road and milling complex, modeled cycletime from ECAS


#This script requires:
#1.   A Postgres binary stability table for the entire bounding box
#2    A Postgres ar table
#3    Paths to ECAS tables where all timbermarks have a "_" prefix
#4    A Postgres VDYP thin table
#5    A boundary feature class for the unit (ar.gdb)
#6.   A pre_thlb with a binary field
#7    A sink raster of mill locations
#8    A road network rater for the unit

############house keeping#################################################################

start<-Sys.time()
source("log_files.R")

#run house_keeping function to create output folders in your working directory, download skey files
# if folders already exist, they will not be re-created.
house_keeping()

# identify the unit to get extents and management unit number for ECAS
tsa_lbl <- "tsa02"
tsa_num <- 02
postgres_table <- "tsa02_operability"

######required paths

stone2023 <- "C:\\Data\\ECAS\\2023\\InteriorStoneQuery.csv" 

stone2016 <- "C:\\Data\\ECAS\\Stone_Query_March_29_2016.csv" 

bnd_path <- "C:\\Data\\TSA02\\DTFolderStructure\\tsa02_2021.gdb"

rds_path <- "C:\\Data\\TSA02\\DTFolderStructure\\STSM\\TSA02\\gisData\\grids\\roads.tif"

sink_path <-"C:\\Data\\TSA02\\DTFolderStructure\\STSM\\TSA02\\gisData\\grids\\sinks.tif"

####### required queries (update names and table for specific TSA)

pre_thlb_query<-"select ogc_fid,thlb_bi from tsa02_pre_thlb;"

stab_query<-" select class2, wkb_geometry from tsa02_stability;"

blk_query<-"select cc_harvest_year, wkb_geometry from tsa02_ar_table join tsa02_skey using (ogc_fid) where cc_harvest_year > 0;"

vdyp_query<-"select feature_id, vol from tsa02_vdyp_thin_clean;"

pfi_query<-"select pfi_id,age, vol from tsa02_vdyp_thin_clean;"

spc1_query<-"select ogc_fid,feature_id,spec_cd_1,cc_harvest_year,included from tsa02_ar2021;"

pfi_spc1_query<-"select ogc_fid,pfi,feature_id,vri_spc,pfi_spc,pfi_spc_pct,harvest_year,included from tsa02_pfi_id230702;"

#Postgres connection object
db = dbConnect(RPostgreSQL::PostgreSQL(), host="localhost", user = "postgres")

# create a dataframe with the extents (xmin, ymin, xmax, ymax) and dimensions (ncol, nrows) of all TSAs in the province (at 100 m resolution):
out_extents <- get_extents()# function requires no argument

# get the template raster extents based on the unit extent
out_ras_template <- ras_template(extents = out_extents, 
                                 tsa_lbl = tsa_lbl, 
                                 res = 100)

#########################create pre-thlb placeholder############################################################## 
# thlb_tab<-"tsa02_ar2021"
# make_pre_thlb_placeholder(db, tsa_lbl,thlb_tab)
####################START#########################################################################################

## ----phys_op----###################################
####get dem for unit

# extract 25m resolution raster template:
tsa02_25m_template <-ras_template25(extents = out_extents,
                  tsa_lbl = tsa_lbl, 
                  res = 25)

# run the get_dem() function to extract 25m resolution elevation for tsa02:
elevation <- get_dem(bnd_path = bnd_path,
                   clip = tsa02_25m_template)

# plot 25 m elevation for tsa02:
plot(elevation)

# date elevation raster
#writeRaster(elevation, "./data/tiff/tsa02_elevation.tif", overwrite = TRUE, datatype = "INT4S")

#######slope ##################################
##covert DEM to percent slope

# run the get_slope() function to convert elevation to slope:
slope <- get_slope(dem = elevation)

# save slope raster to tif folder:
#writeRaster(slope, "./data/tiff/tsa02_slope.tif", overwrite = TRUE, datatype = "INT4S")

plot(slope)

##########stability############################
##rasterize stability field from postgres

stability<-get_stability(db = db,
                         stab_query = stab_query, 
                         ras_template_25m = tsa02_25m_template, 
                         field = "class2"
                         )

plot(stability)

######### Extract Thresholds: 99 percentile for slope and elevation#########################3
## create a cut-block sample cookie cutter to extract thresholds (99 percentiles of slope and elevation)

sampler<-create_sampler(db,blk_query)

unit<-create_unit(bnd_path)

#############inoperable elevation###########################################################################

inoperable_elevation<-elev_inop(elevation,sampler,unit)
plot(inoperable_elevation)

###################################inoperable slope########################################

inoperable_slope<-slp_inop(slope,sampler,unit)
plot(inoperable_slope)

########################aggregate 25m rasters and write df to postgres################
#phys_op_df is written to postgres as 'tsaXX_op' table

phy_ops_df<-aggregate(stability, inoperable_elevation,inoperable_slope, out_ras_template)
View(phy_ops_df)

##########ECAS########################################################################

## Get MHV ###########################################################################
f<-Sys.time()
mhv<-get_ecas(stone2023,stone2016,tsa_num)

mhv01<-mhv[[1]]

ecas_df <- as.data.frame(mhv[['tcas_df']])

#general query
#mhv_df<-get_mhv_df(db,vdyp_query,spc1_query,mhv01)

#pfi specific query for Boundary
mhv_df<-get_mhv_pfi_df(db,pfi_query,pfi_spc1_query,mhv01)

View(mhv_df)
h<-Sys.time()
h-f

############ptt#######################################################################

#deciduous = #bldecid

mhv_df <- mhv_df %>% 
  mutate(ptt = case_when(
    pfi_spc == "bldecid" ~ 1 ,
    TRUE ~ 0
  ))



######################Patches##################################################################################


rd <- rast(rds_path)
rd <- subst(rd, NA, 0)


sink <- rast(sink_path)
sink <- subst(sink, NA, 0)

field <- "thlb_bi"
tiff <- "./data/tiff/tsa02_pre_thlb.tif"

num_rast(db = db,
         template_raster = out_ras_template,
         raster_query = pre_thlb_query, 
         field = field, 
         tiff_file = tiff)

thlb<-rast("./data/tiff/tsa02_pre_thlb.tif")

patch<-get_patches(thlb, return_raster = TRUE)

show_patches(thlb)

p_raster <- patch[[1]]$class_1
SpatPatch<-rast(p_raster)

patch_stats<-calculate_lsmm(thlb, 
                 level = "patch",
                 directions = 8,
                 neighbourhood = 8)

patch_stats<-patch_stats%>%
  filter(class == 1)%>%
  pivot_wider(names_from = metric,
              values_from = value)


dist2mill<-distance(sink, target = 0)
plot(dist2mill)


dist2road<-distance(rd, target = 0)
plot(dist2road)


patch_df<-get_patch_table(out_ras_template,
                             SpatPatch,
                             patch_stats,
                             dist2mill,
                             dist2road)

View(patch_df)

############################CycleTime#####################################

ctime_sample<-get_ctime_sample(tsa_num, ecas_df)
bnd <- st_read(bnd_path,
               layer = "bnd")
SpatBnd <- vect(bnd)

ctime_df<-get_ctime(ctime_sample, out_ras_template,SpatBnd)

View(ctime_df)
########################Create master table #################################

op_df<-left_join(mhv_df,phy_ops_df, join_by("ogc_fid" == "cell"))

op_df<-left_join(op_df,patch_df,join_by("ogc_fid" == "cell"))

op_df<-left_join(op_df,ctime_df,join_by("ogc_fid"))

op_df<-op_df%>%
  filter(included ==1)

View(op_df)

# #########add distance indices###############################################

maxD2M <-max(op_df$edist2mill)
maxD2R <-max(op_df$edist2road)
maxCtime <-max(op_df$ctime,na.rm = TRUE)

op_df<-op_df%>%mutate(
  d2mIndex = round(edist2mill/maxD2M,2),
  d2rIndex = round(edist2road/maxD2R,2),
  ctimeIndex = round(ctime/maxCtime,2),
  distance_index = (d2rIndex + d2rIndex+ctimeIndex)/3
)
 
op_df<-op_df%>%
  mutate(isolated = case_when(
    area <= 3 ~ 1,
    area <= 5 & enn > 200 ~ 1,
    area <= 25 & enn > 500 & distance_index > .43 ~ 1,
    TRUE ~ 0
  ))

sum(op_df$isolated)


#write table to postgres
if (dbExistsTable(db,postgres_table)) {
  dbRemoveTable(db, postgres_table)
}
dbWriteTable(db, postgres_table, op_df, row.names = FALSE)

#query<-"select ogc_fid,distance_index from tsa02_operability;"
query<-paste0("select ogc_fid,distance_index from ", postgres_table,";")
field<-"distance_index"
tiff<-"tsa02_dist_index.tif"

num_rast(db,query,field,tiff) 

#https://cnuge.github.io/post/progress_bar/
end<-Sys.time()
end - start

dbDisconnect(db)


