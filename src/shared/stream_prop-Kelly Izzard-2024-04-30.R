
library(DBI)
library(RPostgreSQL)
library(tidyverse)
library(sf)
library(terra)
library(R.utils)


rm(list = ls())
#########################run background functions: do not change####################


# get the base rasterization inputs using standard function R script
source("road_processing.R")

# run house_keeping function to create output folder, download skey files
house_keeping()


################## proportion function: do not change###################################
stream_prop <- function(Spat_str_buf, out_ras_template5) {
  tmp1 <- rasterize(Spat_str_buf, out_ras_template5, "grid")
  # convert to dataframe with unique identifier for each cell
  tmp2 <- aggregate(tmp1, 20, fun = "sum", na.rm = TRUE)
  # join the rasterization value df to geometry frame
  tmp3 <- tmp2 * 1000 / 400
  # write raster to tiff folder
  outpath <- ".\\data\\tiff\\stream_prop.tif"

  writeRaster(tmp3,
    outpath,
    datatype = "INT4U",
    NAflag = 0,
    overwrite = TRUE
  )
  plot(tmp3)
}
########################user arguments###################################
tsa_lbl <- "tsa27"
path2shp <- "C:\\Data\\test\\data\\shp\\tsa27_str_buf.shp"


###########run functions: do not change########################################

# run functions to get geometry
out_extents <- get_extents() # function requires no argument
out_extents

# run rasterization function: output raster to data folder and plot
# stsm_raster(out_ras_template, df, ras_col, tiff)
out_ras_template5 <- ras_template5(out_extents, tsa_lbl, 5)
out_ras_template5

Spat_str_buf<-vect(path2shp)

stream_prop(Spat_str_buf, out_ras_template5)

#####################load to postgres##################
path2ras<-".\\data\\tiff\\stream_prop.tif"
str_buf<-rast(path2ras)
out_ras_template<- ras_template(out_extents, tsa_lbl, 100)
frame<- as.data.frame(out_ras_template, cells = TRUE)
df <- as.data.frame(str_buf, cells = TRUE)
upload<- left_join(frame, df, by = join_by("cell"))%>%
  rename(ogc_fid = cell,
         str_buf_prop = grid)%>%
  select(-lyr.1)


db = dbConnect(RPostgreSQL::PostgreSQL(), host="localhost", user = "postgres")
if(dbExistsTable(db,"tsa27_strm_buf_prop")) 
{dbRemoveTable(db,"tsa27_strm_buf_prop")}
dbWriteTable(db, "tsa27_strm_buf_prop",upload,row.names=FALSE)

