#-----------------------------------------------------------------------------------------------------------------------------
#-R Script to generate spatial outputs by Region, District for the Old Growth 2020 Dataset
#
#-Mike Fowler
#-Spatial Data Analyst
#-Forest Analysis & Inventory Branch
#-June 16, 2021
#-----------------------------------------------------------------------------------------------------------------------------
library(sf)
# library(gdalUtils)
library(dplyr)
library(DBI)
library(RPostgres)
library(zip)
library(tools)
library(dadmtools)
#-----------------------------------------------------------------------------------------------------------------------------
#-Functions
#-----------------------------------------------------------------------------------------------------------------------------
DeleteFile<-function(inFile){
  if (file.exists(inFile)){
    file.remove(inFile)
  }
}

gdalrast_Process<-function(sql, src, outName, outType='Int32', test){
  if(!test){
    ext <- '-te 159587.5 173787.5 1881187.5 1748187.5'
    }else {ext <- ''}
  ot <- paste('-ot', outType)

  #cmd <- paste('gdal_rasterize', '-a raster_value', '-a_nodata -99', ot, '-a_srs EPSG:3005', ext, '-tr 100 100', paste0('-sql ', '"', sql, '"'), src, outName)
  cmd <- paste('gdal_rasterize', '-a raster_value', '-a_nodata -99', ot, '-a_srs EPSG:3005', ext, '-tr 100 100', paste0('-sql ', shQuote(sql)), src, outName)
  print(cmd)
  print(system2('gdal_rasterize',args=c('-a raster_value',
                                  '-a_nodata -99',
                                  ot,
                                  '-a_srs EPSG:3005',
                                  ext,
                                  '-tr 100 100',
                                  paste0('-sql ', '"', sql, '"'),
                                  src,
                                 outName), stderr = TRUE))
}
gdal_Compress<-function(inTif, compressType='LZW'){
  #--create a temp tiff name to compress to
  tmpTif <- paste0(file_path_sans_ext(inTif), "_zzComp.tif")
  #--Delete that file if already exists
  DeleteFile(tmpTif)
  print(tmpTif)
  print(inTif)
  #--Ouput the command we will be executing
  cmd <- paste('gdal_translate', '-co COMPRESS=', compressType, inTif, tmpTif)
  print(cmd)
  print(system2('gdal_translate',args=c(
    paste0('-co COMPRESS=', compressType),
    shQuote(inTif),
    shQuote(tmpTif)
  ), stderr = TRUE))

  if (file.exists(tmpTif)){
    DeleteFile(inTif)
    file.rename(tmpTif, inTif)
  }


}

#-----------------------------------------------------------------------------------------------------------------------------
#-Main Body
#-----------------------------------------------------------------------------------------------------------------------------
currDir <- dirname(rstudioapi::getSourceEditorContext()$path)
outFolder <- currDir
tiffCSV <- file.path(outFolder, '../utils/GenerateTIFFS.csv')
#outFolder <- "D:\\Projects\\Old_Growth_Analysis\\test"
outZip <- paste0(outFolder, '\\', 'TechPanel_Tiffs.zip')


test <- FALSE
zipit <- FALSE
zips <- c()
#------------------------------------------
#Loop through our Regions
#------------------------------------------
df <- read.csv(tiffCSV, stringsAsFactors = FALSE)
conn_list <- dadmtools::get_pg_conn_list()

dsn=glue("PG:\"dbname='{conn_list$dbname}' host='{conn_list$host}' user='{conn_list$user}' password='{conn_list$password}' \"")
for (row in 1:nrow(df)) {
  name <- df[row, "tif_name"]
  sql <- df[row, "code"]
  if (test){
    sql <- paste0(sql, ' limit 1000')
    }
  output <- df[row, "output"]
  active <- df[row, "active"]
  outType <- df[row, "out_type"]
  if (active=='Y'){
    print('-----------------------------------------------------------------------------------------')
    print(paste0('Processing Tif-', name, '-', Sys.time()))
    print('-----------------------------------------------------------------------------------------')
    timeStart <- Sys.time()
    #--Create output names
    outTif <- output
    print(outTif)
    print(sql)
    gdalrast_Process(sql, dsn, outTif, outType, test)
    gdal_Compress(outTif)

    zips <- append(zips, outTif)
    print('-----------------------------------------------------------------------------------------')
    print(paste0('Finished Processing...(', Sys.time(), ')'))
    print('-----------------------------------------------------------------------------------------')
  }
}

if(zipit){
  #--------------------------------------------------------------------------------------
  #--Zipping up the output(s)
  #--------------------------------------------------------------------------------------
  print(paste0('Zipping up the Output...(', Sys.time(), ')'))
  DeleteFile(outZip)
  zip::zipr(outZip, zips, include_directories=FALSE)
}






