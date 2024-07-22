#Rasterization functions

#Housekeeping function gets the extents tables and sets up the folder structure for the project
# if the folders already exist, the message "FALSE" will be returned
# I have created a TSA02_operability R project where the folders are created the first time the house_keeping() function is run

house_keeping <- function() {
  
  # set path to provincial create skey stored on spatialfiles2 in Kelly's workarea.
  skey_path <- r"(W:\VIC\HTS\ANA\Workarea\kelly\skey)"
  
  # check wether the subfolder 'skey' exists within the 'data' folder of your working directory. Folder skey created and files are copied from the skey_path. If the folder already exists, the code will return "FALSE".
  ifelse(!dir.exists("./data/skey"), 
         copyDirectory(skey_path, 
          "./data/skey", 
          private = TRUE, # If TRUE, files (and directories) starting with a period is also copied, otherwise not.
          recursive = TRUE), # If TRUE, sub-directories are copied too, otherwise not.
         FALSE)
  
  ifelse(!dir.exists("./data/shp"), 
        dir.create("./data/shp"),
         FALSE)
  
  ifelse(!dir.exists("./data/tiff"), 
         dir.create("./data/tiff"),
         FALSE)
  
  ifelse(!dir.exists("./data/cats"), 
         dir.create("./data/cats"),
         FALSE)
}


# standardize the table styling; input argument (x) is a data.frame:
  # booktabs = TRUE - no vertical lines (could be added with vline); table only has horizontal lines for the table header and bottom row.
  # bog.mark = "," indicates that 1000ths will be separated by a comma
  # set caption and styling (striped rows, font size)
pretty_table<-function(x){
  kable(x,
        booktabs = T,
        format.args = list(big.mark = ","),
        caption =  "Operability Table" ) %>% 
    kable_styling(bootstrap_options = "striped",
                  font_size = 10)
}

# The getextents() function extracts the extents (xmin, ymin, xmax, ymax), number of rows (n_row) and number of columns (n_col) from the skey log files and consolidates them into a dataframe where each observation is a tsa.

get_extents <- function() {
  folder_path <- "./data/skey/" # point to a different directory.
  tsa_extent_all <- NULL # placeholder for final table to be created.
  log_files <- list.files(folder_path)# get a list of logfiles in the skey folder
  # for each log file, do the following - the for loop means that the all the steps identified within the for loop will be repeated for each log file listed in the skey directory.
  for (i in 1:length(log_files)) {
    log <- read_tsv(paste0(folder_path, log_files[i]),show_col_types = FALSE) # read tab separated value (TSV) file [i] and store as 'log'. For TSA02, the log file is loaded as a table with 1 variable and 29 observations
    first_header <- colnames(log) # return first line that contains TSA number
    tsa_position <- str_locate(first_header, "tsa") # find the position of the tsa string in the column label
    tsa_start_pos <- tsa_position[1] # store the starting position in the vector tsa_start_pos
    tsa_label <- str_sub(first_header, tsa_start_pos, -1) # extract the tsa string from the label from the start position to the end
    tsa_num <- str_sub(tsa_label, 1, 5) # extract just the tsa number; e.g., "tsa02"
    colnames(log)[1] <- tsa_label # assign the tsa_label to the column name
    rows <- c(3, 5, 7, 9, 12, 13) # identify the rows that 'will' contain the extent metrics
    extent <- log[5:6, ] # limit the df to the rows that contain the extents
    extent %>%
      mutate(
        id = row_number(), # create an id for the two initial rows
        tsa = tsa_num # create a tsa number column
      ) %>% 
      unnest_tokens(extents, tsa_label) %>% # use tidytext::unnest function to create rows for each character string within the rows containing extent metrics (rows 5 and 6). the id column helps to visualize the source of the 'extent' records.
      select(tsa, extents) %>% # get rid of the initial column name
      mutate(row_num = row_number()) %>% # assign a row number id to each row - can see that x_min value is stored in row 3.
      filter(row_num %in% rows) %>% # filter on rows containing extent values ('rows' specified above)
      mutate(extent_lbl = case_when( # create extent label
        row_num == 3 ~ "x_min",
        row_num == 5 ~ "y_min",
        row_num == 7 ~ "x_max",
        row_num == 9 ~ "y_max",
        row_num == 12 ~ "n_col",
        TRUE ~ "n_row"
      )) %>%
      select(tsa, extent_lbl, extents) %>% # select only those columns that are needed and in the order preferred.
      mutate(extents = as.numeric(extents)) %>% #coerce to numeric
      pivot_wider(names_from = extent_lbl, values_from = extents) -> tsa_extent # pivot data to a wider table where all extent and dimension variable are stored as one observation for the given tsa. 
    
    # bind each iteration of the for loop together into tsa_extent_all dataframe.
    tsa_extent_all <-bind_rows(tsa_extent,tsa_extent_all)%>% #append each resulting df to the output
      arrange(tsa) # arrange in ascending order
    
 
  }
  return(tsa_extent_all) # final extant table is returned.
}


#The ras_template function creates the terra raster template based on the TSA selected from the extents dataframe
# It assigns a default vale of -99 to every cell
# three arguments are needed: extents, tsa_lbl, and res:
  # extents is a dataframe that contains the extent information by tsa_num (output of tsa_extent_all function above) 
  # tsa_lbl is the tsa of interest; e.g., "tsa02"
  # res is the resolution (e.g. 100 = 100m resolution)

ras_template <- function(extents, tsa_lbl, res) {
  
  filtered_extents <- extents %>% 
    filter(tsa == tsa_lbl)
  
  nr <- filtered_extents %>%
    pull(n_row)
  
  nc <- filtered_extents %>%
    pull(n_col)
  
  xmn <- filtered_extents %>%
    pull(x_min)
  
  xmx <- filtered_extents %>%
    pull(x_max)
  
  ymn <- filtered_extents %>%
    pull(y_min)
  
  ymx <- filtered_extents %>%
    pull(y_max)

  x <- rast(
    nrows = nr, ncols = nc, xmin = xmn, xmax = xmx,
    ymin = ymn, ymax = ymx,
    crs = "epsg:3005",
    resolution = c(res, res),
    vals = -99 # default value
  )
}

# Create a categorical raster.
## the cat_rast function rasterizes categorical variables based on the factor level and produces a STSM ready legend file that associates the level with the category. For numerical variables, see num_rast() function below.
## there are 4 function arguments:
  # db: connection to postgres (e.g., db = dbConnect(RPostgreSQL::PostgreSQL(), host="localhost", user = "postgres")) 
  # raster_query: SQL query to specify what data is being queried from postgres. E.g., pre_thlb_query<-"select ogc_fid,thlb_bi from tsa02_pre_thlb;"
  # field: name of field. E.g., "thlb_bi" for thlb binary
  # template raster for area/unit of interest. E.g., 'out_ras_template'
  # tiff_file: location and name of tif file being saved. E.g., ".data/tiff/tsa02_pre_thlb.tif"

cat_rast<-function(db, raster_query, field, template_raster, tiff_file){
  
  # Load data from the database
  data_from_db <- dbGetQuery(db, raster_query) 
  
  # Data manipulation:
  data_from_db <- data_from_db %>% 
    arrange(ogc_fid) %>%
    mutate(
      ras_col = factor(get(field)), #convert to factor
      level = as.integer(ras_col),
      ras_col = gsub("\\s+", "", ras_col)) %>% #remove white spaces
    dplyr::select( -!!field) #drop field column; the double exclamation mark (!!) operator is used to unquote the 'field' variable.
  
  # Convert template raster to data frame for left join:  
  template_df <- as.data.frame(template_raster, cells = TRUE)
  
  # joined the data with the template:
  joined_data <- left_join(template_df, data_from_db, by = join_by("cell" == "ogc_fid"))
  
  # Set the values in the template raster: 
  values(template_raster) <- joined_data %>% dplyr::select(level)
  
  #write to a TIFF file:
  writeRaster(template_raster, tiff_file, overwrite = TRUE) 
  
  # plot the raster
  plot(template_raster)
  
}


# similar to the cat_rast function above, this function creates the cat files needed in STSM.
# There are 6 function arguments:
  # db: database connection
  # raster_query: the SQL query to the data in postgres
  # field: the field of interest
  # cats_path: not used in the function as written...
  # cat_name: name of file to be saved
  # prefix: allows you to add a prefix to the ras_col variable

cats_file<-function(db,raster_query,field,prefix, cats_path,cat_name){
  
  # Load data from the database
  data_from_db <- dbGetQuery(db, raster_query) 
  
  # Data manipulation:
  data_from_db <- data_from_db %>%
    arrange(ogc_fid) %>%
    mutate(
      ras_col = factor(get(field)), #convert to factor
      level = as.integer(ras_col),
      ras_col = gsub("\\s+", "", ras_col)) %>% #remove white spaces
    mutate(ras_col = case_when(
      !is.na(ras_col) ~ paste0(prefix,ras_col))) %>%
    select( -!!field) #drop field column. the double exclamation mark (!!) operator is used to unquote the 'field' variable.
  
  # create cat file:
  cats <- data_from_db %>%
    filter(!is.na(ras_col)) %>% #remove NAs
    group_by(ras_col) %>% #group by rasterization column
    summarise() %>%
    ungroup() %>%
    mutate(id = row_number(),
           cats = paste(id,ras_col,sep = ":"))%>% #create concatenated legend
    select(cats)
  
  
  legend <- paste0("./data/cats/", cat_name)
  
  # change the path to your cat folder
  write_delim(cats,legend, col_names = FALSE) # write tab delimited text file w/o header
  
}

# The num_rast() function is the same as the cat_rast function, but for numeric raster values rathter than categorical. See cat_rast for more notes.

num_rast <- function(db,raster_query,field, template_raster, tiff_file) {
  
  # load data from database:
  data_from_db <- dbGetQuery(db, raster_query) 
  
  # manipulate data:
  data_from_db <- data_from_db %>%
    arrange(ogc_fid) %>%
    mutate(
      ras_col = get(field)) %>%
    select( -!!field) #drop field column
  
  # Convert template raster to data frame for left join:  
  template_df <- as.data.frame(template_raster, cells = TRUE)
  
  # joined the data with the template:
  joined_data <- left_join(template_df, data_from_db, by = join_by("cell" == "ogc_fid"))
  
  # Set the values in the template raster: 
  values(template_raster) <- joined_data %>% select(ras_col)
  
  #write to a TIFF file:
  writeRaster(template_raster, tiff_file, overwrite = TRUE) 
  
  # plot the raster
  plot(template_raster)
  

}



##ras_template5 creates a 5 meter resolution rater template : useful for lineal features: roads/streams
# refer to the ras_template function for more notes.

ras_template5 <- function(extents, tsa_lbl, res) {
  
  # filter to tsa of interest:
  filtered_extents <- extents %>% 
    filter(tsa == tsa_lbl)
  # pull of extents and dimensions
  nr <- filtered_extents %>%
    pull(n_row) * 20
  nc <- filtered_extents %>%
    pull(n_col) * 20
  xmn <- filtered_extents %>%
    pull(x_min)
  xmx <- filtered_extents %>%
    pull(x_max)
  ymn <- filtered_extents %>%
    pull(y_min)
  ymx <- filtered_extents %>%
    pull(y_max)
  
  x <- rast(
    nrows = nr, ncols = nc, xmin = xmn, xmax = xmx,
    ymin = ymn, ymax = ymx,
    crs = "epsg:3005",
    resolution = c(res, res),
    vals = -99
  )
}

## ras_template25 creates a 25m resolution template: useful for operability mapping 
# refer to the ras_template function for more notes.

ras_template25 <- function(extents, tsa_lbl, res) {
  
  # filter extents to tsa of interest:
  filtered_extents <- extents %>% 
    filter(tsa == tsa_lbl)
  
  # pull out dimensions and extents:
  nr <- filtered_extents %>%
    pull(n_row) * 4
  nc <- filtered_extents %>%
    pull(n_col) * 4
  xmn <- filtered_extents %>%
    pull(x_min)
  xmx <- filtered_extents %>%
    pull(x_max)
  ymn <- filtered_extents %>%
    pull(y_min)
  ymx <- filtered_extents %>%
    pull(y_max)
  
  x <- rast(
    nrows = nr, ncols = nc, xmin = xmn, xmax = xmx,
    ymin = ymn, ymax = ymx,
    crs = "epsg:3005",
    resolution = c(res, res),
    vals = -99
  )
}

## get_dem function extracts the unit's dem from the image warehouse...need network access/VPN active and a mapped path to the image warehouse
## imagefiles.bcgov\imagery (mapped to R: on my computer)
# function has two arguments:
  # bnd_path: path tsa fgdb that includes a 'bnd' feature class.
  # clip: the bounding box for the tsa.

get_dem<-function(bnd_path,clip){
  message("executing get_dem function to extract provincial dem from image warehouse")
  dem_path <-  "R:\\dem\\elevation\\trim_25m\\bcalbers\\tif\\bc_elevation_25m_bcalb.tif"
  bnd <- st_read(bnd_path,
                 layer = "bnd") # get unit boundary
  SpatBnd <- vect(bnd)  # covert to terra::SpatVector
  bc_dem <- rast(dem_path)  #get provincial dem and convert to terra::SpatRaster
  unit_elev <- terra::crop(bc_dem, clip)  #clip provincial raster to unit bounding box
  rm(bc_dem) # remove the provincial dem
  unit_elev_bnd <- mask(unit_elev,SpatBnd)# clip elevation raster from bounding box down to the unit boundary
}

## The get_slope() function converts a unit's dem to percent slope
  # one function argument of dem: the dem raster (e.g., 'elevation' in operability script.)

get_slope<- function(dem){
  message("executing get_slope function to convert unit dem to percent slope")
  slp_degrees <- terra::terrain(dem, "slope") # use terra::terrain function to convert dem to degrees slope
  slp_percent <- tan(pi / 180 * slp_degrees) * 100 # convert degrees to percent
}

## get_stability function extracts stability table to spatial object
# stability table needs to exist in postgres. Function arguments:
  # db = postgress connection
  # stab_query = sql query to the terrain stability data
  # ras_template_25m is the 25m raster template bounding box. E.g., ras_template_25m <-ras_template25(out_extents,tsa_lbl,25)
# NOTE: "class2" is the name given to the binary field in the stability table in postgress. consider adding this as a function argument so that one can easily adjust based on the variable name.

get_stability<-function(db, stab_query, ras_template_25m, field){ # extract stability table from postgres using 25m template ("x")
  op<-st_read(db,query = stab_query) #use sf::st_read function to create stability spatial object
  SpatOP <- vect(op) # convert to terra: SpatVector
  stability <- terra::rasterize(SpatOP, ras_template_25m, field) # rasterize on binary field "class2"
}

# the create_sampler() function extracts the blocks within the unit (specified in the query) - creates a spatial vector of blks
# for example, sampler<-create_sampler(db,blk_query)
# function arguments:
  # db = database connection
  # q = SQL query to the data. for example, blk_query<-"select cc_harvest_year, wkb_geometry from tsa02_ar_table join tsa02_skey using (ogc_fid) where cc_harvest_year > 0;"

create_sampler<-function(db,q){ 
  blks<-st_read(db,query = q) 
  SpatBlks <- vect(blks)
}

# function to create a vector of the boundary of the unit. 
# requires path to fgdb with feature class of "bnd".
create_unit<-function(bnd_path){
  bnd <- st_read(bnd_path,
                 layer = "bnd")
  SpatBnd <- vect(bnd)
}


# The elev_inop() function determines the 99 percentile of elevation within blocks ('sampler' output)
# function arguments:
  # elevation: the 25m elevation raster
  # sampler: the spatial vector of blks 
  # unit: The spatial vector of the boundary of interest (e.g., unit<-create_unit(bnd_path))

elev_inop<-function(elevation,sampler,unit){
  message("executing elev_inop function to sample elevation by blk to determine 99 percent cutoff")
  blk_elev <- terra::extract(elevation,sampler) # extract the elevation within the boundary of 'sampler' in this case cutblocks.
  blkelev99<-quantile(blk_elev$bc_elevation_25m_bcalb, probs = 0.99, na.rm = TRUE) # determine 99th percentile
  message("blk 99 percentile is ", blkelev99)
  
  unit_elev2 <- terra::extract(elevation, unit) # extract elevation for the unit
  unitelev100<-quantile(unit_elev2$bc_elevation_25m_bcalb, probs = 1, na.rm = TRUE) # determine maximum elevation within the unit.
  message("the unit maximum is ", unitelev100)
  
  elev_cutoff <- c(-Inf, blkelev99, 0, blkelev99, unitelev100, 1, unitelev100, Inf, 0)
  elev_matrix <- matrix(elev_cutoff, ncol = 3, byrow = TRUE)
  ### reclassify elevation raster based on cutoffs
  elev_reclass <- terra::classify(elevation, elev_matrix) 
  
}


# determine the 99th percentile of slope within block boundaries.
slp_inop<-function(slope,sampler,unit){
  message("executing slp_inop function to sample slope by blk to determine 99 percent cutoff")
  blk_slp <- terra::extract(slope,sampler)
  blk_slp99<-quantile(blk_slp$slope, probs = 0.99, na.rm = TRUE)
  message("blk 99 percentile is ", blk_slp99)
  
  unit_slp <- terra::extract(slope, unit)
  unit_slp100<-quantile(unit_slp$slope, probs = 1, na.rm = TRUE)
  message("the unit maximum is ", unit_slp100)
  
  slp_cutoff <- c(-Inf, blk_slp99, 0, blk_slp99, unit_slp100, 1, unit_slp100, Inf, 0)
  slp_matrix <- matrix(slp_cutoff, ncol = 3, byrow = TRUE)
  #### reclassify elevation slope based on cutoffs
  slp_reclass <- terra::classify(slope, slp_matrix) 
  
}
# The aggregate function combines the area that is inoperable due to elevation, stability and slope and aggregates to 100m resoltion. 
# function arguments:
  # stability: binary raster where 1 = unstable (inoperable); 0 = stable (operable)
  # inoperable_elevation: binary raster where 1 = high elevation > 99th percentile (inoperable); 0 = elevation within practice limits (operable)
  # inoperable_slope: binary raster where 1 = high slope > 99th percentile (inoperable); 0 slope within practice limits (operable)
  # out_ras_template: the 100m resolution raster template for the unit.

aggregate<-function(stability, inoperable_elevation,inoperable_slope, out_ras_template){
  message("executing aggregation function to calculate proportion inoperable")
  
  # combine stability, slope and elevation rasters to get overall inoperable raster. values range from 0 (operable) to 3 (all three variables inoperable)
  res25m <- stability + inoperable_elevation + inoperable_slope
  
  ### reclassify the raster values: if the value in the res25m raster cell is between -Inf and 0, the new value is 0; between 1 and 3, the new value is 1; between 3 and Inf, value is 0. Essentially any cell where at least one factor is 'inoperable' gets a value of 1.
  res_cutoff <- c(-Inf, 0, 0, 1, 3, 1, 3, Inf, 0)
  res_matrix <- matrix(res_cutoff, ncol = 3, byrow = TRUE)
  res_reclass <- terra::classify(res25m, res_matrix)
  
  #plot(res_reclass)
  ## aggregate to 1 ha.cells with a value of 16 (4*4) representing 100% inoperable
  ## cells with values < 16 and are partially inoperable
  res_agg <- terra::aggregate(res_reclass, fact = 4, fun = "sum", na.rm = TRUE)
  
  # plot(res_agg)
  ## calculate the proportion of cell that is inoperable - scale by 1000.
  resultant <- round(res_agg * 1000 / 16)
  plot(resultant)
  
  # set the extent of the result to match the 100m raster template
  ext(resultant) <- ext(out_ras_template)
  
  # identify extent of 100m raster template:
  #e<-ext(out_ras_template)
  # assign the 100m raster template extent to the resultant:
  #ext(resultant)<-e
  
  # conver the results and the template to data frames
  tmp2 <- as.data.frame(out_ras_template, cells = TRUE)
  tmp3 <- as.data.frame(resultant, cells = TRUE)
  
  # merge the data frame based on cell ID
  tmp4 <- left_join(tmp2, tmp3, by = join_by("cell"))
  
  # Define a database connection if it's not defined: Remove tsa02_op table if it exists.
  if (dbExistsTable(db, paste(tsa_lbl, "_op", sep = ""))) {
    dbRemoveTable(db, paste(tsa_lbl, "_op", sep = ""))
  }
  
  # write results to postgres table:
  dbWriteTable(db, paste(tsa_lbl, "_op", sep = ""), tmp4, row.names = FALSE)
  #plot(resultant)
  tmp4
}

# This function reads and cleans two ecas data files, combines them and performs data filtering and calculation.
# sPath2023 Path to the 2023 data CSV file.
# sPath2016 Path to the 2016 data CSV file.
# tsa_num = TSA number for filtering.

# the function returns a list containing the 1st percentile minimum harvest volume (mhv) and teh filtered data frame "tcas_df".
# 
get_ecas<-function(sPath2023,sPath2016,tsa){
  message("executing get_ecas function which cleans and combines stone queries: ignore warnings")
  
  # Read data from CSV files:
  ecas2023 <- read_csv(sPath2023,
                       show_col_types = FALSE) %>% 
    rename_all(~ str_replace(., "^\\S* ", "")) %>% # ^ matches the start of string and remove
    clean_names() %>% # make labels snake_case (each space is replaced with an underscore)
    mutate_if(is.numeric, ~ replace_na(., 0)) # if numeric replace NAs with zeros

  ecas2016 <- read_csv(sPath2016,
                       show_col_types = FALSE)%>%
    rename_all(~ str_replace(., "^\\S* ", "")) %>%
    clean_names() %>%
    mutate(
      # convert variables from character to numeric
      man_unit = as.numeric(man_unit), 
      indicated_rate = as.numeric(indicated_rate),
      total_toa_amount = as.numeric(total_toa_amount)
    ) %>%
    mutate_if(is.numeric, ~ replace_na(., 0))
 
  # Identify columns with type mismatches and remove them:
  col_list <- compare_df_cols(ecas2023,  # dump cols from 2016 with type mismatch
                              ecas2016,
                              return = "mismatch") %>%
    select(column_name) %>%
    pull()
  
  ecas2016 <- ecas2016 %>% select(all_of(-col_list)) # removes columns from ecas2016 that are not in ecas 2023
  
  # combine the clean data:
  ecas <- bind_rows(   
    list(
      e2023 = ecas2023,
      e2016 = ecas2016
    ),
    .id = "id"
  ) %>%
    mutate_if(is.numeric, ~ replace_na(., 0)) %>%
    remove_empty("cols") %>%
    group_by(mark) %>%
    # only keep record with most recent appraisal effective date.
    arrange(desc(app_eff_date)) %>%
    slice(1) %>%
    ungroup()
  
  # Filter data based on TSA number and licence types (limit the sample to major licensees and BCTS):
  tsa <- ecas %>% 
    filter(man_unit == tsa_num) %>%
    filter(!str_detect(licence, "T|L|W")) # filter to records where the licence does not contain "T", "L", or "W".
  
  # calculate the volume per hectare based on harvest system. 
  # gscc = ground skidding clear cut
  # chgcc = cable/highlead volume
  tsa <- tsa %>% mutate(tvph = case_when( # assign system values to TVPH variable
    gscc_vol_ha > 0 & chgcc_vol_ha == 0 ~ gscc_vol_ha, 
    chgcc_vol_ha > 0 & gscc_vol_ha == 0 ~ chgcc_vol_ha,
    TRUE ~ ncv / tot_merch_area
  ))
  
  # filter to ground skid clear cut timber marks.
  sub <- tsa %>% filter(gscc_vol > 0 & chgcc_vol_ha == 0)
  
  mhv01<-quantile(sub$gscc_vol_ha, 
           probs = .01)
  message(" The 1 percentile minimum harvest volume is", mhv01 )
  
  # store/return as a list with the mhv variable (1st percentile of observed min vol/ha) and the combined/cleaned ecas data frame.
  # minimum vol/ha is for ground_skid only and includes all species.
  return(list(mhv = mhv01, tcas_df = tsa)) 
}

# Get Maximum Harvest Volume Data Frame:
# This function queries the database to determine maximum volume and apply a 1 percentile cut-off.
#
# db =  A database connection object.
# vdyp_query SQL query for the vdyp table.
# spc1_query SQL query for the spc1 table.
# mhv01 The 1st percentile minimum harvest volume.
#
#' @return A data frame containing feature_id, max_vol, and a binary indicator (less_than_mhv).
#
#' @export

get_mhv_df<-function(db,vdyp_query,spc1_query,mhv01){
  message("executing get_mhv_df to query vdyp tables to determine maximum volume and apply 1 percentile cut-off")
  
  # query and process vdyp data:
  vdyp<-dbGetQuery(db,vdyp_query)%>%
    pivot_wider(names_from = age,
                names_prefix = 'yr',
                values_from = vol)%>%
    # calculate the maximum value across multiple columns - those that start with 'yr'.
    #pmap_dbl is applying the function to each row of the selected columns
    # max(c(...)) is a way to capture all the values in teh selected columns for a particular row and find the maximum among them.
    # a new field is calculated where 1 indicates that the feature_id is less than the minimum harvest volume; 1 indicates it does not.
    mutate(max_vol = pmap_dbl(across(starts_with('yr')), ~ max(c(...))),
           less_than_mhv = case_when(
             max_vol < mhv01 ~ 1,
             TRUE ~ 0))%>%
    select(feature_id, max_vol,less_than_mhv)
  
  # Query spc1 data:
  spc1<-dbGetQuery(db,spc1_query)
  
  # Join the two data frames:
  out_df<-left_join(spc1,vdyp, join_by("feature_id"))
  
}
  
#https://www.rebeccabarter.com/blog/2019-08-19_purrr
# get maximum harvest volume data from the PFI tables:
# This function queries the PFI tables to determine maximum volume and apply a 1 percentile cut-off.
#
# db A database connection object.
# pfi_query SQL query for the PFI table.
# spc1_query SQL query for the spc1 table.
# mhv01 The 1st percentile minimum harvest volume.
#
# Return a data frame containing pfi_id, max_vol, and a binary indicator (less_than_mhv).
#
# @export
get_mhv_pfi_df<-function(db,pfi_query,spc1_query,mhv01){
    message("executing get_mhv_df to query pfi tables to determine maximum volume and apply 1 percentile cut-off")
  
  # Query and process PFI data
    vdyp<-dbGetQuery(db,pfi_query)%>%
    pivot_wider(names_from = age,
                  names_prefix = 'yr',
                  values_from = vol)%>%
      # see get_mhv_df function for notes
    mutate(max_vol = pmap_dbl(across(starts_with('yr')), ~ max(c(...))),
             less_than_mhv = case_when(
               max_vol < mhv01 ~ 1,
               TRUE ~ 0))%>%
    select(pfi_id,max_vol,less_than_mhv)
    
    
  # Query spc1 data
  spc1<-dbGetQuery(db,pfi_spc1_query)
  
  # Join the two data frames
  out_df<-left_join(spc1,vdyp, join_by("pfi" == "pfi_id"))

}


# Generate Patch Table
#
# This function combines data from various sources to generate a patch table.
#
# out_ras_template Raster template.
# SpatPatch Spatial patch data.
# patch_stats Patch statistics data.
# dist2mill Distance to mill data.
# dist2road Distance to road data.
#
# return A data frame containing patch information.
#
#

get_patch_table<-function(out_ras_template,
                          SpatPatch,
                          patch_stats,
                          dist2mill,
                          dist2road){
  
  # Convert out_ras_template to a data frame with a unique identifier for each cell
  tmp1 <- as.data.frame(out_ras_template, cells = TRUE)
  
  #convert to dataframe with unique identifier for each cell
  tmp2 <- as.data.frame(SpatPatch, cells = TRUE)
  
  # join the out_ras_template with the SpatPatch based on cell identifier
  tmp3 <- left_join(tmp1, tmp2, join_by("cell"))
  
  # Join the resulting data frame with patch_stats based on the 'layer' and 'id' columns
  tmp4 <- left_join(tmp3, patch_stats, join_by("layer"=="id"))
  
  # Convert dist2mill to a data frame with a unique identifier for each cell
  tmp1 <- as.data.frame(dist2mill,cells = TRUE)
  
  # Convert dist2road to a data frame with a unique identifier for each cell
  tmp2 <- as.data.frame(dist2road, cells = TRUE)
  
  # Join dist2mill and dist2road based on the cell identifier
  tmp3 <- left_join(tmp1, tmp2, join_by("cell"))
  
  # Join the resulting data frame with tmp4 based on the cell identifier
  tmp4 <- left_join(tmp4, tmp3, join_by("cell"))
  
  # Select specific columns for the final patch table
  tmp5<-tmp4%>%
    select(cell,
           patch_id = layer,
           area,
           enn,
           edist2mill = sinks,
           edist2road = roads)
}



get_ctime_sample<-function(num,edf){
  
  cNum<-ifelse(tsa_num < 10,
               paste0("0",as.character(tsa_num)),
               as.character(tsa_num))
  
  bnd<- bcdc_query_geodata("8daa29da-d7f4-401c-83ae-d962e3a28980") %>%
    filter(TSA_NUMBER == cNum) %>% # filter for Boundary TSA
    collect()
  
  tsa_openings<-bcdc_query_geodata("53a17fec-e9ad-4ac0-95e6-f5106a97e677") %>%
    filter(INTERSECTS(bnd))%>%
    collect()
  
  #clip to the boundary
  tsa_openings_clip <- st_intersection(tsa_openings, bnd)
  
  #view data
  SpatBlk<-vect(tsa_openings_clip) # create spatial vector object
  
  # create centroid from blocks, keep opening_id and timber mark attribute
  SpatCent <- centroids(SpatBlk) %>%
    select(OPENING_ID, TIMBER_MARK) %>%
    mutate(TIMBER_MARK = paste0("_", TIMBER_MARK))
  
  #join datasets
  blk_sample<-merge(SpatCent,edf,by.x = "TIMBER_MARK",by.y = "mark")
  
  # subset to the cycletime variable
  ctime <- blk_sample %>%
    filter(prim_cyc_time_all > 0) %>%
    select(OPENING_ID, TIMBER_MARK, prim_cyc_time_all)
}

get_ctime<-function(sample,template,bnd){
  
  #create df with centroid coordinates and attribute for gstat model from the ctime object
  pt_df <- data.frame(geom(ctime_sample)[,c("x", "y")], as.data.frame(ctime_sample))
  
  #create boundary template model
  x <- out_ras_template
  
  #create the gstats model object using the dataframe with xy cordinates
  gs <- gstat(formula=prim_cyc_time_all~1, locations=~x+y, data=pt_df)
  
  #run the gstats interpolater function with the raster template and gs model
  idw <- interpolate(x, gs, debug.level=0)
  
  # crop the output by the TSA boundary
  idwr <- mask(idw, bnd)
  
  tmp1 <- as.data.frame(out_ras_template,cells = TRUE)
  #convert to dataframe with unique identifier for each cell
  tmp2 <- as.data.frame(idwr, cells = TRUE)
  # join the rasterization value df to geometry frame
  ctime <- left_join(tmp1, tmp2, join_by("cell"))
  ctime <- ctime%>%
    rename(ogc_fid = cell,
           ctime = var1.pred)%>%
    select(ogc_fid,ctime)
}


check_rd_name <- function(name){
  stopifnot(basename(rds_path) == name)
  name
}


make_pre_thlb_placeholder<-function(db,tsa_lbl,thlb_tab){
  pre_thlb<-paste0(tsa_lbl,"_pre_thlb")
    create_query<-paste0("create table ",
                          tsa_lbl,
                          "_pre_thlb as (select a.*, b.thlb_fact from ",
                          tsa_lbl,
                          "_skey a join ",
                          thlb_tab,
                           " b using (ogc_fid));")
    alter_table_query<-paste0("alter table ",
                              tsa_lbl,
                              "_pre_thlb add column thlb_bi numeric")
    update_table_query<-paste0("update ",
                               tsa_lbl,
                               "_pre_thlb set thlb_bi = case when thlb_fact > 0 then 1 else 0 end")
    if(dbExistsTable(db,pre_thlb)) {dbRemoveTable(db,pre_thlb)}
    dbSendQuery(db,create_query)
    message("pre-thlb placeholder created" )
    dbSendQuery(db,alter_table_query)
    dbSendQuery(db,update_table_query)
    message("thlb_bi variable created" )
    }

#######################################
#### other functions; not currently used in the operability script.

