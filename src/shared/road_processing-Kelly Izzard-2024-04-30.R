library(tidyverse)
library(tidytext)
library(terra)

rm(list = ls())

house_keeping <- function() {
  skey_path <- r"(W:\FOR\VIC\HTS\ANA\Workarea\kelly\skey)"
  
  ifelse(!dir.exists("./data/skey"), 
         copyDirectory(skey_path, 
          "./data/skey", 
          private = TRUE, 
          recursive = TRUE),
         FALSE)
  
  ifelse(!dir.exists("./data/shp"), 
        dir.create("./data/shp"),
         FALSE)
  
  ifelse(!dir.exists("./data/tiff"), 
         dir.create("./data/tiff"),
         FALSE)
  
  getwd()
}

get_extents <- function() {
  #folder_path <- r"(W:\FOR\VIC\HTS\ANA\Workarea\kelly\skey\)"
  folder_path <- "./data/skey/"
  tsa_extent_all <- NULL
  log_files <- list.files(folder_path)# get a list of logfiles
  for (i in 1:length(log_files)) {
    log <- read_tsv(paste0(folder_path, log_files[i]),show_col_types = FALSE)
    first_header <- colnames(log) # return first line that contains TSA number
    tsa_position <- str_locate(first_header, "tsa") # find the position of the tsa string in the column label
    tsa_start_pos <- tsa_position[1] # store the starting position in the vector tsa_start_pos
    tsa_label <- str_sub(first_header, tsa_start_pos, -1) # extract the tsa string from the label from the start position to the end
    tsa_num <- str_sub(tsa_label, 1, 5) # extract just the tsa number
    colnames(log)[1] <- tsa_label # assign the tsa_label to the column name
    rows <- c(3, 5, 7, 9, 12, 13) # identify the rows that 'will' contain the extent metrics
    extent <- log[5:6, ] # limit the df to the rows that contain the extents
    extent %>%
      mutate(
        id = row_number(), # create an id fire for the two initial rows
        tsa = tsa_num # create a tsa number column
      ) %>% 
      unnest_tokens(extents, tsa_label) %>% # use tidytext::unnest function to create rows for each character string
      select(tsa, extents) %>% # get rid of the initial column name
      mutate(row_num = row_number()) %>% # assign a row number id to each row
      filter(row_num %in% rows) %>% # filter on rows containing extent values
      mutate(extent_lbl = case_when( # create extent label
        row_num == 3 ~ "x_min",
        row_num == 5 ~ "y_min",
        row_num == 7 ~ "x_max",
        row_num == 9 ~ "y_max",
        row_num == 12 ~ "n_col",
        TRUE ~ "n_row"
      )) %>%
      select(tsa, extent_lbl, extents) %>%
      mutate(extents = as.numeric(extents)) %>% #coerce to numeric
      pivot_wider(names_from = extent_lbl, values_from = extents) -> tsa_extent
    tsa_extent_all <-bind_rows(tsa_extent,tsa_extent_all)%>% #append each resulting df to the output
      arrange(tsa) # arrange in ascending order
    
 
  }
  return(tsa_extent_all)
}

ras_template <- function(extents, tsa_lbl, res) {
  nr <- out_extents %>%
    filter(tsa == tsa_lbl) %>%
    pull(n_row)
  nc <- out_extents %>%
    filter(tsa == tsa_lbl) %>%
    pull(n_col)
  xmn <- out_extents %>%
    filter(tsa == tsa_lbl) %>%
    pull(x_min)
  xmx <- out_extents %>%
    filter(tsa == tsa_lbl) %>%
    pull(x_max)
  ymn <- out_extents %>%
    filter(tsa == tsa_lbl) %>%
    pull(y_min)
  ymx <- out_extents %>%
    filter(tsa == tsa_lbl) %>%
    pull(y_max)

  x <- rast(
    nrows = nr, ncols = nc, xmin = xmn, xmax = xmx,
    ymin = ymn, ymax = ymx,
    crs = "epsg:3005",
    resolution = c(res, res),
    vals = -99
  )
}


## 5 meter

ras_template5 <- function(extents, tsa_lbl, res) {
  nr <- out_extents %>%
    filter(tsa == tsa_lbl) %>%
    pull(n_row) * 20
  nc <- out_extents %>%
    filter(tsa == tsa_lbl) %>%
    pull(n_col) * 20
  xmn <- out_extents %>%
    filter(tsa == tsa_lbl) %>%
    pull(x_min)
  xmx <- out_extents %>%
    filter(tsa == tsa_lbl) %>%
    pull(x_max)
  ymn <- out_extents %>%
    filter(tsa == tsa_lbl) %>%
    pull(y_min)
  ymx <- out_extents %>%
    filter(tsa == tsa_lbl) %>%
    pull(y_max)
  
  x <- rast(
    nrows = nr, ncols = nc, xmin = xmn, xmax = xmx,
    ymin = ymn, ymax = ymx,
    crs = "epsg:3005",
    resolution = c(res, res),
    vals = -99
  )
}

get_rds <- function(tsa_lbl,path2roads) {
  tsa_num<-str_sub(tsa_lbl,-2,-1)
  q<-paste0("select * from roads_sp where TSA_NUMBER = '", tsa_num , "'")
  rds<-st_read(path2roads,
               query = q)
 
  rds<-rds%>%
    mutate(grid = 1)%>%
    rename(rd_cls = Integrated_Road_Class_Num)%>%
    select(rd_cls,grid)
  rds <- st_cast(rds,"MULTIPOLYGON")
  
  Spat_rds_buf<-vect(rds)
}


