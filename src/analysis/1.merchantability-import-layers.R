library(dadmtools)
# Load required libraries
library(dplyr)   # For data manipulation
library(openxlsx)  # For reading and writing Excel files
source('src/utils/functions.R')
conn_list <- dadmtools::get_pg_conn_list()
