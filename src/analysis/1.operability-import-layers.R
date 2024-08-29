library(dadmtools)
library(httr)
library(rvest)
source('src/utils/functions.R')
conn_list <- dadmtools::get_pg_conn_list()

# Define the URL
url <- "https://www.env.gov.bc.ca/esd/distdata/ecosystems/TEI/TEI_Data/"

# Read the webpage content
webpage <- read_html(url)

# Extract all the links
links <- webpage %>% html_nodes("a") %>% html_attr("href")

# Filter the links that end with .zip
zip_files <- links[grepl("\\.zip$", links)]

# If the links are relative, prepend the base URL
zip_files <- ifelse(grepl("^http", zip_files), zip_files, paste0(url, zip_files))

# Print the list of .zip files
print(zip_files)

# Download the file
download.file(url, destfile)