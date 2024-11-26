## ---------------------------
##
## Script name: NUTS
##
## Project: NUTS 1 and 2 aggregation
##
## Purpose of script: 
##
## Author: Andrei Wong Espejo
##
## Date Created: 2024-11-11
##
## Email: awonge01@student.bbk.ac.uk
##
## ---------------------------
##
## Notes: 
##   
##
## ---------------------------

## Load required packages ----
source("Master_script.R")

dir.create(here("Data", "nuts"), recursive = TRUE)


## Runs the following --------
# 1. Load the NUTS data
# 2. 

# https://github.com/antaldaniel/eurostat_regional/blob/master/Rearrange-Regional-Data.md
# Nuts R package: https://cran.r-project.org/web/packages/nuts/vignettes/nuts.html

# Data is from https://ec.europa.eu/eurostat/web/gisco/geodata/statistical-units/territorial-units-statistics

## Loading data --------
require(nuts)

nuts <- vect(here("Data", "nuts","NUTS_RG_20M_2016_3035.geojson")) %>% 
        

glimpse(nuts)

# #  A SpatVector 2,016 x 8
# #  Geometry type: Polygons
# #  Projected CRS: ETRS89-extended / LAEA Europe (EPSG:3035)
# #  CRS projection units: meter <m>
# #  Extent (x / y) : ([-2,823,672 / 10,026,276] , [-3,076,354 /  5,410,788])
# 
# $ NUTS_ID    <chr> "ES", "FI", "IS", "PT2", "FR", "HR", "HU", "AL", "AT", "B…
# $ LEVL_CODE  <int> 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, …
# $ CNTR_CODE  <chr> "ES", "FI", "IS", "PT", "FR", "HR", "HU", "AL", "AT", "BE…
# $ NAME_LATN  <chr> "ESPAÑA", "SUOMI / FINLAND", "ÍSLAND", "REGIÃO AUTÓNOMA D…
# $ NUTS_NAME  <chr> "ESPAÑA", "SUOMI / FINLAND", "ÍSLAND", "REGIÃO AUTÓNOMA D…
# $ MOUNT_TYPE <int> 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, …
# $ URBN_TYPE  <int> 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, …
# $ COAST_TYPE <int> 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, …

terra::values(nuts) %>% View()

# Creating separate NUTS level objects
nuts1 <- nuts[nuts$LEVL_CODE == 1,]
nuts2 <- nuts[nuts$LEVL_CODE == 2,]

# Verify CRS matches population data
nuts1 <- project(nuts1, "EPSG:3035") 
nuts2 <- project(nuts2, "EPSG:3035")

crs(nuts1) == crs(pop2015) # TRUE

