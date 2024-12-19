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
# 2. Create separate NUTS level objects
# 3. Sets CRS to Mollweide

# https://github.com/antaldaniel/eurostat_regional/blob/master/Rearrange-Regional-Data.md
# Nuts R package: https://cran.r-project.org/web/packages/nuts/vignettes/nuts.html

# Data is from https://ec.europa.eu/eurostat/web/gisco/geodata/statistical-units/territorial-units-statistics

## Loading data --------
require(nuts)

nuts <- vect(here("Data", "nuts","NUTS_RG_20M_2016_4326.geojson"))

glimpse(nuts)

# #  A SpatVector 2,016 x 8
# #  Geometry type: Polygons
#  Geodetic CRS: lon/lat WGS 84 (EPSG:4326)
#  Extent (x / y) : ([63° 5' 17.71" W / 55° 50' 17.09" E] , [21° 23' 26.75" S / 71° 7' 5.29" N])
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

crs(nuts1)==crs(nuts2)

# Verifying if NUTS_ID is unique
length(unique(nuts1$NUTS_ID)) == nrow(nuts1)
length(unique(nuts2$NUTS_ID)) == nrow(nuts2)

# Verifying if NUTS_ID has missing valuess or empty values
sum(is.na(nuts1$NUTS_ID)) == 0
sum(is.na(nuts2$NUTS_ID)) == 0

