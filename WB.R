## ---------------------------
##
## Script name: WB
##
## Project:
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

## Runs the following --------
# 1. Downloads cmip6-x0.25m, era5-x0.25 and pop_x0.25 data via API

## Loading data ----

source("Master_script.R")

require(httr)
require(jsonlite)
require(dplyr)
require(ggplot2)
require(tidyr)
require(here)
require(progressr)
require(future)
require(furrr)
require(future.apply)
require(ncdf4)
require(ncmeta)

dir.create(here("Output", "climate_data"), recursive = TRUE)
dir.create(here("Data", "raw_climate"), recursive = TRUE)

source("climate_api.R")


# Get data for all variables and scenarios

# ## cmip6-x0.25-----
# 
# cmip6 <- get_climate_data_batch_parallel(variables = c("tas", "cdd65"
#                                                        , "hdd65","hd30"
#                                                        , "hd35", "fd"
#                                                        , "id", "r20mm")                              
#                                          ,collection = "cmip6-x0.25"
#                                          ,scenarios = c("ssp245", "ssp585")
#                                          ,product = "climatology"
#                                          ,aggregation = "annual"
#                                          ,time_period = "1950-2022"
#                                          ,product_type = "timeseries"
#                                          ,model = "ensemble-all"
#                                          ,percentile = "median"
#                                          ,statistic = "mean"
#                                          ,chunk_size = 3
#                                          )
# 
# ncdf4::nc_open(here("Output", "climate_data_cmip6-x0.25_ssp245_ssp585_combined.nc"))
# raster::brick(here("Output", "climate_data_cmip6-x0.25_ssp245_ssp585_combined.nc"))
# 
# ## era5-x0.25-----
# eras5 <- get_climate_data_batch_parallel(collection = "era5-x0.25"
#                                          ,variables = c("tas", "pr", "tasmax"
#                                                        , "tasmin", "fd")
#                                          ,scenarios = "historical_era5"
#                                          ,product = "climatology"
#                                          ,aggregation = "annual"
#                                          ,time_period = "1950-2022"
#                                          ,product_type = "timeseries"
#                                          ,model = "ensemble-all"
#                                          ,percentile = "median"
#                                          ,statistic = "mean"
#                                          ,chunk_size = 3
#                                         )
# 
# ncdf4::nc_open(here("Output", "climate_data_cmip6-x0.25_ssp245_ssp585_combined.nc"))
# raster::brick(here("Output", "climate_data_cmip6-x0.25_ssp245_ssp585_combined.nc"))


## Parametrized queries----
era5_data_a <- get_climate_data_batch_parallel(
  collection = "era5-x0.25",
  variables = c("tas", "cdd65", "hdd65", "hd30", "hd35", 
                "fd", "id", "r20mm", "r50mm", "pr"),
  scenarios = "historical_era5",
  product = "timeseries",
  aggregation = "annual",
  time_period = "1950-2022",
  product_type = "timeseries",
  model = "ensemble-all",
  percentile = "median",
  statistic = "mean"
)

era5_data_b <- get_climate_data_batch_parallel(
  collection = "era5-x0.25",
  variables = c("tas", "cdd65", "hdd65", "hd30", "hd35", 
                "fd", "id", "r20mm", "r50mm"
                # , "pr"
                ),
  scenarios = "historical_era5",
  product = "climatology", 
  aggregation = "annual",
  time_period = "1995-2014", # 1991-2020, for pr
  product_type = "climatology",
  model = "ensemble-all",
  percentile = "median",
  statistic = "mean"
)

era5_data_c <- get_climate_data_batch_parallel(
  collection = "era5-x0.25",
  variables =  "pr",
  scenarios = "historical_era5",
  product = "climatology",
  aggregation = "annual",
  time_period = "1991-2020",
  product_type = "climatology",
  model = "ensemble-all",
  percentile = "median",
  statistic = "mean"
)

cmip6_data_a <- get_climate_data_batch_parallel(
  collection = "cmip6-x0.25",
  variables = c("tas", "cdd65", "hdd65", "hd30", "hd35", 
                "fd", "id", "r20mm", "r50mm", "pr"),
  scenarios = c("ssp245", "ssp585"),
  product = "anomaly",
  aggregation = "annual", 
  time_period = "2040-2059",
  product_type = "climatology",
  model = "ensemble-all",
  percentile = "median",
  statistic = "mean"
)

cmip6_data_b <- get_climate_data_batch_parallel(
  collection = "cmip6-x0.25",
  variables = c("r20mm", "r50mm", "pr"),
  scenarios = c("ssp245", "ssp585"),
  product = "climatology",
  aggregation = "annual", 
  time_period = "1995-2014", # VERIFY!!!!!!!!
  product_type = "climatology",
  model = "ensemble-all",
  percentile = "median",
  statistic = "mean"
)

pop_x0.25_hist  <- get_climate_data(
  collection = "pop-x0.25",
  variable_code =  "popcount",
  scenario = "historical",
  product = "climatology", 
  aggregation = "annual",
  time_period = "1995-2014",
  product_type = "climatology"
)

pop_x0.25_future  <- get_climate_data(
  collection = "pop-x0.25",
  variable_code =  "popcount",
  scenario = "ssp245",
  product = "climatology", 
  aggregation = "annual",
  time_period = "2040-2059",
  product_type = "climatology"
)

pop_x0.25_future  <- get_climate_data(
  collection = "pop-x0.25",
  variable_code =  "popcount",
  scenario = "ssp585",
  product = "climatology", 
  aggregation = "annual",
  time_period = "2040-2059",
  product_type = "climatology"
)

pattern_pop <- sprintf("^.*pop-x0.25.*\\.nc$")

pop_files <- list.files(here("Data", "raw_climate"),
                       pattern = pattern_pop,
                       full.names = TRUE)

# Move files, inefficient but cute
for (file in pop_files) { 
  
  file.rename(file, file.path(here("Output"), basename(file))) 
  }
