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
# 1. Downloads the data via API
# 2. Performs data checks
# 3. Exports data in CSV
# 4. 

## Loading data ----
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

dir.create(here("Output", "climate_data"), recursive = TRUE)
dir.create(here("Data", "raw_climate"), recursive = TRUE)

source("climate_api.R")

# Get data for all variables and scenarios

# Simple test
results <- test_parallel_api("FRA")

# Or direct use with custom parameters
results <- get_climate_data_batch_parallel(
  geocode = "FRA",
  variables = c("tas", "cdd65", "hdd65", "hd30", "hd35", "fd", "id", "r20mm"),
  chunk_size = 3
)