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
# 1. Downloads cmip6-x0.25 and era5-x0.25 data via API
# 2. Performs data checks
# 3. Wrangles data
# 4. 

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
  time_period = "2040-2059",
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

# results <- test_parallel_api("FRA")
# 
# # Or direct use with custom parameters
# results <- get_climate_data_batch_parallel(
#   geocode = "FRA",
#   variables = c("tas", "cdd65", "hdd65", "hd30", "hd35", "fd", "id", "r20mm"),
#   chunk_size = 3
# )

# # European country codes + UK
# eu_countries <- c(
#   "ALB", "AND", "AUT", "BEL", "BIH", "BGR", "HRV", "CZE", "DNK", "EST",
#   "FIN", "FRA", "DEU", "GRC", "HUN", "ISL", "IRL", "ITA", "LVA", "LIE",
#   "LTU", "LUX", "MLT", "MDA", "MCO", "MNE", "NLD", "MKD", "NOR", "POL",
#   "PRT", "ROU", "SMR", "SRB", "SVK", "SVN", "ESP", "SWE", "CHE", "GBR",
#   "VAT"
# )
# 
# # Wrapper function to process European countries with AWS S3 data
# process_european_climate_data <- function(countries = eu_countries,
#                                           chunk_size = 5,
#                                           collection = "cmip6-x0.25",
#                                           scenarios = c("ssp245", "ssp585")) {
#   # Set up parallel processing
#   future::plan(future::multisession)
#   
#   # Split countries into chunks
#   country_chunks <- split(countries, ceiling(seq_along(countries)/chunk_size))
#   
#   # Process chunks in parallel 
#   results <- future_map(country_chunks, function(chunk_countries) {
#     chunk_results <- list()
#     
#     for(country in chunk_countries) {
#       message(sprintf("\nProcessing country: %s", country))
#       
#       tryCatch({
#         chunk_results[[country]] <- get_climate_data_batch_parallel(
#           collection = collection,
#           scenarios = scenarios
#         )
#         
#         # Small delay between countries
#         Sys.sleep(2)
#         
#       }, error = function(e) {
#         message(sprintf("Error processing %s: %s", country, e$message))
#         chunk_results[[country]] <- NULL
#       })
#     }
#     return(chunk_results)
#   }, .progress = TRUE)
#   
#   # Combine all results
#   all_results <- unlist(results, recursive = FALSE)
#   
#   # Create summary
#   summary <- data.frame(
#     country = names(all_results),
#     status = sapply(all_results, function(x) !is.null(x)),
#     rows = sapply(all_results, function(x) if(!is.null(x)) nrow(x) else 0)
#   )
#   
#   # Save results
#   saveRDS(all_results, 
#           file = file.path("Output", 
#                            paste0("european_climate_data_", 
#                                   paste(scenarios, collapse="_"),
#                                   "_", Sys.Date(), ".rds")))
#   
#   write.csv(summary, 
#             file = file.path("Output", 
#                              paste0("european_climate_data_summary_",
#                                     Sys.Date(), ".csv")))
#   # Clean up parallel processing
#   future::plan(future::sequential)
#   
#   return(list(results = all_results, summary = summary))
# }
# 
# # Run forest, run:
# results <- process_european_climate_data(chunk_size = 5)
# print(results$summary)
