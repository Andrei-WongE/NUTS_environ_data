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
source("Master_script.R")

# Get data for all variables and scenarios

cmip6 <- get_climate_data_batch_parallel(variables = c("tas", "cdd65"
                                                       , "hdd65","hd30"
                                                       , "hd35", "fd"
                                                       , "id", "r20mm")
                                         , collection = "cmip6-x0.25"
                                         , scenarios = c("ssp245", "ssp585")
                                         , chunk_size = 3
                                         )

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
