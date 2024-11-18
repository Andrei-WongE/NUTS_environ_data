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

# # Simple test
# results <- test_parallel_api("FRA")
# 
# # Or direct use with custom parameters
# results <- get_climate_data_batch_parallel(
#   geocode = "FRA",
#   variables = c("tas", "cdd65", "hdd65", "hd30", "hd35", "fd", "id", "r20mm"),
#   chunk_size = 3
# )

# European country codes + UK
eu_countries <- c(
  "ALB", "AND", "AUT", "BEL", "BIH", "BGR", "HRV", "CZE", "DNK", "EST",
  "FIN", "FRA", "DEU", "GRC", "HUN", "ISL", "IRL", "ITA", "LVA", "LIE",
  "LTU", "LUX", "MLT", "MDA", "MCO", "MNE", "NLD", "MKD", "NOR", "POL",
  "PRT", "ROU", "SMR", "SRB", "SVK", "SVN", "ESP", "SWE", "CHE", "GBR",
  "VAT"
)

# Wrapper function to process all European countries
process_european_climate_data <- function(chunk_size = 5) {
  # Set up parallel processing
  future::plan(future::multisession)
  
  # Split countries into chunks for batch processing
  country_chunks <- split(eu_countries, ceiling(seq_along(eu_countries)/chunk_size))
  
  # Process chunks in parallel
  results <- future_map(country_chunks, function(countries) {
    chunk_results <- list()
    
    for(country in countries) {
      message(sprintf("\nProcessing country: %s", country))
      
      # Try to get data with error handling
      tryCatch({
        chunk_results[[country]] <- get_climate_data_batch_parallel(
          geocode = country
        )
        # Add delay between countries in same chunk to respect rate limits
        Sys.sleep(2)
      }, error = function(e) {
        message(sprintf("Error processing %s: %s", country, e$message))
        chunk_results[[country]] <- NULL
      })
    }
    return(chunk_results)
  }, .progress = TRUE)
  
  # Combine all results
  all_results <- unlist(results, recursive = FALSE)
  
  # Create summary
  summary <- data.frame(
    country = names(all_results),
    status = sapply(all_results, function(x) !is.null(x)),
    rows = sapply(all_results, function(x) if(!is.null(x)) nrow(x) else 0)
  )
  
  # Save combined results
  saveRDS(all_results, file = here::here("Output", "european_climate_data_all.rds"))
  write.csv(summary, file = here::here("Output", "european_climate_data_summary.csv"))
  
  # Clean up parallel processing
  future::plan(future::sequential)
  
  return(list(results = all_results, summary = summary))
}

# Run forest, run:
results <- process_european_climate_data(chunk_size = 5)
print(results$summary)
