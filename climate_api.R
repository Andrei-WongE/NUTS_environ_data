# Function to download climate data from WB AWS S3-----
get_climate_data <- function(collection = "cmip6-x0.25",
                             variable_code, 
                             product = "climatology",
                             scenario = "ssp245", 
                             aggregation = "annual",
                             model = "ensemble-all",
                             time_period = "2040-2059",
                             percentile = "median",
                             product_type = "climatology",
                             statistic = "mean") {
  
  # Construct file path following CCKP structure
  model_scenario <- paste0(model, "-", scenario)
  
  filename <- paste0(
    # First part with hyphens
    paste(product, variable_code, aggregation, statistic, sep="-"),
    # Second part with underscores
    "_", collection, "_", model_scenario, "_", product_type, "_", 
    percentile, "_", time_period, ".nc"
  )
  
  local_file <- file.path(here::here("Data", "raw_climate"), filename)
  
  url <- paste0(
    "https://wbg-cckp.s3.amazonaws.com/data/",
    collection, "/", 
    variable_code, "/",
    model_scenario, "/",
    filename
  )
  
  # print(url)
  
    response <- httr::GET(
      url,
      httr::write_disk(local_file, overwrite = TRUE),
      httr::progress()
    )
    
    if(httr::status_code(response) != 200) {
      stop(paste("Failed to download file:", httr::http_status(response)$message))
    }
    
    df <- terra::rast(local_file,lyrs = 1)
    
    return(df)
}

# Function to batch process climate data in parallel from AWS S3 -----
get_climate_data_batch_parallel <- function(variables = c("tas", "cdd65", "hdd65", 
                                                          "hd30", "hd35", "fd", 
                                                          "id", "r20mm"),
                                            collection = "cmip6-x0.25",
                                            scenarios = c("ssp245", "ssp585"),
                                            chunk_size = 3) {
  
  jobs <- expand.grid(variable = variables,
                      scenario = scenarios,
                      stringsAsFactors = FALSE
                      )
  
  # Split variables into chunks
  job_chunks <- split(jobs, ceiling(seq_along(1:nrow(jobs))/chunk_size))  
  
  # Set up parallel processing
  future::plan(future::multisession
               , workers = parallel::detectCores() - 2
               )
  
  on.exit(future::plan(future::sequential))
  
  # Progress tracking
  total_chunks <- length(job_chunks)
  message("\nProcessing ", length(variables), " variables in "
          , total_chunks, " chunks")
  
  future_map(seq_len(nrow(jobs)), 
             function(i) {
               tryCatch({
                 get_climate_data(
                   variable_code = jobs$variable[i],
                   scenario = jobs$scenario[i]
                 )
               }, error = function(e) {
                 message(sprintf("Failed: %s-%s: %s", 
                                 jobs$variable[i], jobs$scenario[i], e$message))
               })
             },
             .options = furrr::furrr_options(seed = TRUE),
             .progress = TRUE)
  
  gc()
  
  # List downloaded .nc files
  nc_files <- list.files(here("Data", "raw_climate")
                         , pattern = ".nc$"
                         , full.names = TRUE
  )
  
  combined <- terra::rast(nc_files)  # Does not work with a SpatRasterCollection
  
  # Write consolidated NetCDF
  terra::writeCDF(combined
                  , here::here("Output","climate_data_cmip6-x0.25_combined.nc" )
                  , overwrite = TRUE)
  
  message("Data consolidated...")
  return(combined)
}


# Test function with error handling
# test_parallel_api <- function(geocode = "FRA") {
#   tryCatch({
#     message("\nStarting parallel climate data retrieval for ", geocode)
#     
#     # All variables to process
#     variables <- c("tas", "cdd65", "hdd65", "hd30", "hd35", "fd", "id", "r20mm")
#     
#     # Get data
#     results <- get_climate_data_batch_parallel(
#       geocode = geocode,
#       variables = variables
#     )
#     
#     if (!is.null(results)) {
#       message("\nSuccessfully retrieved data:")
#       message("Total rows: ", nrow(results))
#       
#       # Summary by variable
#       summary <- results %>%
#         group_by(variables) %>%
#         summarise(
#           n_observations = n(),
#           .groups = 'drop'
#         )
#       
#       message("\nSummary by variable chunk:")
#       print(summary)
#       
#       return(results)
#     } else {
#       message("\nNo data retrieved")
#       return(NULL)
#     }
#     
#   }, error = function(e) {
#     message("\nError in parallel processing: ", e$message)
#     return(NULL)
#   }, finally = {
#     # Ensure we always clean up parallel processing
#     future::plan(future::sequential)
#   })
# }

# Blood, sweat, and tears went into this function.
