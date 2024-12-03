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
    
  url <- paste0(
    "https://wbg-cckp.s3.amazonaws.com/data/",
    collection, "/", 
    variable_code, "/",
    model_scenario, "/",
    filename
  )

  # print(url)
  
  # Create temp file for download
  temp_file <- tempfile(fileext = ".nc")
  on.exit(unlink(temp_file))

    response <- httr::GET(
      url,
      httr::write_disk(temp_file),
      httr::progress()
    )
    
    if(httr::status_code(response) != 200) {
      stop(paste("Failed to download file:", httr::http_status(response)$message))
    }
    
    df <- terra::rast(temp_file)
    # df <- terra::as.data.frame(r, xy=TRUE)
    
    print(paste("SpatRaster cells:", terra::ncell(df)))
    print(paste("Data frame rows:", nrow(df)))
    
    # Add metadata 
    df$collection <- collection
    df$variable <- variable_code
    df$scenario <- scenario
    df$model <- model
    df$time_period <- time_period
    df$crs <- terra::crs(df)
    df$resolution <- terra::res(df)
    
    return(df)
}

# Function for parallel processing
get_climate_data_batch <- function(variables,
                                   collection = "cmip6-x0.25",
                                   scenario = "ssp245",
                                   chunk_size = 3) {
  
  future::plan(future::multisession)
  
  var_chunks <- split(variables, ceiling(seq_along(variables)/chunk_size))
  
  results <- future_map(var_chunks, function(vars) {
    map_df(vars, function(var) {
      get_climate_data(
        collection = collection,
        variable_code = var,
        scenario = scenario
      )
    })
  }, .progress = TRUE)
  
  final_df <- bind_rows(results)
  
  future::plan(future::sequential)
  
  return(final_df)
}

# Function to batch process climate data in parallel from AWS S3 -----
get_climate_data_batch_parallel <- function(variables = c("tas", "cdd65", "hdd65", 
                                                          "hd30", "hd35", "fd", 
                                                          "id", "r20mm"),
                                            collection = "cmip6-x0.25",
                                            scenarios = c("ssp245", "ssp585"),
                                            chunk_size = 3) {
  
  # Set up parallel processing
  future::plan(future::multisession)
  
  # Split variables into chunks
  var_chunks <- split(variables, ceiling(seq_along(variables)/chunk_size))
  
  # Progress tracking
  total_chunks <- length(var_chunks)
  message("\nProcessing ", length(variables), " variables in ", total_chunks, " chunks")
  
  # Process chunks in parallel
  results <- future_map(seq_along(var_chunks), function(chunk_idx) {
    current_vars <- var_chunks[[chunk_idx]]
    
    message("\nProcessing chunk ", chunk_idx, " of ", total_chunks, 
            ": ", paste(current_vars, collapse = ", "))
    
    # Get data for each scenario
    scenario_results <- lapply(scenarios, function(scenario) {
    
    # Process each variable separately
      var_results <- lapply(current_vars, function(var) {
        scenario_results <- lapply(scenarios, function(scenario) {
          get_climate_data(
            collection = collection,
            variable_code = var,  # Now passing single variable
            scenario = scenario
          )
        })
        
        bind_rows(scenario_results)
      })
      
      if(!is.null(result)) result$scenario <- scenario
      return(result)
    })
    
    return(bind_rows(scenario_results))
  }, .progress = TRUE)
  
  # Combine results
  final_df <- bind_rows(results)
  
  # Save results
  if (!is.null(final_df) && nrow(final_df) > 0) {
    output_file <- file.path("Output", "climate_data", 
                             paste0("climate_data_", collection, "_", 
                                    paste(scenarios, collapse = "_"), "_",
                                    Sys.Date(), ".csv"))
    write.csv(final_df, output_file, row.names = FALSE)
    message("\nCombined results saved to: ", output_file)
  }
  
  # Clean up
  future::plan(future::sequential)
  
  return(final_df)
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
