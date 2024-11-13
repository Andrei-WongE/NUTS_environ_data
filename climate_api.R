# Function to download climate data-----
get_climate_data <- function(geocode,
                             collection_code = "cmip6-x0.25",
                             type_code = "climatology",
                             variable_codes,        # Max 3 variables
                             product_code = "anomaly",
                             aggregation_code = "annual",
                             period_code = "2040-2059",
                             percentile_code = "median",
                             scenario_codes = "ssp245,ssp585",  # allows combined scenarios
                             model_code = "ensemble",
                             model_calculation_code = "all",
                             statistic_code = "mean") {
  
  # New base URL
  base_url <- "https://cckpapi.worldbank.org/cckp/v1" # Base URL corrected
  
  # Combine variables with comma
  variables_str <- paste(variable_codes, collapse = ",")
  
  # Construct API path
  api_path <- paste(
    collection_code,
    type_code,
    variables_str,
    product_code,
    aggregation_code,
    period_code,
    percentile_code,
    scenario_codes,
    model_code,
    model_calculation_code,
    statistic_code,
    sep = "_"
  )
  
  # Construct full URL
  url <- file.path(base_url, paste0(api_path, "/", geocode, "?_format=json"))
  
  # Print status and URL for verification
  message("\nAPI Path: ", api_path)
  message("Full URL: ", url)
  
  # Make API request with error handling
  tryCatch({
    message("\nMaking API request...")
    response <- GET(
      url,
      add_headers(
        "Accept" = "application/json",
        "Content-Type" = "application/json",
        "User-Agent" = "R-climate-data-client/1.0"
      ),
      config = config(ssl_verifypeer = FALSE)
    )
    
    message("Status code: ", status_code(response))
    
    if (http_status(response)$category == "Success") {
      # Parse JSON response
      data <- fromJSON(rawToChar(response$content))
      
      # Convert to data frame
      if ("data" %in% names(data) && length(data$data) > 0) {
        df <- as.data.frame(data$data)
        
        # Add metadata
        df$geocode <- geocode
        df$date_retrieved <- Sys.Date()
        df$variables <- variables_str
        df$scenarios <- scenario_codes
        
        # Save raw data
        raw_file <- here("Data", "raw_climate", 
                         paste0("climate_raw_", geocode, "_", 
                                paste(variable_codes, collapse = "_"), "_",
                                Sys.Date(), ".rds"))
        saveRDS(data, raw_file)
        
        return(df)
      } else {
        stop("No data in response")
      }
    } else {
      message("\nResponse content: ")
      message(rawToChar(response$content))
      stop(paste("API request failed with status:", http_status(response)$message))
    }
  }, error = function(e) {
    message("\nError details: ", e$message)
    return(NULL)
  })
}


# Function to batch process climate data in parallel -----
get_climate_data_batch_parallel <- function(geocode,
                                            variables = c("tas"
                                                          , "cdd65"
                                                          , "hdd65"
                                                          , "hd30"
                                                          , "hd35"
                                                          , "fd"
                                                          , "id"
                                                          , "r20mm"),
                                            chunk_size = 3) {  # Process 3 variables at a time
  
  # Set up parallel processing
  future::plan(future::multisession)
  
  # Split variables into chunks of 3
  var_chunks <- split(variables, ceiling(seq_along(variables)/chunk_size))
  
  # Create progress tracking
  total_chunks <- length(var_chunks)
  message("\nProcessing ", length(variables), " variables in ", total_chunks, " chunks")
  
  # Process chunks in parallel
  results <- future_map(seq_along(var_chunks), function(chunk_idx) {
    current_vars <- var_chunks[[chunk_idx]]
    
    message("\nProcessing chunk ", chunk_idx, " of ", total_chunks, 
            ": ", paste(current_vars, collapse = ", "))
    
    # Make API request for current chunk
    result <- get_climate_data(
      geocode = geocode,
      variable_codes = current_vars,
      scenario_codes = "ssp245,ssp585"  # Both scenarios in one request
    )
    
    return(result)
  }, .progress = TRUE)
  
  # Combine results
  final_df <- bind_rows(results)
  
  # Save combined results
  if (!is.null(final_df) && nrow(final_df) > 0) {
    output_file <- here("Output", "climate_data", 
                        paste0("climate_data_", geocode, "_all_", Sys.Date(), ".csv"))
    write.csv(final_df, output_file, row.names = FALSE)
    message("\nCombined results saved to: ", output_file)
  }
  
  # Clean up parallel processing
  future::plan(future::sequential)
  
  return(final_df)
}

# Test function with error handling
test_parallel_api <- function(geocode = "FRA") {
  tryCatch({
    message("\nStarting parallel climate data retrieval for ", geocode)
    
    # All variables to process
    variables <- c("tas", "cdd65", "hdd65", "hd30", "hd35", "fd", "id", "r20mm")
    
    # Get data
    results <- get_climate_data_batch_parallel(
      geocode = geocode,
      variables = variables
    )
    
    if (!is.null(results)) {
      message("\nSuccessfully retrieved data:")
      message("Total rows: ", nrow(results))
      
      # Summary by variable
      summary <- results %>%
        group_by(variables) %>%
        summarise(
          n_observations = n(),
          .groups = 'drop'
        )
      
      message("\nSummary by variable chunk:")
      print(summary)
      
      return(results)
    } else {
      message("\nNo data retrieved")
      return(NULL)
    }
    
  }, error = function(e) {
    message("\nError in parallel processing: ", e$message)
    return(NULL)
  }, finally = {
    # Ensure we always clean up parallel processing
    future::plan(future::sequential)
  })
}
# Blood, sweat, and tears went into this function.
