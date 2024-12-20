# Function to download climate data from WB AWS S3-----
get_climate_data <- function(collection,
                             variable_code,
                             product = "climatology",
                             scenario = NULL,
                             aggregation = "annual",
                             time_period = NULL,
                             product_type = "climatology",
                             model = NULL,
                             percentile = NULL,
                             statistic = "mean") {
  
  # Construct file path following CCKP structure
  model_scenario <- paste0(model, "-", scenario)
  
  if(collection == "era5-x0.25") {
    
    collection_id <- paste0(collection, "-historical")
    
    filename <- paste0(
      paste(product_type, variable_code, aggregation, statistic, sep="-"), "_",
      collection, "_",
      collection_id, "_",
      product_type, "_",
      statistic, "_",
      time_period, ".nc"
    )
    
    url <- paste0(
      "https://wbg-cckp.s3.amazonaws.com/data/",
      collection, "/",
      variable_code, "/",
      collection_id, "/",
      filename
    )
    
  } else if (collection == "cmip6-x0.25") {
    
    filename <- paste0(
      paste(product, variable_code, aggregation, statistic, sep="-"),
      "_", collection, "_", 
      paste0(model, "-", scenario), "_", 
      product_type, "_", percentile, "_",
      time_period, ".nc"
    )
    
    url <- paste0(
      "https://wbg-cckp.s3.amazonaws.com/data/",
      collection, "/", 
      variable_code, "/",
      paste0(model, "-", scenario), "/",
      filename
    )
    
  } else if(collection == "pop-x0.25") {
    collection_id <- paste0("gpw-v4-rev11-", scenario)
    
    filename <- paste0(
      paste(product_type, variable_code, aggregation, "mean", sep="-"),
      "_", collection, "_",
      collection_id, "_",
      product_type, "_",
      "mean", "_",
      time_period, ".nc"
    )
    
    url <- paste0(
      "https://wbg-cckp.s3.amazonaws.com/data/",
      collection, "/",
      variable_code, "/",
      collection_id, "/",
      filename
    )
  } else (message("ERROR: not a valid collection"))
  
  print(url)
  
  local_file <- file.path(here::here("Data", "raw_climate"), filename)
  
  head_response <- httr::HEAD(url)
  expected_size <- httr::headers(head_response)$`content-length`
  
  response <- httr::GET(
      url,
      httr::write_disk(local_file, overwrite = TRUE),
      httr::progress()
    )
  
  downloaded_size <- httr::headers(response)$`content-length`
  actual_size <- file.size(local_file)
  
  if(httr::status_code(response) != 200) {
      stop(paste("Failed to download file:", httr::http_status(response)$message))
    }
  
  if (!is.null(expected_size) && !is.null(downloaded_size)) {
    if (expected_size != downloaded_size) {
      warning("Warning: Downloaded size differs from expected size")
    } else {
      message("File sizes match as expected")
    }
  }
  
  # # Open NetCDF file
  # nc <- nc_open(local_file)
  # 
  # # Get coordinates
  # lon <- ncvar_get(nc, "lon")
  # lat <- ncvar_get(nc, "lat")
  # 
  # # Read data and preserve metadata
  # data <- ncvar_get(nc, variable_code)
  # global_atts <- ncatt_get(nc, 0)
  # var_atts <- ncatt_get(nc, variable_code)
  # 
  # r <- terra::rast(data,
  #                  crs = "epsg:4326",
  #                  extent = c(min(lon), max(lon), min(lat), max(lat)))
  # 
  # # Attach metadata as attributes
  # attr(r, "global_attributes") <- global_atts
  # attr(r, "variable_attributes") <- var_atts
  # attr(r, "source_file") <- filename
  # attr(r, "scenario") <- scenario
  # attr(r, "variable") <- variable_code
  # 
  # # Close connection
  # nc_close(nc)
  # 
  # names(r) <- paste(variable_code, scenario, sep="_")
  # 
  return(local_file)
}

# Function to batch process climate data in parallel from AWS S3 -----
get_climate_data_batch_parallel <-  function(collection,
                                             variables,
                                             scenarios = NULL,
                                             product = "climatology",
                                             aggregation = "annual",
                                             time_period = NULL,
                                             product_type = "climatology",
                                             model = "ensemble-all",
                                             percentile = "median",
                                             statistic = "mean",
                                             chunk_size = 3) {
  
  if(collection == "era5-x0.25") {
    
    scenarios <- "historical_era5"
    time_period <- time_period %||% "1991-2020"
    
  } else {
    
    scenarios <- scenarios %||% c("ssp245", "ssp585")
    time_period <- time_period %||% "2040-2059"
  }
  
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
                   collection = collection,
                   variable_code = jobs$variable[i],
                   scenario = jobs$scenario[i],
                   product = product,
                   aggregation = aggregation,
                   time_period = time_period,
                   product_type = product_type,
                   model = model,
                   percentile = percentile,
                   statistic = statistic
                 )
               }, error = function(e) {
                 message(sprintf("Failed: %s-%s: %s", 
                                 jobs$variable[i], jobs$scenario[i], e$message))
               })
             },
             .options = furrr::furrr_options(seed = TRUE),
             .progress = TRUE
             )
  gc()
  
  # List downloaded files 
  pattern <- if(grepl("era5", collection)) {
    sprintf("^.*era5-x0.25.*\\.nc$")
  } else if(grepl("cmip6", collection)) {
    sprintf("^.*cmip6-x0.25.*\\.nc$") 
  }
  
  nc_files <- list.files(here("Data", "raw_climate"),
                         pattern = pattern,
                         full.names = TRUE)
  
  # Create output file with same dimensions
  outfile <- here::here("Output", paste("climate_data", 
                                        collection,
                                        paste(scenarios, collapse="_"),
                                        product_type, 
                                        time_period,
                                        if(length(variables) == 1) variables[1],
                                        "combined.nc",
                                        sep="_"))
  
  # Create a combined raster
  combined_nc <- c(rast(nc_files))
  
  # Write single file
  terra::writeCDF(combined_nc
                    , outfile
                    , overwrite = TRUE
                    )
  
  # Cleanup
  unlink(nc_files)
  gc()
  
  message("Created output file: ", outfile)
  
  return(combined_nc)
}

# # Write to single NetCDF
# merged_nc <- terra::writeCDF(r, outfile)
# 
# # Get back metadata from original
# nc <- nc_open(nc_files[1])
# r <- terra::rast(nc_files)
# 
# # Read all files as SpatRaster, but looses some metadat attributes
# terra::writeCDF(r, outfile)
# nc_close(nc)

# # Open first file and get variable directly
# template_nc <- nc_open(nc_files[1])
# dims <- template_nc$dim
# 
# # Define variables with dimensions and attributes
# var_defs <- list()
# for(f in nc_files) {
#   src <- nc_open(f)
#   var_name <- names(src$var)[1]
#   var_defs[[var_name]] <- ncvar_def(var_name, 
#                                     src$var[[1]]$units,
#                                     dims)
#   nc_close(src)
# }
# 
# nc_close(template_nc)
# 
# # Add global attributes
# londim <- ncdim_def("lon", "degrees_east", lon) 
# latdim <- ncdim_def("lat", "degrees_north", lat)
# timedim <- ncdim_def("time", tunits, time, unlim = TRUE)
# 
# # Explicitly place unlimited dimension last
# var_defs <- lapply(nc_files, function(f) {
#   src <- nc_open(f)
#   var_name <- names(src$var)[1]
#   var_def <- ncvar_def(var_name, 
#                        src$var[[1]]$units,
#                        list(timedim,londim, latdim))  # Consistent order
#   nc_close(src)
#   return(var_def)
# })  
# 
# # Create file
# merged_nc <- nc_create(outfile, var_defs)
# 
# # Copy attributes
# for(f in nc_files) {
#   src <- nc_open(f)
#   
#   var_name <- names(src$var)[1]
#   ncvar_put(merged_nc, var_name, ncvar_get(src))
#   var_atts <- ncatt_get(src, var_name)
#   
#   for(att in names(var_atts)) {
#     ncatt_put(merged_nc, var_name, att, var_atts[[att]])
#   }
#   
#   nc_close(src)
# }
# 
# ncatt_put(merged_nc, 0, "collection", collection)
# ncatt_put(merged_nc, 0, "source", if(collection == "era5-x0.25") "ERA5" else "CMIP6")
# ncatt_put(merged_nc, 0, "date_created", as.character(Sys.Date()))


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
