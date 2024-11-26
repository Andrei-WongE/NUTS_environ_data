## ---------------------------
##
## Script name: 
##
## Project:
##
## Purpose of script: 
##
## Author: Andrei Wong Espejo
##
## Date Created: 2024-11-18
##
## Email: awonge01@student.bbk.ac.uk
##
## ---------------------------
##
## Notes: 
##   
##
## ---------------------------

## Program Set-up ------------
source("Master_script.R")

dir.create(here("Data", "ghs"), recursive = TRUE)


## Runs the following --------
# 1. Downloads the data via URL
# 2. Performs data checks
# 3. Exports data in CSV
# 4. 


## Downloading and Uploading data ----
require(httr)
require(sf)
require(tidyverse)
require(terra)
require(tidyterra)
require(ggplot2)
require(ggspatial)

# Data form https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/
# https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GLOBE_R2023A/

output_dir <- here("Data", "ghs")
zip_path <- file.path(output_dir, "ucdb_data.zip")
# 
# url <- "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_UCDB_GLOBE_R2024A/GHS_UCDB_REGION_GLOBE_R2024A/GHS_UCDB_REGION_EUROPE_R2024A/V1-0/"
# file_name <- "GHS_UCDB_REGION_EUROPE_R2024A_V1_0.zip"

url <- "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GLOBE_R2023A/GHS_POP_E2030_GLOBE_R2023A_54009_1000/V1-0/"
file_name <- "GHS_POP_E2030_GLOBE_R2023A_54009_1000_V1_0.zip"

url <- paste0(url, file_name)
zip_path <- file.path(output_dir, file_name)

message("Downloading ", file_name, "...")
GET(url,
    write_disk(zip_path, overwrite = TRUE),
    config(ssl_verifypeer = FALSE))

# Extract files and clean up
unzip(zip_path, exdir = output_dir)
file.remove(zip_path)

# 1. List the layers in UCDB----
(layers <- st_layers(here("Data", "ghs", "GHS_UCDB_REGION_EUROPE_R2024A.gpkg")))
# Driver: GPKG 
# Available layers:
#   layer_name geometry_type features fields        crs_name
# 1  GHSL_UCDB_THEME_GENERAL_CHARACTERISTICS_GLOBE_R2024A Multi Polygon     1165     13 World_Mollweide
# 2                     GHSL_UCDB_THEME_GHSL_GLOBE_R2024A Multi Polygon     1165    551 World_Mollweide
# 3                GHSL_UCDB_THEME_EMISSIONS_GLOBE_R2024A Multi Polygon     1165    397 World_Mollweide
# 4                 GHSL_UCDB_THEME_EXPOSURE_GLOBE_R2024A Multi Polygon     1165   1066 World_Mollweide
# 5                GHSL_UCDB_THEME_GEOGRAPHY_GLOBE_R2024A Multi Polygon     1165     37 World_Mollweide
# 6                GHSL_UCDB_THEME_GREENNESS_GLOBE_R2024A Multi Polygon     1165     63 World_Mollweide
# 7          GHSL_UCDB_THEME_INFRASTRUCTURES_GLOBE_R2024A Multi Polygon     1165     17 World_Mollweide
# 8                     GHSL_UCDB_THEME_LULC_GLOBE_R2024A Multi Polygon     1165     49 World_Mollweide
# 9          GHSL_UCDB_THEME_NATURAL_SYSTEMS_GLOBE_R2024A Multi Polygon     1165     23 World_Mollweide
# 10                     GHSL_UCDB_THEME_SDG_GLOBE_R2024A Multi Polygon     1165     43 World_Mollweide
# 11           GHSL_UCDB_THEME_SOCIOECONOMIC_GLOBE_R2024A Multi Polygon     1165    176 World_Mollweide
# 12                   GHSL_UCDB_THEME_WATER_GLOBE_R2024A Multi Polygon     1165     28 World_Mollweide
# 13                 GHSL_UCDB_THEME_CLIMATE_GLOBE_R2024A Multi Polygon     1165    103 World_Mollweide
# 14             GHSL_UCDB_THEME_HAZARD_RISK_GLOBE_R2024A Multi Polygon     1165    118 World_Mollweide
# 15                  GHSL_UCDB_THEME_HEALTH_GLOBE_R2024A Multi Polygon     1165     17 World_Mollweide
# 16                                         UC_centroids         Point    11422      3 World_Mollweide

# Read the data
ghs_data_eu <- st_read(here("Data", "ghs", "GHS_UCDB_REGION_EUROPE_R2024A.gpkg")
                       , layer = "GHSL_UCDB_THEME_GHSL_GLOBE_R2024A")

ghs_data_general <- st_read(here("Data", "ghs", "GHS_UCDB_REGION_EUROPE_R2024A.gpkg")
                       , layer = "GHSL_UCDB_THEME_GENERAL_CHARACTERISTICS_GLOBE_R2024A")

# Examine the structure and temporal coverage of zonal population data

(str_summary <- ghs_data_eu %>% 
               sf::st_drop_geometry() %>%  
               dplyr::select(starts_with("GH_POP_TOT_")) %>% 
               colnames() %>% 
               str_extract_all("\\d+") %>%
               unlist()
)

# [1] "1975" "1980" "1985" "1990" "1995" "2000" "2005" "2010" "2015" "2020" "2025" "2030"

(str_summary <- ghs_data_general %>% 
    sf::st_drop_geometry() %>%  
    dplyr::select(starts_with("GC_CNT_GAD_")) %>% 
    colnames() %>% 
    str_extract_all("\\d+") %>%
    unlist()
)

# Basic spatial information
(st_crs(ghs_data_eu))
(paste("Number of features:", nrow(ghs_data_eu)))

# Summary of key attributes
summary_stats <- ghs_data %>%
  st_drop_geometry() %>%
  select(all_of(pop_cols)) %>%
  summary()

print("\nPopulation statistics:")
print(summary_stats)

# Check for any missing values in population columns
missing_values <- ghs_data %>%
  st_drop_geometry() %>%
  select(all_of(pop_cols)) %>%
  summarise(across(everything(), ~sum(is.na(.))))

print("\nMissing values per population column:")
print(missing_values)




# 2. Check population raster ----
require(httr)
require(terra)
require(tidyverse)
require(here)
require(furrr)
require(tictoc)
require(tidyterra)
require(viridis)
require(ggspatial)


## Download all years, processs and store [With aide of Claude]-----
dir.create(here("Data", "ghs", "raw"), recursive = TRUE, showWarnings = FALSE)
dir.create(here("Data", "ghs", "plots"), recursive = TRUE, showWarnings = FALSE)

# Setup parallel processing
n_cores <- availableCores() - 1
plan(multisession, workers = n_cores)

# Setting up main parameters
BASE_URL <- "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GLOBE_R2023A/"
YEARS <- c(1975, 1980, 1985, 1990, 1995, 2000, 2005, 2010, 2015, 2020, 2025, 2030)

# Define Europe extent
europe_proj <- vect(cbind(c(-11,32,32,-11), c(35,35,65,65)), 
                    type = "polygons", 
                    crs = "EPSG:4326")

# Functions
download_year <- function(year) {
  file_name <- sprintf("GHS_POP_E%d_GLOBE_R2023A_54009_1000_V1_0.zip", year)
  url <- file.path(BASE_URL, 
                   sprintf("GHS_POP_E%d_GLOBE_R2023A_54009_1000/V1-0", year), 
                   file_name)
  zip_path <- here("Data", "ghs", "raw", file_name)

  tif_path <- here("Data", "ghs", "raw", 
                   sprintf("GHS_POP_E%d_GLOBE_R2023A_54009_1000_V1_0.tif", year))
  if(file.exists(tif_path)) {
    message(sprintf("File for year %d already exists, skipping download.", year))
    return(TRUE)
  }
  
  tryCatch({
    message(sprintf("Downloading year %d...", year))
    GET(url,
        write_disk(zip_path, overwrite = TRUE),
        config(ssl_verifypeer = FALSE))
    
    # Extract and clean up
    unzip(zip_path, exdir = here("Data", "ghs", "raw"))
    file.remove(zip_path)
    return(TRUE)
  }, error = function(e) {
    message(sprintf("Error downloading year %d: %s", year, e$message))
    return(FALSE)
  })
}

# Main Execution
tic.clearlog()
tic("Complete Processing")

# 1. Parallel download
message("\nStarting downloads...")
tic("Downloads")
download_results <- future_map(YEARS, download_year, 
                               .progress = TRUE,
                               .options = furrr_options(
                                 seed = TRUE,
                                 packages = c("httr", "here")
                               ))
toc(log = TRUE)

# Switch to sequential for terra operations
plan(sequential)

# Check downloads
failed_downloads <- YEARS[!unlist(download_results)]
if (length(failed_downloads) > 0) {
  warning(sprintf("Failed downloads for years: %s", 
                  paste(failed_downloads, collapse = ", ")))
}

successful_years <- YEARS[unlist(download_results)]

# 2. Process sequentially 
message("\nStarting processing phase...")
tic("Raster Processing")

# Pre-compute projections
message("Pre-computing projections...")
europe_proj_3035 <- project(europe_proj, "EPSG:3035")
target_rast <- rast(crs = "EPSG:3035",
                    resolution = c(1000, 1000),
                    extent = ext(europe_proj_3035))

process_files <- function(years, batch_size = 5) {
  n_batches <- ceiling(length(years) / batch_size)
  results <- vector("list", length(years))
  names(results) <- years
  
  for(i in 1:n_batches) {
    tic(sprintf("Batch %d/%d", i, n_batches))
    
    batch_start <- ((i-1) * batch_size) + 1
    batch_end <- min(i * batch_size, length(years))
    batch_years <- years[batch_start:batch_end]
    
    message(sprintf("\nProcessing years %s", 
                    paste(batch_years, collapse=", ")))
    
    batch_results <- map(batch_years, function(year) {
      tic(sprintf("Year %d", year))
      
      input_path <- here("Data", "ghs", "raw", 
                         sprintf("GHS_POP_E%d_GLOBE_R2023A_54009_1000_V1_0.tif", year))
      output_path <- here("Data", "ghs",
                          sprintf("pop_data_eu_%d.tif", year))
      
      tryCatch({
        pop_data <- rast(input_path) %>%
          crop(project(europe_proj_3035, crs(.)))
        
        pop_data <- terra::project(pop_data, target_rast, 
                                   method="bilinear", 
                                   threads=TRUE)
        
        if(!all(is.na(values(pop_data)))) {
          writeRaster(pop_data, output_path, 
                      overwrite = TRUE, datatype = "FLT4S")
          toc(log = TRUE)
          return(pop_data)
        }
        message(sprintf("Warning: Empty raster for year %d", year))
        return(NULL)
      }, error = function(e) {
        message(sprintf("Error processing year %d: %s", year, e$message))
        return(NULL)
      })
    })
    
    results[batch_start:batch_end] <- batch_results
    toc(log = TRUE)
    gc(verbose = FALSE)
  }
  return(results)
}

# Execute processing
pop_data_list <- process_files(successful_years)
toc(log = TRUE)

# Print timing summary
message("\nTiming Summary:")
tic.log(format = TRUE) %>%
  print()



## Examine structure and resolution------

analyze_population_data <- function(years) {
  
  # Parallel data loading
  message("Loading files...")
  pop_data_list <- future_map(available_years, 
                              function(year) {
                                pop_file <- here("Data"
                                                 , "ghs"
                                                 , sprintf("pop_data_eu_%d.tif"
                                                           , year))
                                
                                # Convert to dataframe
                                pop_df <- rast(pop_file) %>%
                                  as.data.frame(xy=TRUE)
                                names(pop_df)[3] <- "population"
                                pop_df$year <- as.factor(year)
                                
                                return(pop_df)
                              },
                              .progress = TRUE,
                              .options = furrr_options(
                                seed = TRUE,
                                packages = c("terra", "here")
                              ))
  
  # Combine for density plot
  message("Creating all years density plots...")
  all_years_df <- bind_rows(pop_data_list)
  
  # Density plot
  p_density <- ggplot(data = all_years_df, 
                      aes(x = population, fill = year)) +
    geom_density(alpha = 0.4) +
    scale_x_log10(limits = c(1, NA)) +  # Avoid crack when zero/negative values
    scale_fill_viridis_d() +
    labs(title = "Population Distribution by Year",
         x = "Population (log scale)",
         y = "Density",
         fill = "Year") +
    theme_minimal()
  
  ggsave(here("Data", "ghs", "plots", "density_plot_all_years.png"),
         plot = p_density,
         width = 12,
         height = 8,
         dpi = 600)
  
  # Spatial plots
  message("Creating spatial plots...")
  
  for(year in years) {
    message(sprintf("Processing year %d", year))
    pop_data <- rast(here("Data", "ghs", 
                          sprintf("pop_data_eu_%d.tif", year)))
    
    p2 <- ggplot() +
      geom_spatraster(data = pop_data) + # 500,897 cells sample, maxcell
      scale_fill_viridis_c(
        name = "Population",
        na.value = NA,
        trans = "log1p"
      ) +
      coord_sf(crs = st_crs(3035),  
               expand = FALSE) +     # Plot extends exactly to data boundaries
      annotation_scale(location = "br", style = "ticks") +
      annotation_north_arrow(location = "tl", 
                             style = north_arrow_minimal()) +
      labs(title = sprintf("Projected Population %d", year)) +
      theme_minimal()
    
    ggsave(here("Data", "ghs", "plots", 
                sprintf("spatial_plot_%d.png", year)),
           plot = p2,
           width = 12,
           height = 8,
           dpi = 600)
  }
}

available_years <- list.files(here("Data", "ghs"), 
                               pattern = "pop_data_eu_.*\\.tif$") %>%
  str_extract("\\d{4}") %>%
  as.numeric()

analyze_population_data(available_years)


# 3. Aggregates population grids----
# With a lot of aide of Claude, for messaging system, parallelizing and debugging
# Following functions does 
# Validation Checks:
  # CRS consistency
  # Resolution and extent checks
  # Population totals and density
  # Edge effects
  # UCDB cross-validation when available
# 
# Outputs:
  # Validation summary (CSV)
  # Trend plots (in "plots" folder)
  # Metadata (RDS)
  # Aggregated rasters (if needed)

require(terra)
require(sf)
require(tidyverse)
require(future)
require(future.apply)
require(here)
require(tictoc)
require(data.table)
require(pryr)

#' Print messages with timestamp
#' @param msg Message text
#' @param type Type of message (info, warning, error, success symbilogy)

log_message <- function(msg, type = "info") {
  timestamp <- format(Sys.time(), "%H:%M:%S")
  prefix <- switch(type,
                   info = "===",
                   warning = "!!!",
                   error = "XXX",
                   success = "✓")
  
  cat(sprintf("[%s] %s %s\n", timestamp, prefix, msg))
}

#' Format numbers for display
#' @param x Number to format
#' @param digits Decimal places
format_number <- function(x, digits = 1) {
  scales::label_number(accuracy = 0.1, 
                       scale_cut = scales::cut_short_scale())(x)
}

#' UCDB validation function with proper CRS handling
#' @param r Raster to validate
#' @param year Year to check
#' @param ucdb UCDB data
validate_with_ucdb <- function(r, year, ucdb) {
  if(is.null(ucdb)) return(NULL)
  
  pop_col <- paste0("GH_POP_TOT_", year)
  
  if(!(pop_col %in% names(ucdb))) {
    log_message(sprintf("No UCDB population data for %s", year), "warning")
    return(NULL)
  }
  
  tryCatch({
    # Get CRS of input raster
    r_crs <- st_crs(crs(r))
    
    # Transform UCDB from Mollweide to LAEA
    log_message("Transforming UCDB from Mollweide to LAEA", "info")
    ucdb_projected <- st_transform(ucdb, r_crs)
    
    # Get population totals
    ucdb_total <- sum(ucdb_projected[[pop_col]], na.rm = TRUE)
    raster_total <- global(r, "sum", na.rm = TRUE)[[1]]
    
    diff_pct <- (raster_total - ucdb_total) / ucdb_total * 100
    
    log_message(sprintf("UCDB stats for %s:", year), "info")
    log_message(sprintf("  - UCDB total: %s", format_number(ucdb_total)), "info")
    log_message(sprintf("  - Raster total: %s", format_number(raster_total)), "info")
    log_message(sprintf("  - Difference: %.1f%%", diff_pct),
                if(abs(diff_pct) > 3) "warning" else "info")
    
    return(list(
      ucdb_total = ucdb_total,
      raster_total = raster_total,
      difference_pct = diff_pct
    ))
  }, error = function(e) {
    log_message(sprintf("UCDB validation failed: %s", e$message), "warning")
    return(NULL)
  })
}

#' Validate single GHS raster
#' @param rast_path Path to raster file
#' @param year Year of the data
#' @param ucdb UCDB data
validate_ghs_file <- function(rast_path, year, ucdb = NULL) {
  log_message(sprintf("\nProcessing year %s", year), "info")
  
  # Read raster
  r <- rast(rast_path)
  
  # Basic checks
  basic_checks <- list(
    year = year,
    crs = crs(r) == "EPSG:3035",
    dims = dim(r),
    resolution = res(r),
    extent = ext(r),
    has_values = !all(is.na(values(r))),
    total_pop = global(r, "sum", na.rm=TRUE)[[1]]
  )
  
  log_message(sprintf("Population total: %s", 
                      format_number(basic_checks$total_pop)), "info")
  
  # Edge effects check with proper CRS handling
  edge_stats <- tryCatch({
    r_poly <- as.polygons(ext(r), crs=crs(r))
    edge_mask <- buffer(r_poly, width=-res(r)[1])
    
    masked_r <- mask(r, edge_mask)
    stats <- list(
      edge_cells = global(is.na(masked_r), "sum", na.rm=TRUE)[[1]],
      edge_pop = global(masked_r, "sum", na.rm=TRUE)[[1]],
      total_cells = ncell(r)
    )
    
    stats$edge_pct <- (stats$edge_cells / stats$total_cells) * 100
    log_message(sprintf("Edge cells: %.1f%% of total", stats$edge_pct), 
                if(stats$edge_pct > 5) "warning" else "info")
    
    stats
  }, error = function(e) {
    log_message(sprintf("Edge effect calculation failed: %s", e$message), "warning")
    list(
      edge_cells = NA,
      edge_pop = NA,
      total_cells = ncell(r),
      edge_pct = NA
    )
  })
  
  # UCDB validation
  ucdb_check <- validate_with_ucdb(r, year, ucdb)
  
  return(list(
    basic_checks = basic_checks,
    edge_stats = edge_stats,
    ucdb_check = ucdb_check
  ))
}

#' Main processing function
#' @param input_dir Directory containing GHS files
#' @param target_res Target resolution in meters
#' @param n_cores Number of cores for parallel processing
process_ghs_timeseries <- function(input_dir = here("Data", "ghs"), 
                                   target_res = 25000,
                                   n_cores = availableCores() - 1) {
  
  total_time <- Sys.time()
  initial_mem <- mem_used()
  
  log_message("=== GHS Processing Pipeline Started ===", "info")
  
  # List files and extract years
  ghs_files <- list.files(input_dir, 
                          pattern = "pop_data_eu_\\d{4}\\.tif$",
                          full.names = TRUE)
  
  years <- str_extract(basename(ghs_files), "\\d{4}") %>% as.numeric()
  
  if(length(ghs_files) == 0) {
    stop("No GHS files found in input directory")
  }
  
  log_message(sprintf("Found %d files to process (%d-%d)", 
                      length(ghs_files), min(years), max(years)), "info")
  
  # Load UCDB with correct layer and CRS handling
  log_message("Loading UCDB data for cross-validation...", "info")
  ucdb_path <- here("Data", "ghs", "GHS_UCDB_REGION_EUROPE_R2024A.gpkg")
  ucdb <- if(file.exists(ucdb_path)) {
    # Explicitly specify the correct layer
    ucdb_data <- st_read(ucdb_path, 
                         layer = "GHSL_UCDB_THEME_GHSL_GLOBE_R2024A", 
                         quiet = TRUE)
    
    # Verify CRS
    if(st_crs(ucdb_data)$input != "World_Mollweide") {
      log_message("Warning: Unexpected CRS in UCDB data", "warning")
    }
    
    # Verify population columns exist
    pop_cols <- paste0("GH_POP_TOT_", years)
    missing_years <- years[!pop_cols %in% names(ucdb_data)]
    
    if(length(missing_years) > 0) {
      log_message(sprintf("Warning: Missing UCDB data for years: %s",
                          paste(missing_years, collapse=", ")), "warning")
    }
    
    log_message(sprintf("UCDB data loaded with %d features", nrow(ucdb_data)), "info")
    log_message("UCDB CRS: World_Mollweide", "info")
    ucdb_data
  } else {
    log_message("UCDB file not found", "warning")
    NULL
  }
  
  # Setup parallel processing
  plan(multisession, workers = n_cores)
  log_message(sprintf("Using %d cores for processing", n_cores), "info")
  
  # Process files
  validation_results <- future_map(seq_along(ghs_files), function(i) {
    validate_ghs_file(ghs_files[i], years[i], ucdb)
  }, .progress = TRUE)
  
  # Compile results
  validation_summary <- data.table(
    year = years,
    total_pop = sapply(validation_results, function(x) x$basic_checks$total_pop),
    mean_density = sapply(validation_results, function(x) 
      x$basic_checks$total_pop / prod(x$basic_checks$dims[1:2])),
    edge_cells_pct = sapply(validation_results, function(x) x$edge_stats$edge_pct),
    ucdb_diff_pct = sapply(validation_results, function(x) 
      if(!is.null(x$ucdb_check)) x$ucdb_check$difference_pct else NA)
  )
  
  # Final Summary Report
  cat("\n=== Validation Summary ===\n")
  
  # Population trend analysis
  pop_growth <- diff(validation_summary$total_pop)/validation_summary$total_pop[-1]*100
  high_growth_years <- years[-1][abs(pop_growth) > 5]
  
  if(length(high_growth_years) == 0) {
    log_message("Population trends: Consistent growth pattern", "success")
  } else {
    log_message(sprintf("Population trends: Anomalies in years: %s", 
                        paste(high_growth_years, collapse=", ")), "warning")
  }
  
  # Edge effects summary
  mean_edge <- mean(validation_summary$edge_cells_pct, na.rm = TRUE)
  if(mean_edge < 3) {
    log_message("Edge effects: Within acceptable range", "success")
  } else {
    log_message(sprintf("Edge effects: High average (%.1f%%)", mean_edge), "warning")
  }
  
  # UCDB comparison summary
  ucdb_issues <- validation_summary[abs(ucdb_diff_pct) > 3, year]
  if(length(ucdb_issues) == 0) {
    log_message("UCDB validation: All comparisons within ±3%", "success")
  } else {
    log_message(sprintf("UCDB validation: Large differences in years: %s",
                        paste(ucdb_issues, collapse=", ")), "warning")
  }
  
  # Generate plots if directory exists
  plots_dir <- file.path(input_dir, "plots")
  if(dir.exists(plots_dir)) {
    if(requireNamespace("ggplot2", quietly = TRUE)) {
      # Population trend plot
      p1 <- ggplot(validation_summary, aes(x = year, y = total_pop/1e6)) +
        geom_line() +
        geom_point() +
        labs(title = "Total Population Over Time",
             x = "Year",
             y = "Population (millions)") +
        theme_minimal()
      
      ggsave(file.path(plots_dir, "population_trend.png"), 
             p1, width = 10, height = 6)
      
      # UCDB comparison plot
      p2 <- ggplot(validation_summary, aes(x = year, y = ucdb_diff_pct)) +
        geom_line() +
        geom_point() +
        geom_hline(yintercept = c(-3, 3), linetype = "dashed", color = "red") +
        labs(title = "UCDB Comparison",
             x = "Year",
             y = "Difference (%)") +
        theme_minimal()
      
      ggsave(file.path(plots_dir, "ucdb_comparison.png"), 
             p2, width = 10, height = 6)
    }
  }
  
  # Save outputs
  fwrite(validation_summary, file.path(input_dir, "validation_summary.csv"))
  
  # Save detailed metadata
  metadata <- list(
    processing_date = Sys.time(),
    input_files = ghs_files,
    years_processed = years,
    resolution = target_res,
    crs = "EPSG:3035",
    ucdb_comparison = !is.null(ucdb),
    validation_summary = validation_summary,
    processing_time = difftime(Sys.time(), total_time, units = "secs"),
    memory_used = mem_used() - initial_mem
  )
  
  saveRDS(metadata, file.path(input_dir, "processing_metadata.rds"))
  
  # Final timing and memory usage
  total_time <- difftime(Sys.time(), total_time, units = "secs")
  final_mem <- mem_used()
  mem_used <- final_mem - initial_mem
  
  log_message(sprintf("\nProcessing completed in %.1f seconds", total_time), "info")
  log_message(sprintf("Peak memory usage: %.1f GB", mem_used/1e9), "info")
  
  return(list(
    validation = validation_summary,
    processing_time = total_time,
    memory_used = mem_used,
    metadata = metadata
  ))
}

results <- process_ghs_timeseries( input_dir = here("Data", "ghs")     
                                  , target_res = 25000             
                                  , n_cores = 12                 
                                  )

results$validation        # Table with validation metrics

validation <- fread(here("Data", "ghs", "validation_summary.csv"))

(metadata <- readRDS(here("Data", "ghs", "processing_metadata.rds")))



