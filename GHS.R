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
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

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

results_eupop <- readRDS(here("Data", "Eurostat", "results_eupop.rds"))

require(terra)
require(sf)
require(tidyverse)
require(future)
require(future.apply)
require(here)
require(tictoc)
require(data.table)
require(pryr)

#' Print message with timestamp
log_message <- function(msg, type = "info") {
  timestamp <- format(Sys.time(), "%H:%M:%S")
  prefix <- switch(type,
                   info = "===",
                   warning = "!!!", 
                   error = "XXX",
                   success = "✓")
  
  cat(sprintf("[%s] %s %s\n", timestamp, prefix, msg))
}

#' Format numbers with commas
format_pop <- function(x) {
  format(x, big.mark = ",", scientific = FALSE, trim = TRUE)
}

#' Aggregate raster to target resolution
#' @param r Input raster
#' @param target_res Target resolution in meters
#' @return List with aggregated raster and aggregation stats

aggregate_ghs <- function(r, target_res) {
  current_res <- res(r)[1]
  
  if(target_res <= current_res) {
    log_message("Target resolution is smaller or equal to current resolution", "warning")
    return(list(
      raster = r,
      aggregated = FALSE,
      orig_total = global(r, "sum", na.rm=TRUE)[[1]],
      agg_total = global(r, "sum", na.rm=TRUE)[[1]],
      agg_factor = 1
    ))
  }
  
  # Calculate aggregation factor
  agg_factor <- ceiling(target_res / current_res)
  log_message(sprintf("Aggregating from %g to %g meters (factor: %d)", 
                      current_res, target_res, agg_factor), "info")
  
  # Store original total
  orig_total <- global(r, "sum", na.rm=TRUE)[[1]]
  
  # Perform aggregation
  r_agg <- aggregate(r, fact=agg_factor, fun="sum", na.rm=TRUE)
  
  # Check population preservation
  agg_total <- global(r_agg, "sum", na.rm=TRUE)[[1]]
  diff_pct <- (agg_total - orig_total) / orig_total * 100
  
  if(abs(diff_pct) > 0.01) {
    log_message(sprintf("Warning: Aggregation changed total population by %.4f%%", 
                        diff_pct), "warning")
  } else {
    log_message("Population preserved after aggregation", "success")
  }
  
  return(list(
    raster = r_agg,
    aggregated = TRUE,
    orig_total = orig_total,
    agg_total = agg_total,
    agg_factor = agg_factor
  ))
}

#' Validate GHS raster against Eurostat data
#' @param r Raster to validate
#' @param year Year to check
#' @param eupop Eurostat population data

validate_with_eurostat <- function(r, year, eupop) {
  if(is.null(eupop)) return(NULL)
  
  # Get Eurostat population for this year
  eu_data <- eupop$totals %>%
    filter(time == year)
  
  if(nrow(eu_data) == 0) {
    log_message(sprintf("No Eurostat data for %s", year), "warning")
    return(NULL)
  }
  
  # Calculate GHS total
  raster_total <- global(r, "sum", na.rm=TRUE)[[1]]
  
  # Get Eurostat total and coverage
  eu_total <- eu_data$total_pop
  countries_covered <- eu_data$countries_with_data
  countries_missing <- eu_data$countries_missing
  
  # Calculate difference
  diff_abs <- raster_total - eu_total
  diff_pct <- (diff_abs / eu_total) * 100
  
  log_message(sprintf("Eurostat comparison for %s:", year), "info")
  log_message(sprintf("  - GHS total: %s", format_pop(raster_total)), "info")
  log_message(sprintf("  - Eurostat total: %s", format_pop(eu_total)), "info")
  log_message(sprintf("  - Absolute difference: %s", format_pop(diff_abs)), "info")
  log_message(sprintf("  - Relative difference: %.2f%%", diff_pct),
              if(abs(diff_pct) > 10) "warning" else "info")
  log_message(sprintf("  - Eurostat coverage: %d countries (missing: %d)", 
                      countries_covered, countries_missing), "info")
  
  return(list(
    year = year,
    ghs_total = raster_total,
    eurostat_total = eu_total,
    diff_abs = diff_abs,
    diff_pct = diff_pct,
    countries_covered = countries_covered,
    countries_missing = countries_missing
  ))
}

#' Validate and process single GHS raster
#' @param rast_path Path to raster file
#' @param year Year of the data
#' @param eupop Eurostat population data
#' @param target_res Target resolution in meters
#' @param output_dir Directory to save processed files

validate_ghs_file <- function(rast_path, year, eupop = NULL, 
                              target_res = 25000, output_dir = NULL) {
  log_message(sprintf("\nProcessing year %s", year), "info")
  
  # Read raster
  r <- rast(rast_path)
  
  # Perform aggregation
  agg_result <- aggregate_ghs(r, target_res)
  r_final <- agg_result$raster
  
  # Save aggregated raster if output directory is provided
  if(!is.null(output_dir)) {
    output_file <- file.path(output_dir, 
                             sprintf("pop_data_eu_%d_%dkm.tif", 
                                     year, 
                                     target_res/1000))
    log_message(sprintf("Saving aggregated raster to: %s", output_file), "info")
    writeRaster(r_final, output_file, overwrite=TRUE)
  }
  
  # Basic checks on aggregated raster
  basic_checks <- list(
    year = year,
    crs = crs(r_final) == "EPSG:3035",
    dims = dim(r_final),
    resolution = res(r_final),
    extent = ext(r_final),
    has_values = !all(is.na(values(r_final))),
    total_pop = agg_result$agg_total,
    orig_res = res(r)[1],
    agg_factor = agg_result$agg_factor
  )
  
  log_message(sprintf("Aggregated population total: %s", 
                      format_pop(basic_checks$total_pop)), "info")
  
  # Edge effects check
  edge_stats <- tryCatch({
    r_poly <- as.polygons(ext(r_final), crs=crs(r_final))
    edge_mask <- buffer(r_poly, width=-res(r_final)[1])
    
    masked_r <- mask(r_final, edge_mask)
    stats <- list(
      edge_cells = global(is.na(masked_r), "sum", na.rm=TRUE)[[1]],
      edge_pop = global(masked_r, "sum", na.rm=TRUE)[[1]],
      total_cells = ncell(r_final)
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
      total_cells = ncell(r_final),
      edge_pct = NA
    )
  })
  
  # Eurostat validation
  eurostat_check <- validate_with_eurostat(r_final, year, eupop)
  
  return(list(
    basic_checks = basic_checks,
    edge_stats = edge_stats,
    eurostat_check = eurostat_check,
    aggregation = agg_result[c("aggregated", "orig_total", "agg_total", "agg_factor")]
  ))
}

#' Main processing function
#' @param input_dir Directory containing input GHS files
#' @param output_dir Directory to save processed files
#' @param target_res Target resolution in meters
#' @param n_cores Number of cores for parallel processing
#' @param eupop Eurostat population data

process_ghs_timeseries <- function(input_dir = here("Data", "ghs"),
                                   output_dir = here("Output"),
                                   target_res = 25000,
                                   n_cores = availableCores() - 1,
                                   eupop = results_eupop) {
  
  # Start timing
  total_time <- Sys.time()
  initial_mem <- mem_used()
  
  log_message("=== GHS Processing Pipeline Started ===", "info")
  
  # Create output directory if it doesn't exist
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  
  # List files and extract years
  ghs_files <- list.files(input_dir, 
                          pattern = "pop_data_eu_\\d{4}\\.tif$",
                          full.names = TRUE)
  
  years <- str_extract(basename(ghs_files), "\\d{4}") %>% as.numeric()
  
  log_message(sprintf("Found %d files to process (%d-%d)", 
                      length(ghs_files), min(years), max(years)), "info")
  
  # Setup parallel processing
  plan(multisession, workers = n_cores)
  log_message(sprintf("Using %d cores for processing", n_cores), "info")
  
  # Process files
  validation_results <- future_map(seq_along(ghs_files), function(i) {
    validate_ghs_file(ghs_files[i], years[i], eupop, target_res, output_dir)
  }, .progress = TRUE)
  
  # Compile results
  validation_summary <- data.table(
    year = years,
    total_pop = sapply(validation_results, function(x) x$basic_checks$total_pop),
    mean_density = sapply(validation_results, function(x) 
      x$basic_checks$total_pop / prod(x$basic_checks$dims[1:2])),
    edge_cells_pct = sapply(validation_results, function(x) x$edge_stats$edge_pct),
    eurostat_diff_pct = sapply(validation_results, function(x) 
      if(!is.null(x$eurostat_check)) x$eurostat_check$diff_pct else NA)
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
  
  # Eurostat comparison summary
  eurostat_issues <- validation_summary[abs(eurostat_diff_pct) > 10, year]
  if(length(eurostat_issues) == 0) {
    log_message("Eurostat validation: All comparisons within ±10%", "success")
  } else {
    log_message(sprintf("Eurostat validation: Large differences in years: %s",
                        paste(eurostat_issues, collapse=", ")), "warning")
  }
  
  # Save validation summary
  fwrite(validation_summary, 
         file.path(output_dir, "validation_summary.csv"))
  
  # Create detailed comparison table
  comparison_table <- data.table(
    year = years,
    ghs_population = format_pop(validation_summary$total_pop),
    eurostat_population = sapply(validation_results, function(x) 
      if(!is.null(x$eurostat_check)) format_pop(x$eurostat_check$eurostat_total) else NA),
    absolute_difference = sapply(validation_results, function(x) 
      if(!is.null(x$eurostat_check)) format_pop(x$eurostat_check$diff_abs) else NA),
    percent_difference = sapply(validation_results, function(x) 
      if(!is.null(x$eurostat_check)) sprintf("%.2f%%", x$eurostat_check$diff_pct) else NA),
    countries_covered = sapply(validation_results, function(x) 
      if(!is.null(x$eurostat_check)) x$eurostat_check$countries_covered else NA)
  )
  
  fwrite(comparison_table,
         file.path(output_dir, "eurostat_comparison.csv"))
  
  # Save metadata
  metadata <- list(
    processing_date = Sys.time(),
    input_files = ghs_files,
    output_directory = output_dir,
    years_processed = years,
    resolution = list(
      target = target_res,
      original = sapply(validation_results, function(x) x$basic_checks$orig_res)
    ),
    crs = "EPSG:3035",
    eurostat_validation = TRUE,
    validation_summary = validation_summary,
    processing_time = difftime(Sys.time(), total_time, units = "secs"),
    memory_used = mem_used() - initial_mem
  )
  
  saveRDS(metadata, 
          file.path(output_dir, "processing_metadata.rds"))
  
  # Final timing and memory usage
  total_time <- difftime(Sys.time(), total_time, units = "secs")
  final_mem <- mem_used()
  mem_used <- final_mem - initial_mem
  
  log_message(sprintf("\nProcessing completed in %.1f seconds", total_time), "info")
  log_message(sprintf("Peak memory usage: %.1f GB", mem_used/1e9), "info")
  log_message(sprintf("Output files saved in: %s", normalizePath(output_dir)), "info")
  
  # Print comparison table
  message("\nDetailed Population Comparison:")
  print(comparison_table)
  
  return(list(
    validation = validation_summary,
    comparison = comparison_table,
    processing_time = total_time,
    memory_used = mem_used,
    metadata = metadata
  ))
}

results <- process_ghs_timeseries( input_dir = here("Data", "ghs") 
                                  , output_dir = here("Output")
                                  , target_res = 25000             
                                  , n_cores = 12
                                  , eupop = results_eupop # Pop dataset fro validation
                                  )

results$validation        # Table with validation metrics

validation <- fread(here("Data", "ghs", "validation_summary.csv"))

(metadata <- readRDS(here("Data", "ghs", "processing_metadata.rds")))

# [21:37:43] === === GHS Processing Pipeline Started ===
#   [21:37:43] === Found 12 files to process (1975-2030)
# [21:37:59] === Using 12 cores for processing
# [21:38:04] === 
#   Processing year 1975
# [21:38:04] === Aggregating from 1000 to 25000 meters (factor: 25)
# [21:38:05] ✓ Population preserved after aggregation
# [21:38:05] === Saving aggregated raster to: C:/Users/Andre/OneDrive - Birkbeck, University of London/R/R_projects/NUTS_environ_data/Output/pop_data_eu_1975_25km.tif
# [21:38:05] === Aggregated population total: 586,708,070
# [21:38:05] !!! Edge cells: 40.5% of total
# [21:38:05] === Eurostat comparison for 1975:
#   [21:38:05] ===   - GHS total: 586,708,070
# [21:38:05] ===   - Eurostat total: 493,370,098
# [21:38:05] ===   - Absolute difference: 93,337,972
# [21:38:05] !!!   - Relative difference: 18.92%
# [21:38:05] ===   - Eurostat coverage: 36 countries (missing: 13)
# [21:38:10] === 
#   Processing year 1980
# [21:38:10] === Aggregating from 1000 to 25000 meters (factor: 25)
# [21:38:11] ✓ Population preserved after aggregation
# [21:38:11] === Saving aggregated raster to: C:/Users/Andre/OneDrive - Birkbeck, University of London/R/R_projects/NUTS_environ_data/Output/pop_data_eu_1980_25km.tif
# [21:38:11] === Aggregated population total: 602,921,649
# [21:38:11] !!! Edge cells: 40.5% of total
# [21:38:11] === Eurostat comparison for 1980:
#   [21:38:11] ===   - GHS total: 602,921,649
# [21:38:11] ===   - Eurostat total: 508,328,028
# [21:38:11] ===   - Absolute difference: 94,593,621
# [21:38:11] !!!   - Relative difference: 18.61%
# [21:38:11] ===   - Eurostat coverage: 36 countries (missing: 13)
# [21:38:15] === 
#   Processing year 1985
# [21:38:15] === Aggregating from 1000 to 25000 meters (factor: 25)
# [21:38:16] ✓ Population preserved after aggregation
# [21:38:16] === Saving aggregated raster to: C:/Users/Andre/OneDrive - Birkbeck, University of London/R/R_projects/NUTS_environ_data/Output/pop_data_eu_1985_25km.tif
# [21:38:16] === Aggregated population total: 616,777,619
# [21:38:16] !!! Edge cells: 40.5% of total
# [21:38:16] === Eurostat comparison for 1985:
#   [21:38:16] ===   - GHS total: 616,777,619
# [21:38:16] ===   - Eurostat total: 521,495,920
# [21:38:16] ===   - Absolute difference: 95,281,699
# [21:38:16] !!!   - Relative difference: 18.27%
# [21:38:16] ===   - Eurostat coverage: 36 countries (missing: 13)
# [21:38:20] === 
#   Processing year 1990
# [21:38:20] === Aggregating from 1000 to 25000 meters (factor: 25)
# [21:38:20] ✓ Population preserved after aggregation
# [21:38:20] === Saving aggregated raster to: C:/Users/Andre/OneDrive - Birkbeck, University of London/R/R_projects/NUTS_environ_data/Output/pop_data_eu_1990_25km.tif
# [21:38:20] === Aggregated population total: 630,559,372
# [21:38:20] !!! Edge cells: 40.5% of total
# [21:38:20] === Eurostat comparison for 1990:
#   [21:38:20] ===   - GHS total: 630,559,372
# [21:38:20] ===   - Eurostat total: 535,147,528
# [21:38:20] ===   - Absolute difference: 95,411,844
# [21:38:20] !!!   - Relative difference: 17.83%
# [21:38:20] ===   - Eurostat coverage: 37 countries (missing: 12)
# [21:38:24] === 
#   Processing year 1995
# [21:38:24] === Aggregating from 1000 to 25000 meters (factor: 25)
# [21:38:25] ✓ Population preserved after aggregation
# [21:38:25] === Saving aggregated raster to: C:/Users/Andre/OneDrive - Birkbeck, University of London/R/R_projects/NUTS_environ_data/Output/pop_data_eu_1995_25km.tif
# [21:38:25] === Aggregated population total: 639,322,906
# [21:38:25] !!! Edge cells: 40.5% of total
# [21:38:25] === Eurostat comparison for 1995:
#   [21:38:25] ===   - GHS total: 639,322,906
# [21:38:25] ===   - Eurostat total: 619,358,177
# [21:38:25] ===   - Absolute difference: 19,964,729
# [21:38:25] ===   - Relative difference: 3.22%
# [21:38:25] ===   - Eurostat coverage: 37 countries (missing: 12)
# [21:38:29] === 
#   Processing year 2000
# [21:38:29] === Aggregating from 1000 to 25000 meters (factor: 25)
# [21:38:29] ✓ Population preserved after aggregation
# [21:38:29] === Saving aggregated raster to: C:/Users/Andre/OneDrive - Birkbeck, University of London/R/R_projects/NUTS_environ_data/Output/pop_data_eu_2000_25km.tif
# [21:38:30] === Aggregated population total: 644,392,575
# [21:38:30] !!! Edge cells: 40.5% of total
# [21:38:30] === Eurostat comparison for 2000:
#   [21:38:30] ===   - GHS total: 644,392,575
# [21:38:30] ===   - Eurostat total: 859,322,082
# [21:38:30] ===   - Absolute difference: -214,929,507
# [21:38:30] !!!   - Relative difference: -25.01%
# [21:38:30] ===   - Eurostat coverage: 46 countries (missing: 3)
# [21:38:34] === 
#   Processing year 2005
# [21:38:34] === Aggregating from 1000 to 25000 meters (factor: 25)
# [21:38:34] ✓ Population preserved after aggregation
# [21:38:34] === Saving aggregated raster to: C:/Users/Andre/OneDrive - Birkbeck, University of London/R/R_projects/NUTS_environ_data/Output/pop_data_eu_2005_25km.tif
# [21:38:34] === Aggregated population total: 653,095,059
# [21:38:34] !!! Edge cells: 40.5% of total
# [21:38:34] === Eurostat comparison for 2005:
#   [21:38:34] ===   - GHS total: 653,095,059
# [21:38:34] ===   - Eurostat total: 866,751,019
# [21:38:34] ===   - Absolute difference: -213,655,960
# [21:38:34] !!!   - Relative difference: -24.65%
# [21:38:34] ===   - Eurostat coverage: 49 countries (missing: 0)
# [21:38:38] === 
#   Processing year 2010
# [21:38:38] === Aggregating from 1000 to 25000 meters (factor: 25)
# [21:38:39] ✓ Population preserved after aggregation
# [21:38:39] === Saving aggregated raster to: C:/Users/Andre/OneDrive - Birkbeck, University of London/R/R_projects/NUTS_environ_data/Output/pop_data_eu_2010_25km.tif
# [21:38:39] === Aggregated population total: 665,326,526
# [21:38:39] !!! Edge cells: 40.5% of total
# [21:38:39] === Eurostat comparison for 2010:
#   [21:38:39] ===   - GHS total: 665,326,526
# [21:38:39] ===   - Eurostat total: 879,455,023
# [21:38:39] ===   - Absolute difference: -214,128,497
# [21:38:39] !!!   - Relative difference: -24.35%
# [21:38:39] ===   - Eurostat coverage: 47 countries (missing: 2)
# [21:38:43] === 
#   Processing year 2015
# [21:38:43] === Aggregating from 1000 to 25000 meters (factor: 25)
# [21:38:44] ✓ Population preserved after aggregation
# [21:38:44] === Saving aggregated raster to: C:/Users/Andre/OneDrive - Birkbeck, University of London/R/R_projects/NUTS_environ_data/Output/pop_data_eu_2015_25km.tif
# [21:38:44] === Aggregated population total: 675,049,344
# [21:38:44] !!! Edge cells: 40.5% of total
# [21:38:44] === Eurostat comparison for 2015:
#   [21:38:44] ===   - GHS total: 675,049,344
# [21:38:44] ===   - Eurostat total: 674,457,504
# [21:38:44] ===   - Absolute difference: 591,840.2
# [21:38:44] ===   - Relative difference: 0.09%
# [21:38:44] ===   - Eurostat coverage: 42 countries (missing: 7)
# [21:38:48] === 
#   Processing year 2020
# [21:38:48] === Aggregating from 1000 to 25000 meters (factor: 25)
# [21:38:48] ✓ Population preserved after aggregation
# [21:38:48] === Saving aggregated raster to: C:/Users/Andre/OneDrive - Birkbeck, University of London/R/R_projects/NUTS_environ_data/Output/pop_data_eu_2020_25km.tif
# [21:38:48] === Aggregated population total: 682,604,866
# [21:38:49] !!! Edge cells: 40.5% of total
# [21:38:49] === Eurostat comparison for 2020:
#   [21:38:49] ===   - GHS total: 682,604,866
# [21:38:49] ===   - Eurostat total: 676,919,639
# [21:38:49] ===   - Absolute difference: 5,685,227
# [21:38:49] ===   - Relative difference: 0.84%
# [21:38:49] ===   - Eurostat coverage: 42 countries (missing: 7)
# [21:38:53] === 
#   Processing year 2025
# [21:38:53] === Aggregating from 1000 to 25000 meters (factor: 25)
# [21:38:53] ✓ Population preserved after aggregation
# [21:38:53] === Saving aggregated raster to: C:/Users/Andre/OneDrive - Birkbeck, University of London/R/R_projects/NUTS_environ_data/Output/pop_data_eu_2025_25km.tif
# [21:38:53] === Aggregated population total: 682,054,949
# [21:38:53] !!! Edge cells: 40.5% of total
# [21:38:53] === Eurostat comparison for 2025:
#   [21:38:53] ===   - GHS total: 682,054,949
# [21:38:53] ===   - Eurostat total: 181,346,868
# [21:38:53] ===   - Absolute difference: 500,708,081
# [21:38:53] !!!   - Relative difference: 276.11%
# [21:38:53] ===   - Eurostat coverage: 28 countries (missing: 4)
# [21:38:57] === 
#   Processing year 2030
# [21:38:57] === Aggregating from 1000 to 25000 meters (factor: 25)
# [21:38:58] ✓ Population preserved after aggregation
# [21:38:58] === Saving aggregated raster to: C:/Users/Andre/OneDrive - Birkbeck, University of London/R/R_projects/NUTS_environ_data/Output/pop_data_eu_2030_25km.tif
# [21:38:58] === Aggregated population total: 680,767,590
# [21:38:58] !!! Edge cells: 40.5% of total
# [21:38:58] === Eurostat comparison for 2030:
#   [21:38:58] ===   - GHS total: 680,767,590
# [21:38:58] ===   - Eurostat total: 180,981,036
# [21:38:58] ===   - Absolute difference: 499,786,554
# [21:38:58] !!!   - Relative difference: 276.15%
# [21:38:58] ===   - Eurostat coverage: 28 countries (missing: 4)
# 
# === Validation Summary ===
#   [21:39:03] ✓ Population trends: Consistent growth pattern
# [21:39:03] !!! Edge effects: High average (40.5%)
# [21:39:03] !!! Eurostat validation: Large differences in years: 1975, 1980, 1985, 1990, 2000, 2005, 2010, 2025, 2030
# [21:39:08] === 
#   Processing completed in 89.8 seconds
# [21:39:08] === Peak memory usage: 1.5 GB
# [21:39:08] === Output files saved in: C:\Users\Andre\OneDrive - Birkbeck, University of London\R\R_projects\NUTS_environ_data\Output
# 
# Detailed Population Comparison:
#   year ghs_population eurostat_population absolute_difference
# <num>         <char>              <char>              <char>
#   1:  1975    586,708,070         493,370,098          93,337,972
# 2:  1980    602,921,649         508,328,028          94,593,621
# 3:  1985    616,777,619         521,495,920          95,281,699
# 4:  1990    630,559,372         535,147,528          95,411,844
# 5:  1995    639,322,906         619,358,177          19,964,729
# 6:  2000    644,392,575         859,322,082        -214,929,507
# 7:  2005    653,095,059         866,751,019        -213,655,960
# 8:  2010    665,326,526         879,455,023        -214,128,497
# 9:  2015    675,049,344         674,457,504             591,840
# 10:  2020    682,604,866         676,919,639           5,685,227
# 11:  2025    682,054,949         181,346,868         500,708,081
# 12:  2030    680,767,590         180,981,036         499,786,554
# percent_difference countries_covered
# <char>             <int>
#   1:             18.92%                36
# 2:             18.61%                36
# 3:             18.27%                36
# 4:             17.83%                37
# 5:              3.22%                37
# 6:            -25.01%                46
# 7:            -24.65%                49
# 8:            -24.35%                47
# 9:              0.09%                42
# 10:              0.84%                42
# 11:            276.11%                28
# 12:            276.15%                28

