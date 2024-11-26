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
## Date Created: 2024-11-26
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

options(scipen = 100, digits = 4) # Prefer non-scientific notation
dir.create(here("Data", "Eurostat"), showWarnings = FALSE)

## Load required packages ----

source("Master_script.R")

## Runs the following --------
# WARNING: ALL created by Claude, minor modifications CHECK

# Load required libraries
library(eurostat)
library(tidyverse)
library(data.table)
library(here)

# Create directories
dir.create(here("Data", "Eurostat"), showWarnings = FALSE)

# Function to format large numbers with commas
format_pop <- function(x) {
  format(x, big.mark = ",", scientific = FALSE, trim = TRUE)
}

# Define European countries list
european_countries <- c(
  # Core EU + EFTA
  "BE", "BG", "CZ", "DK", "DE", "EE", "IE", "EL", "ES", "FR",
  "HR", "IT", "CY", "LV", "LT", "LU", "HU", "MT", "NL", "AT",
  "PL", "PT", "RO", "SI", "SK", "FI", "SE", 
  "CH", "IS", "LI", "NO",
  # UK
  "UK",
  # Additional European countries
  "AD", "AL", "AM", "BA", "BY", "FX", "GE", "MC", "MD", 
  "ME", "MK", "RS", "RU", "SM", "TR", "UA", "XK"
)

# Function to get historical population data
get_historical_pop <- function() {
  message("Downloading historical population data...")
  
  hist_data <- get_eurostat("demo_pjan",
                            time_format = "num",
                            cache = TRUE,
                            filters = list(
                              sex = "T",    
                              age = "TOTAL" 
                            ))
  
  # Process historical data
  hist_processed <- hist_data %>%
    filter(geo %in% european_countries) %>%
    select(geo, time, values) %>%
    mutate(data_source = "demo_pjan",
           data_type = "historical")
  
  # Summary of available countries with populations
  message("\nHistorical data coverage:")
  coverage_summary <- hist_processed %>%
    filter(time == max(time)) %>%
    arrange(desc(values))
  
  message("Latest year (", max(hist_processed$time), ") population summary:")
  message("Total population: ", format_pop(sum(coverage_summary$values, na.rm = TRUE)))
  message("Countries with data: ", nrow(coverage_summary))
  
  # Report missing countries
  missing_hist <- setdiff(european_countries, unique(hist_processed$geo))
  if(length(missing_hist) > 0) {
    message("Missing countries: ", paste(missing_hist, collapse = ", "))
  }
  
  return(hist_processed)
}

# Function to get projection data
get_projection_pop <- function() {
  message("\nDownloading projection data...")
  
  proj_data <- get_eurostat("urt_proj_19rp",
                            time_format = "num",
                            cache = TRUE) %>%
    filter(
      TIME_PERIOD %in% c(2025, 2030),
      sex == "T",          
      age == "TOTAL",      
      terrtypo == "INT",   
      projection == "BSL", 
      unit == "PER"       
    ) %>%
    rename(time = TIME_PERIOD)
  
  # Process projection data
  proj_processed <- proj_data %>%
    filter(geo %in% european_countries) %>%
    select(geo, time, values) %>%
    mutate(data_source = "urt_proj_19rp",
           data_type = "projection")
  
  # Add UK with NA values
  uk_proj <- expand.grid(
    geo = "UK",
    time = c(2025, 2030),
    stringsAsFactors = FALSE
  ) %>%
    mutate(
      values = NA_real_,
      data_source = "urt_proj_19rp",
      data_type = "projection"
    )
  
  proj_processed <- bind_rows(proj_processed, uk_proj)
  
  # Summary of projections
  message("\nProjection data coverage:")
  proj_summary <- proj_processed %>%
    group_by(time) %>%
    summarise(
      total_pop = sum(values, na.rm = TRUE),
      countries_with_data = sum(!is.na(values)),
      countries_missing = sum(is.na(values))
    )
  
  message("Projection totals by year:")
  print(proj_summary %>%
          mutate(total_pop = format_pop(total_pop)))
  
  # Report missing countries
  available_proj <- unique(proj_processed$geo[!is.na(proj_processed$values)])
  missing_proj <- setdiff(european_countries, available_proj)
  if(length(missing_proj) > 0) {
    message("Missing projections for: ", paste(missing_proj, collapse = ", "))
  }
  
  return(proj_processed)
}

# Function to combine and process all data
create_population_database <- function() {
  # Get both datasets
  historical <- get_historical_pop()
  projections <- get_projection_pop()
  
  # Combine datasets
  combined_data <- bind_rows(historical, projections) %>%
    arrange(time, geo)
  
  # Filter for target years
  target_years <- seq(1975, 2030, by = 5)
  final_data <- combined_data %>%
    filter(time %in% target_years) %>%
    arrange(time, geo)
  
  # Calculate detailed totals by year
  totals <- final_data %>%
    group_by(time) %>%
    summarise(
      total_pop = sum(values, na.rm = TRUE),
      countries_with_data = sum(!is.na(values)),
      countries_missing = sum(is.na(values)),
      mean_pop = mean(values, na.rm = TRUE),
      min_pop = min(values, na.rm = TRUE),
      max_pop = max(values, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(time)
  
  # Create metadata with detailed counts
  metadata <- list(
    created_date = Sys.time(),
    sources = list(
      historical = list(
        dataset = "demo_pjan",
        description = "Population on 1 January by age and sex",
        years_covered = range(historical$time)
      ),
      projections = list(
        dataset = "urt_proj_19rp",
        description = "Regional population projections - baseline",
        notes = "UK values marked as NA for projections",
        years_covered = range(projections$time)
      )
    ),
    countries = list(
      included = european_countries,
      available_historical = sort(unique(historical$geo)),
      available_projections = sort(unique(projections$geo[!is.na(projections$values)])),
      total_countries = length(european_countries)
    ),
    years_covered = sort(unique(final_data$time)),
    coverage_stats = list(
      total_timepoints = length(target_years),
      complete_country_years = sum(complete.cases(final_data)),
      missing_country_years = sum(!complete.cases(final_data))
    ),
    notes = c(
      "All available European countries included",
      "UK included throughout series (NA for projections)",
      "Population values stored with full precision",
      "Values represent total population (all ages, both sexes)"
    )
  )
  
  # Save files with full precision
  message("\nSaving data files...")
  fwrite(final_data, 
         here("Data", "Eurostat", "population_all_europe.csv"),
         scipen = 999)
  
  fwrite(totals %>%
           mutate(across(where(is.numeric), format_pop)),
         here("Data", "Eurostat", "population_totals_all_europe.csv"))
  
  saveRDS(metadata, 
          here("Data", "Eurostat", "population_metadata_all_europe.rds"))
  
  # Print detailed summary
  message("\nDetailed population summary by year:")
  print(totals %>%
          mutate(
            total_pop = format_pop(total_pop),
            mean_pop = format_pop(mean_pop),
            min_pop = format_pop(min_pop),
            max_pop = format_pop(max_pop),
            coverage = sprintf("%d countries (missing: %d)",
                               countries_with_data,
                               countries_missing)
          ))
  
  # Print coverage overview
  message("\nOverall coverage summary:")
  message("Total timepoints: ", length(target_years))
  message("Complete country-years: ", 
          metadata$coverage_stats$complete_country_years)
  message("Missing country-years: ", 
          metadata$coverage_stats$missing_country_years)
  
  return(list(
    data = final_data,
    totals = totals,
    metadata = metadata
  ))
}

# Execute the process
results_eupop <- create_population_database()