library(rjson) # install.packages("rjson")

geocode = "FRA"
collection_code = "cmip6-x0.25"
type_code = "timeseries"
variable_codes = "hd30"        # Max 3 variables
product_code = "timeseries"
aggregation_code = "annual"
period_code = "2015-2100"
percentile_code = "median"
scenario_codes = "ssp245"  # allows combined scenarios
model_code = "ensemble"
model_calculation_code = "all"
statistic_code = "mean"
  
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
    
    View(df)
  }
}