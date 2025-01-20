In this project I combine ERA5 climate data at 0.25º x 0.25º resolution, raster population data from two different sources and resolutions and, NUTS boundaries to create climate indicators at nuts1 and nuts2 region level.

# Workflow description:
This code creates two climate datasets for each EU NUTS regions level 1 and 2. The objective is to create a panel series for 2005-2022 with latitudinal and population weighted climate variables and a dataset with future climate projections, population weighted.

This projections combines historical ERA5 data with CMIP6 anomalies, using different methods for no-precipitation (additive) and precipitation variable (delta method), for each future climate scenario and NUTS region level. Results are exported in vector, RDS and tabular formats.

To obtain this we need to extract the following data:

era5-x0.25_A:
- Panel dataset for 2005-2022 
- Ten climate indicators across NUTS regions 
- Population weighted using GHS data 
- Three base years (2005/2015/2020) with interpolation for years in between

era5-x0.25_B: 
- Single baseline value per region
- Reference period 1995-2014
- Provides foundation for projections

cmip6-x0.25_A:
- Future projections for 2040-2059
- Two scenarios: SSP245 and SSP585
- Anomalies relative to baseline
- Uses scenario-specific populations

cmip6-x0.25_B:
- Historical CMIP6 data 1995-2014 
- Used for precipitation projections 
- Enables relative change calculations

pop_x0.25_future and pop_x0.25_future: 
- Future population projections for 2040-2059 
- Two scenarios: SSP245 and SSP585

# Obtaining data:

Three main inputs:

GHS Population: 
1km resolution population grid in Mollweide projection spanning 1975-2030. Gridded data enables consistent integration, stable cells over time, and custom zone aggregation.
  Epochs: 2005, 2015, 2020, 2025
  Resolution: 1km 
  Coordinate System: ESPG: 4326
  Raster

ERA5: High-resolution climate and population data. 
  Resolution: 0.25 by 0.25 degrees
  Coordinate System: Mollweide
  Raster
  
NUTS1/2 (2016): Administrative boundaries defining statistical regions. 
  Resolution: 20M 
  Coordinate System: ESPG: 4326
  Polygons
