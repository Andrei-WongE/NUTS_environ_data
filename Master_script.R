## ---------------------------
##
## Script name: NUTS
##
## Project: NUTS 1 and 2 aggregation
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

## Program Set-up ------------

options(scipen = 100, digits = 4) # Prefer non-scientific notation
terra::terraOptions(
  memfrac = 0.8,    # Use 80% of available RAM
  progress = 10    # Show progress bar for operations >10 chunks
)

## Load required packages ----

library("pacman")
library("here")
library("groundhog")

set.groundhog.folder(here("groundhog_library"))
groundhog.day = "2024-04-25" #"2020-05-12"
#Dowloaded fromn https://github.com/CredibilityLab/groundhog

pkgs = c("dplyr", "tidyverse", "janitor", "sf"
         , "ggplot2","xfun", "remotes", "sp", "spdep"
         , "foreach", "doParallel", "progress"
         , "doSNOW", "purrr", "patchwork"
         , "haven", "openxlsx", "MASS", "reticulate"
         , "future", "furrr", "data.table","leaflet"
         , "jtools", "tidyr", "ggspatial", "raster"
         , "prettymapr", "viridis", "labelled"
         , "writexl", "WDI", "wesanderson", "ggrepel"
         , "ggbreak", "nuts", "httr", "jsonlite", "tidyr"
         ,"progressr", "future", "furrr", "future.apply"
         , "ncdf4", "terra", "tidyterra", "tictoc", "pryr"
)

groundhog.library(pkgs, groundhog.day
                  #, ignore.deps =  "fs"
                  )

