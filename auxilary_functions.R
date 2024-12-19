# auxilary_functions

## function for saving and reading metadata From ErikKusch----- 
## https://github.com/rspatial/terra/issues/1549

Meta.NC <- function(NC, FName, Attrs, Write = FALSE, Read = FALSE){
  ## Writing metadata
  if(Write){
    writeCDF(x = NC, filename = FName , overwrite = TRUE)
    
    nc <- nc_open(FName, write = TRUE)
    for(name in names(Attrs)) {
      ncatt_put(nc, 0, name, Attrs[[name]])
    }
    nc_close(nc)
  }
  ## Reading metadata
  if(Read){
    nc <- nc_open(FName)
    # Retrieve custom metadata
    Meta <- lapply(names(Attrs), FUN = function(name){
      ncatt_get(nc, 0, name)$value
    })
    # Close the NetCDF file
    nc_close(nc)
    Meta_vec <- unlist(Meta)
    names(Meta_vec) <- names(Attrs)
    terra::metags(NC) <- Meta_vec
  }
  ## return object
  return(NC)
}
