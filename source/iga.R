#--- Script details ------------------------------------------------------------
# Creation date: 23 February 2024
# Project:       Weekly scrapes
# Description:   IGA store list
# Author:        Nick Twort
library(tidyverse)
library(rvest)
library(arrow)

url <- "https://www.iga.com.au/stores/"
states <- c(
  "nt",
  "nsw",
  "vic",
  "qld",
  "act",
  "wa",
  "tas",
  "sa"
)

iga_stores <- map_dfr(states, function(state) {
  
  tryCatch({
    message(paste("Processing state:", state))
    
    state_page <- read_html(paste0(url, state))
    
    state_store_list <- state_page |>
      html_elements(".states-list") |>
      html_children() |> 
      html_children()
    
    state_stores <- tibble(
      url = html_attr(state_store_list, "href"),
      name = html_text(state_store_list),
      state = state
    )
    
    # Filter out any rows with missing URLs or names
    state_stores <- state_stores |> 
      filter(!is.na(url), !is.na(name), url != "", name != "")
    
    if (nrow(state_stores) == 0) {
      warning(paste("No stores found for state:", state))
      return(tibble())
    }
    
    message(paste("Found", nrow(state_stores), "stores in", state))
    
    pmap_dfr(state_stores, function(url, name, state) {
      
      tryCatch({
        message(paste("  Processing store:", name))
        
        store_page <- read_html(url)
        
        # Get address components with error handling
        address_line_1 <- tryCatch({
          store_page |> 
            html_element("#store-address-line-1") |> 
            html_text()
        }, error = function(e) "")
        
        address_line_2 <- tryCatch({
          store_page |> 
            html_element("#store-address-line-2") |> 
            html_text()
        }, error = function(e) "")
        
        # Clean up address components
        address_line_1 <- ifelse(is.na(address_line_1) | address_line_1 == "", "", address_line_1)
        address_line_2 <- ifelse(is.na(address_line_2) | address_line_2 == "", "", address_line_2)
        
        # Build address, handling empty components
        address_components <- c(address_line_1, address_line_2, "Australia")
        address_components <- address_components[address_components != ""]
        store_address <- paste(address_components, collapse = ", ")
        
        tibble(
          address = store_address,
          name = name,
          url = url,
          state = state
        )
        
      }, error = function(e) {
        warning(paste("Error processing store", name, "in", state, ":", e$message))
        # Return empty tibble for failed stores
        tibble()
      })
      
    })
    
  }, error = function(e) {
    warning(paste("Error processing state", state, ":", e$message))
    # Return empty tibble for failed states
    tibble()
  })
  
})

# Add timestamp
iga_stores <- iga_stores |> mutate(timestamp = Sys.time())

# Save results with error handling
tryCatch({
  concat_tables(
    read_parquet("data/iga_stores.parquet", as_data_frame = FALSE),
    arrow_table(iga_stores)
  ) |> 
    write_parquet(sink = "data/iga_stores_temp.parquet")
  
  file.rename("data/iga_stores_temp.parquet", "data/iga_stores.parquet")
  message("Successfully saved", nrow(iga_stores), "stores to parquet file")
  
}, error = function(e) {
  warning(paste("Error saving data:", e$message))
  # Fallback: save just the new data
  tryCatch({
    arrow_table(iga_stores) |> 
      write_parquet(sink = paste0("data/iga_stores_backup_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".parquet"))
    message("Saved backup file due to concatenation error")
  }, error = function(e2) {
    stop(paste("Critical error: Could not save data at all:", e2$message))
  })
})
