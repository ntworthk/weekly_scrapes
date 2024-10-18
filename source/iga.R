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
  
  pmap_dfr(state_stores, function(url, name, state) {
    
    store_page <- read_html(url)
    
    store_address <- paste(
      store_page |> 
        html_element("#store-address-line-1") |> 
        html_text(),
      store_page |> 
        html_element("#store-address-line-2") |> 
        html_text(),
      "Australia",
      sep = ", "
    )
    
    tibble(
      address = store_address,
      name = name,
      url = url,
      state = state
    )
    
    
  })
  
})

iga_stores <- iga_stores |> mutate(timestamp = Sys.time())

concat_tables(
  read_parquet("data/iga_stores.parquet", as_data_frame = FALSE),
  arrow_table(iga_stores)
) |> 
  write_parquet(sink = "data/iga_stores_temp.parquet")

file.rename("data/iga_stores_temp.parquet", "data/iga_stores.parquet")
