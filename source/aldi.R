#--- Script details ------------------------------------------------------------
# Creation date: 23 February 2024
# Project:       Weekly scrapes
# Description:   Aldi store list
# Author:        Nick Twort

library(tidyverse)
library(rvest)
library(jsonlite)
library(arrow)

##--- * Get list of states and links thereto -----------------------------------

url <- "https://store.aldi.com.au/"

x <- read_html(url) |>
  html_nodes(".Directory-listLink")

states <- map_dfr(x, function(y) {
  
  tibble(state = html_text(y), current_link = paste0(url, html_attr(y, "href")))
  
})

##--- * For each state, get location links -------------------------------------

location_links <- pmap_dfr(states, function(state, current_link) {
  
  state_page <- read_html(current_link) |>
    html_nodes(".Directory-listLink")
  
  # List location links
  map_dfr(state_page, function(y) {
    
    tibble(
      state = state,
      store = html_text(y),
      current_link = paste0(url, html_attr(y, "href"))
    )
    
  })
  
})

# Given a store webpage, extract some relevant information
get_store_info <- function(state, store, current_link) {
  
  store_page <- read_html(current_link)
  
  address_info <- store_page |>
    html_node(".Address") |>
    html_nodes("meta")
  
  address <- tibble(
    name = html_attr(address_info, "itemprop"),
    value = html_attr(address_info, "content")
  ) |> 
    pivot_wider()
  
  store_name <- store_page |>
    html_nodes(".Hero-heading") |> 
    html_text() |> set_names("store") |>
    as_tibble_row()
  
  store_id <- store_page |>
    html_nodes(".Main") |>
    html_attr("itemid") |>
    set_names("id") |>
    as_tibble_row()
  
  # Try extract hours information
  hours <- store_page |> 
    html_nodes(".js-hours-config") |> 
    html_text()
  
  # If hours available, include it, else not
  if (length(hours) > 0) {
    hours <- hours |> 
      first() |> 
      fromJSON() |> 
      compact()
    hours <- tibble(
      hours = list(hours$hours)
    )
    
    return(
      address |> 
        bind_cols(hours, store_name, store_id) |> 
        mutate(
          state = state, store = store, link = current_link
        )
    )
    
  } else {
    
    return(
      address |> 
        bind_cols(store_name, store_id) |> 
        mutate(
          state = state, store = store, link = current_link
        )
    )
    
  }
  
}

# Get stores
aldi_stores <- location_links |> 
  pmap_dfr(function(state, store, current_link) {
    
    # Some locations have multiple stores - these need to go one level deeper
    is_sub_page <- read_html(current_link) |>
      html_nodes(".Teaser-titleLink") |> 
      html_attr("href") |> 
      str_detect("\\.\\./\\.\\.") |> 
      all() |> 
      (\(x) !x)()
    
    # If only one store, just get the information for that store
    if (!is_sub_page) {
      
      get_store_info(state, store, current_link)
      
      # If multiple stores, get the information for each store
    } else {
      
      sub_stores <- read_html(current_link) |>
        html_nodes(".Teaser-titleLink") |> 
        html_attr("href") |> 
        str_replace("\\.\\.", "https://store.aldi.com.au")
      
      tibble(state = state, store = store, current_link = sub_stores) |> 
        pmap_dfr(get_store_info)
      
    }
    
  })

aldi_stores <- aldi_stores |> select(-hours) |> mutate(timestamp = Sys.time())

concat_tables(
  read_parquet("data/aldi_stores.parquet", as_data_frame = FALSE),
  arrow_table(aldi_stores)
) |> 
  write_parquet(sink = "data/aldi_stores_temp.parquet")

file.rename("data/aldi_stores_temp.parquet", "data/aldi_stores.parquet")
