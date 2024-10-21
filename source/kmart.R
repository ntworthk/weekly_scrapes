library(httr)
library(rvest)
library(jsonlite)
library(tidyverse)
library(arrow)

url <- "https://www.kmart.com.au/sitemap/au/storelocation-sitemap.xml"

urls <- url |> 
  url() |> 
  readLines() |> 
  str_subset("<loc>") |> 
  str_extract("https.*/<") |> 
  str_remove("<")
  

get_store <- function(url) {

  resp <- GET(url)
  
  if (status_code(resp) == 200) {
    rr <- read_html(resp) |> 
      html_element("#__NEXT_DATA__") |> 
      html_text() |> 
      fromJSON()
    
    y <- rr$props$pageProps$location
    
    tibble(
      locationId = y$locationId,
      publicName = y$publicName,
      phoneNumber = y$phoneNumber,
      address1 = y$address1,
      address2 = y$address2,
      address3 = y$address3,
      city = y$city,
      state = y$state,
      postcode = y$postcode,
      latitude = y$latitude,
      longitude = y$longitude,
      tradingHours = list(y$tradingHours),
      typename = y$`__typename`
    )
    
  } else {
    tibble()
  }
}

kmart_stores <- map_dfr(urls, get_store)

kmart_stores <- kmart_stores |> mutate(timestamp = Sys.time()) |> 
  select(-tradingHours)

concat_tables(
  read_parquet("data/kmart_stores.parquet", as_data_frame = FALSE),
  arrow_table(kmart_stores)
) |> 
  write_parquet(sink = "data/kmart_stores_temp.parquet")

file.rename("data/kmart_stores_temp.parquet", "data/kmart_stores.parquet")
