library(tidyverse)
library(rvest)
library(arrow)
library(httr)

headers <- c(
  accept = "application/json, text/plain, */*",
  `accept-language` = "en,en-AU;q=0.9,en-NZ;q=0.8,en-GB;q=0.7,en-US;q=0.6",
  `cache-control` = "no-cache",
  dnt = "1",
  origin = "https://www.bigw.com.au",
  pragma = "no-cache",
  priority = "u=1, i",
  referer = "https://www.bigw.com.au/",
  `sec-ch-ua` = '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
  `sec-ch-ua-mobile` = "?0",
  `sec-ch-ua-platform` = '"Windows"',
  `sec-fetch-dest` = "empty",
  `sec-fetch-mode` = "cors",
  `sec-fetch-site` = "same-site",
  `user-agent` = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)

res <- GET(url = "https://api.bigw.com.au/api/stores/v0/list", add_headers(headers))

big_w_stores <- map_dfr(content(res), function(y) {
  tibble(
    id = y$id,
    name = y$name,
    phoneNumber = y$phoneNumber,
    address = paste(y$address, collapse = ", "),
    lat = y$location$lat,
    lng = y$location$lng
  )
  
}) |> mutate(timestamp = Sys.time())

concat_tables(
  read_parquet("data/big_w_stores.parquet", as_data_frame = FALSE),
  arrow_table(big_w_stores)
) |> 
  write_parquet(sink = "data/big_w_stores_temp.parquet")

file.rename("data/big_w_stores_temp.parquet", "data/big_w_stores.parquet")
