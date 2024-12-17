library(httr)
library(tidyverse)
library(arrow)

headers <- c(
  accept = "application/json, text/plain, */*",
  `accept-language` = "en,en-AU;q=0.9,en-NZ;q=0.8,en-GB;q=0.7,en-US;q=0.6",
  `cache-control` = "no-cache",
  dnt = "1",
  origin = "https://www.hoyts.com.au",
  pragma = "no-cache",
  priority = "u=1, i",
  referer = "https://www.hoyts.com.au/",
  `sec-ch-ua` = '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
  `sec-ch-ua-mobile` = "?0",
  `sec-ch-ua-platform` = '"Windows"',
  `sec-fetch-dest` = "empty",
  `sec-fetch-mode` = "cors",
  `sec-fetch-site` = "same-site",
  `user-agent` = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)

res <- GET(
  url = "https://apim.hoyts.com.au/au/cinemaapi/api/cinemas",
  add_headers(.headers=headers)
)

x <- content(res)

hoyts_stores <- map_dfr(x, function(y) {
  tibble(
    id = y$id,
    slug = y$slug,
    name = y$name,
    state = y$state,
    latitude = y$latitude,
    longitude = y$longitude,
    link = y$link,
    street = y$address$street,
    suburb = y$address$suburb,
    postcode = y$address$postcode
  )
})|> mutate(timestamp = Sys.time())

concat_tables(
  read_parquet("data/hoyts_stores.parquet", as_data_frame = FALSE),
  arrow_table(hoyts_stores)
) |> 
  write_parquet(sink = "data/hoyts_stores_temp.parquet")

file.rename("data/hoyts_stores_temp.parquet", "data/hoyts_stores.parquet")

