library(httr)
library(tidyverse)
library(arrow)

headers <- c(
  Accept = "application/json, text/plain, */*",
  `Accept-Language` = "en,en-AU;q=0.9,en-NZ;q=0.8,en-GB;q=0.7,en-US;q=0.6",
  Authorization = "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IkJGRTFEMDBBRUZERkVDNzM4N0E1RUFFMzkxNjRFM0MwMUJBNzVDODciLCJ4NXQiOiJ2LUhRQ3VfZjdIT0hwZXJqa1dUandCdW5YSWMiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL2J1bm5pbmdzLmNvbS5hdS8iLCJuYmYiOjE3MzIwODQ2MjQsImlhdCI6MTczMjA4NDYyNCwiZXhwIjoxNzMyNTE2NjI0LCJhdWQiOlsiQ2hlY2tvdXQtQXBpIiwiY3VzdG9tZXJfYnVubmluZ3MiLCJodHRwczovL2J1bm5pbmdzLmNvbS5hdS9yZXNvdXJjZXMiXSwic2NvcGUiOlsiY2hrOmV4ZWMiLCJjbTphY2Nlc3MiLCJlY29tOmFjY2VzcyIsImNoazpwdWIiXSwiYW1yIjpbImV4dGVybmFsIl0sImNsaWVudF9pZCI6ImJ1ZHBfZ3Vlc3RfdXNlcl9hdSIsInN1YiI6IjFkMTg4YjRkLTJiYjQtNDg0ZS1hYmI3LWEyNTg0MTI0NTg1NSIsImF1dGhfdGltZSI6MTczMjA4NDYyNCwiaWRwIjoibG9jYWxsb29wYmFjayIsImItaWQiOiIxZDE4OGI0ZC0yYmI0LTQ4NGUtYWJiNy1hMjU4NDEyNDU4NTUiLCJiLXJvbGUiOiJndWVzdCIsImItdHlwZSI6Imd1ZXN0IiwibG9jYWxlIjoiZW5fQVUiLCJiLWNvdW50cnkiOiJBVSIsImFjdGl2YXRpb25fc3RhdHVzIjoiRmFsc2UiLCJ1c2VyX25hbWUiOiIxZDE4OGI0ZC0yYmI0LTQ4NGUtYWJiNy1hMjU4NDEyNDU4NTUiLCJiLXJiYWMiOlt7ImFzYyI6ImRiNzQzZDg5LTc5ZDgtNDcwYS1iODdiLTAyNjAyYTg4NGY4NCIsInR5cGUiOiJDIiwicm9sIjpbIkNISzpHdWVzdCJdfV0sInNpZCI6Ijg4Qzg2QzY3MzY4MEQ0MUU1RkY3REYwNUYwQzNBQTk2IiwianRpIjoiOTdERTRFNTYyMzJDM0FCM0NGREFFMkMzRUMyQjIxMEMifQ.c4fzwyvKecGlZyssfxCSzEEGzUpnObfBjGftzJkXGk9M53Ot7Cnx-UNEPg0JWAiW8bvEsgWau2aryanaH_NZWEQRlLxqnNwQ3qC-Mk1HyjkAorbTT2uCH2DzuwXmE5pAHVDABa3GAmXtDVPcsxURzpOntFeXLNYIbrZPDzj2U3Dvt-wXkwsv_M8ovPyE-qZI3mcfOwPAG48StRaczwiVlnpXJIEvi5Rc2n9Y3APSNT2XIx5DIfTIQG35Y3Xga5euEmgcAN3fNefVhqgXZB4qcyXwoQ2lUWJAypuM1JLg6tgYN7QSHx2sP9iywBCk0g5x-KRbi66oz-ejywMYjuNblw",
  `Cache-Control` = "no-cache",
  Connection = "keep-alive",
  DNT = "1",
  Origin = "https://www.bunnings.com.au",
  Pragma = "no-cache",
  Referer = "https://www.bunnings.com.au/",
  `Sec-Fetch-Dest` = "empty",
  `Sec-Fetch-Mode` = "cors",
  `Sec-Fetch-Site` = "same-site",
  `User-Agent` = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
  `X-region` = "VICMetro",
  clientId = "mHPVWnzuBkrW7rmt56XGwKkb5Gp9BJMk",
  country = "AU",
  currency = "AUD",
  locale = "en_AU",
  locationCode = "6400",
  `sec-ch-ua` = '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
  `sec-ch-ua-mobile` = "?0",
  `sec-ch-ua-platform` = '"Windows"',
  stream = "RETAIL",
  userId = "anonymous"
)

params <- list(
  fields = "FULL"
)

res <- GET(
  url = "https://api.prod.bunnings.com.au/v1/stores/country/AU",
  add_headers(.headers=headers),
  query = params
)

x <- content(res)

bunnings_stores <- map_dfr(x$data$pointOfServices, function(y) {
  tibble(
    id = y$address$id,
    postalCode = y$address$postalCode,
    line1 = y$address$line1,
    shippingAddress = y$address$shippingAddress,
    formattedAddress = y$address$formattedAddress,
    displayName = y$displayName,
    latitude = y$geoPoint$latitude,
    longitude = y$geoPoint$longitude,
    name = y$name,
    pricingRegion = y$pricingRegion,
    storeRegion = y$storeRegion,
    mapUrl = y$mapUrl
  )
})|> mutate(timestamp = Sys.time())

concat_tables(
  read_parquet("data/bunnings_stores.parquet", as_data_frame = FALSE),
  arrow_table(bunnings_stores)
) |> 
  write_parquet(sink = "data/bunnings_stores_temp.parquet")

file.rename("data/bunnings_stores_temp.parquet", "data/bunnings_stores.parquet")

