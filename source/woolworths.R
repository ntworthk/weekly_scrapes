library(xml2)
library(tidyverse)
library(arrow)

url <- "https://contact.woolworths.com.au/storelocator/service/proximity/supermarkets/latitude/-37.7510/longitude/144.8981/range/50000/max/2000.xml"

ll <- read_xml(url) |> as_list()

woolworths_stores <- map_dfr(ll$locatorList$storeList, function(x) {
  
  y <- x$storeDetail
  tibble(
    division = unlist(y$division),
    no = unlist(y$no),
    name = unlist(y$name),
    addressLine1 = unlist(y$addressLine1),
    addressLine2 = unlist(y$addressLine2),
    suburb = unlist(y$suburb),
    postcode = unlist(y$postcode),
    state = unlist(y$state),
    country = unlist(y$country),
    phone = unlist(y$phone),
    email = unlist(y$email),
    latitude = unlist(y$latitude),
    longtitude = unlist(y$longtitude),
    geoLevel = unlist(y$geoLevel),
    extra1 = unlist(y$extra1),
    extra2 = unlist(y$extra2),
    extra3 = unlist(y$extra3),
    extra4 = unlist(y$extra4),
    extra5 = unlist(y$extra5),
    extra6 = unlist(y$extra6),
    extra7 = unlist(y$extra7),
    extra8 = unlist(y$extra8),
    extra9 = unlist(y$extra9),
    extra10 = unlist(y$extra10),
    extra11 = unlist(y$extra11),
    extra12 = unlist(y$extra12),
    extra13 = unlist(y$extra13),
    extra14 = unlist(y$extra14),
    extra15 = unlist(y$extra15),
    GMTZone = unlist(y$GMTZone)
    
  )
}) |> mutate(timestamp = Sys.time())

concat_tables(
  read_parquet("data/woolworths_stores.parquet", as_data_frame = FALSE),
  arrow_table(woolworths_stores)
) |> 
  write_parquet(sink = "data/woolworths_stores_temp.parquet")

file.rename("data/woolworths_stores_temp.parquet", "data/woolworths_stores.parquet")

