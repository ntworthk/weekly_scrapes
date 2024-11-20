library(arrow)
library(tidyverse)

data <- list.files("data", full.names = TRUE)
names(data) <- str_remove_all(data, "data/|_stores.*")

df <- map_dfr(data, function(file) {
  
  read_parquet(file, as_data_frame = FALSE) |> 
    filter(timestamp == max(timestamp)) |> 
    count(timestamp) |> 
    collect()
  
}, .id = "Store") |> 
  arrange(desc(n)) |> 
  mutate(Store = fct_inorder(Store))

g <- df |> 
  ggplot(aes(x = n, y = Store)) + 
  geom_col(fill = "#008698") +
  geom_text(aes(label = n), hjust = 1.05, colour = "white") +
  geom_vline(xintercept = 0) +
  labs(x = NULL, y = NULL, title = paste0("Number of stores as at ", format(max(df$timestamp), "%d %B %Y"))) +
  theme_minimal(base_size = 12) +
  theme(
    panel.grid.major.y = element_blank(),
    panel.grid.minor.y = element_blank(),
    panel.background = element_blank(),
    plot.background = element_blank(),
    legend.background = element_blank(),
    legend.position = "bottom",
    strip.background = element_rect(fill = "#E6E7E8")
  )

ggsave(filename = "README_files/figure-commonmark/unnamed-chunk-1-1.png",
       plot = g,
       width = 17.00,
       height = 8,
       units = "cm",
       bg = "transparent"
)
