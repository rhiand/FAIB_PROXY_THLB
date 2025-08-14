library(dadmtools)
library(bcdata)
library(tidyverse)
library(cowplot)
library(ggplot2)
library(dplyr)
source('src/utils/functions.R')
conn_list <- dadmtools::get_pg_conn_list()
db <- DBI::dbConnect(conn_list["driver"][[1]],
				host = conn_list["host"][[1]],
				user = conn_list["user"][[1]],
				dbname = conn_list["dbname"][[1]],
				password = conn_list["password"][[1]],
				port = conn_list["port"][[1]])

dst_schema <- "whse"
vector_schema <- "whse_vector"
repo_path <- 'C:/projects/FAIB_PROXY_THLB'
## look at percentiles
query <- glue("SELECT 
	man_unit,
	opening_id,
  opening_area,
	sum(silv_polygon_area)/opening_area as prop
  FROM 
	{dst_schema}.retention_data_explore
WHERE 
	silv_reserve_code = 'G'
AND
	(silv_reserve_objective_code NOT IN ('TIM'))
AND
	man_unit is not null
AND 
	opening_category_code not in ('NREQ', 'SPEX', 'UHRV') -- filter out logged blocked vs govt funded silviculture activities (filter out govt funded silviculture)
GROUP BY 
	man_unit, opening_id, opening_area
ORDER BY 
	man_unit")
ret <- sql_to_df(query, conn_list)

# Calculate quantiles (0-10% to 90-100%) grouped by tsa_rank1
percentiles <- ret %>%
  group_by(man_unit) %>%
  summarise(
    count = n(),  # Count rows per man_unit
    percentiles = list(quantile(prop, probs = c(seq(0, 0.1, 0.01), seq(0.88, 1, 0.01)), na.rm = TRUE))
  ) %>%
  unnest_wider(percentiles, names_sep = "_p") %>%
  rename_with(~ paste0("p", c(seq(0, 10, 1), seq(88, 100, 1))), starts_with("percentiles_p"))

write.csv(percentiles, glue("{repo_path}/data/analysis/retention/retention_explore_percentiles_{format(Sys.Date(), '%Y_%m_%d')}.csv"), row.names=F)


output_pdf <- glue("{repo_path}/data/analysis/retention/man_unit_retention_histograms_{format(Sys.Date(), '%Y_%m_%d')}.pdf")
pdf(output_pdf, width = 11, height = 8.5)  # Landscape PDF

# Get unique man_units
man_units <- unique(ret$man_unit)

# List to store plots
plot_list <- list()

# Loop through each man_unit and create a histogram
for (unit in man_units) {
  # Filter data for the current man_unit
  filtered_data <- ret %>% filter(man_unit == unit)

  filtered_percentile <- percentiles %>% filter(man_unit == unit)
  tail_cutoff <- filtered_percentile %>%
    select(man_unit, starts_with("p")) %>%          # Select man_unit and percentile columns
    pivot_longer(cols = starts_with("p"),           # Transform wide data to long
                names_to = "percentile", 
                values_to = "value") %>%
    filter(value < 0.5) %>%                         # Keep values below 0.5
    slice_max(order_by = value, n = 1)   
  
  # Create a histogram for the 'prop' field
  p <- ggplot(filtered_data, aes(x = prop)) +
    geom_histogram(binwidth = 0.01, fill = "lightgrey", color = "black") +
 	  geom_vline(xintercept = tail_cutoff$value, color = "#0caa0c", linetype = "dashed", linewidth = 0.8) +  # Add vertical line
    geom_vline(xintercept = filtered_percentile$p1, color = "#0caa0c", linetype = "dashed", linewidth = 0.8) +  # Add vertical line
    ggtitle(unit) +
    xlab("Retention %") +
    ylab("Frequency") +
    theme_minimal() + 
    xlim(0.0, 1.0) +  # Set x-axis limits
    annotate(
      "label", 
      x=Inf, y = Inf,vjust=1, hjust=1,
      label = paste("Tail cutoff:", tail_cutoff$percentile),
      color = "black", 
      fill = "white",
      size = 3.3
    )
  
  # Add the plot to the list
  plot_list[[length(plot_list) + 1]] <- p
}

# Combine plots into grids of 12 per page
num_plots <- length(plot_list)
plots_per_page <- 12
pages <- ceiling(num_plots / plots_per_page)

for (page in 1:pages) {
  start_idx <- (page - 1) * plots_per_page + 1
  end_idx <- min(page * plots_per_page, num_plots)
  
  # Select plots for this page
  plots_this_page <- plot_list[start_idx:end_idx]
  
  # Arrange plots in a 3x4 grid
  grid <- plot_grid(plotlist = plots_this_page, ncol = 3, nrow = 3)
  
  # Print the grid to the PDF
  print(grid)
}

# Close the PDF device
dev.off()


## After reviewing histograms & tables - leads (Mark, Kelly, Gordon) are happy enough with the cutoff method suggested - going forward with using this method
## commit them to db
man_units <- unique(ret$man_unit)
ret_df <- data.frame(
  man_unit = character(),
  lower_bound_percentile = character(),
  upper_bound_percentile = character(),
  lower_bounds = numeric(),
  upper_bounds = numeric(),
  weighted_mean_ltr = numeric(), ## ltr: long term retention %
  stringsAsFactors = FALSE # Avoid converting strings to factors
)
for (unit in man_units) {
  filtered_ret <- ret %>% filter(man_unit == unit)
  filtered_percentile <- percentiles %>% filter(man_unit == unit)
  tail_cutoff <- filtered_percentile %>%
    select(man_unit, starts_with("p")) %>%          # Select man_unit and percentile columns
    pivot_longer(cols = starts_with("p"),           # Transform wide data to long
                names_to = "percentile", 
                values_to = "value") %>%
    filter(value < 0.5) %>%                         # Keep values below 0.5
    slice_max(order_by = value, n = 1)  

  filtered_ret <- filtered_ret %>%
    filter(between(prop, filtered_percentile$p1, tail_cutoff$value)) ## lower bounds: 1st percentile, upper bounds: max percentile less than 0.5 

  ltr <- weighted.mean(filtered_ret$prop, filtered_ret$opening_area)

  new_row <- data.frame(
    man_unit = tail_cutoff$man_unit,
    lower_bound_percentile = 'p1',
    upper_bound_percentile = tail_cutoff$percentile,
    lower_bounds = filtered_percentile$p1,
    upper_bounds = tail_cutoff$value,
    weighted_mean_ltr = ltr,
    stringsAsFactors = FALSE
  )
  ## append new row to dataframe
  ret_df <- rbind(ret_df, new_row)
}

df_to_pg(Id(schema = dst_schema, table = 'retention_thresholds_man_unit'), ret_df, conn_list, overwrite=TRUE, append=FALSE)