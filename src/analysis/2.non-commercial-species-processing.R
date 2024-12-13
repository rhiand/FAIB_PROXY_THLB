library(dadmtools)
source('src/utils/functions.R')

conn_list <- dadmtools::get_pg_conn_list()
dst_schema <- "thlb_proxy"

query <- "DROP TABLE IF EXISTS thlb_proxy.non_commercial_tsa_species_1_cc_ha;"
run_sql_r(query, conn_list)

query <- "CREATE TABLE thlb_proxy.non_commercial_tsa_species_1_cc_ha AS
SELECT
tsa_rank1
, CASE 
	WHEN vri.bclcs_level_1 = 'U' THEN vritfl.species_cd_1 
	ELSE vri.species_cd_1
  END AS species_cd_1
 , count(*) as species_cd_1_ha
, sum(CASE WHEN cc.harvest_start_year_calendar >= 2017 THEN 1 ELSE 0 END) as harvested_ha

FROM
whse.all_bc_gr_skey bc
LEFT JOIN whse.veg_consolidated_cut_blocks_sp_2024_gr_skey cc_key ON cc_key.gr_skey = bc.gr_skey
LEFT JOIN whse.veg_consolidated_cut_blocks_sp_2024 cc ON cc.pgid = cc_key.pgid
LEFT JOIN thlb_proxy.veg_comp_lyr_r1_poly_2016_gr_skey vri_key on vri_key.gr_skey = bc.gr_skey
LEFT JOIN thlb_proxy.veg_comp_lyr_r1_poly_2016 vri ON vri.pgid = vri_key.pgid
LEFT JOIN thlb_proxy.tfl_integrated2016_gr_skey vritfl_key ON vritfl_key.gr_skey = vri_key.gr_skey
LEFT JOIN thlb_proxy.tfl_integrated2016 vritfl ON vritfl_key.pgid = vritfl.pgid
LEFT JOIN whse.man_unit_gr_skey man_unit on man_unit.gr_skey = bc.gr_skey
LEFT JOIN thlb_proxy.seral_2023_tap_method fmlb on fmlb.gr_skey = bc.gr_skey
-- LEFT JOIN whse.north_south_coast_gr_skey area_key on area_key.gr_skey = bc.gr_skey
-- LEFT JOIN whse.north_south_coast area on area.fid = area_key.fid
WHERE
	fmlb.fmlb_adj = 1
GROUP BY
	tsa_rank1
	, CASE 
		WHEN vri.bclcs_level_1 = 'U' THEN vritfl.species_cd_1 
		ELSE vri.species_cd_1
	 END;"
run_sql_r(query, conn_list)

query <- "DROP TABLE IF EXISTS thlb_proxy.non_commercial_lu_table;"
run_sql_r(query, conn_list)

query <- "CREATE TABLE thlb_proxy.non_commercial_lu_table AS
WITH vri_species_cd_datadict AS (
	SELECT
		CASE 
			WHEN upper(species_cd) IN ('AC','ACB','ACT','AD','AX','DG', 'E','EA','EB','EP', 'ES', 'EW','M','MB','MN', 'MV', 'QG', 'RA', 'VB', 'W', 'WS', 'XH', 'ZH') then 'decid'
			WHEN upper(species_cd) IN ('AT', 'SS', 'SB', 'PA') THEN lower(species_full_name)
			WHEN upper(species_cd) IN ('D', 'DR') THEN 'alder'
			WHEN upper(species_cd) LIKE 'F%' THEN 'fir'
			WHEN upper(species_cd) LIKE 'C%' then 'cedar'
			WHEN upper(species_cd) LIKE 'H%' then 'hemlock'
			WHEN upper(species_cd) LIKE 'S%' then 'spruce'
			WHEN upper(species_cd) LIKE 'B%' then 'balsam'
			WHEN upper(species_cd) IN ('PF','PW') then 'white pine'
			WHEN upper(species_cd) IN ('P','PL','PLI','PLC', 'PJ') then 'pine'
			WHEN upper(species_cd) IN ('PY') then 'yellow pine'
			WHEN upper(species_cd)  LIKE 'L%' then 'larch'
			WHEN upper(species_cd) IN ('YC') then 'cypress'
			ELSE 'other'
		END as species_grouping,
		species_cd,
		species_full_name,
		type
	FROM
		thlb_proxy.vri_species_cd_datadict			   
), species_grouping AS (
SELECT 
	tsa_rank1,
	species_grouping,
	sum(species_cd_1_ha) as species_ha,
	sum(harvested_ha) as harvested_ha
FROM
thlb_proxy.non_commercial_tsa_species_1_cc_ha tbl
JOIN vri_species_cd_datadict dict on tbl.species_cd_1 = dict.species_cd
GROUP BY 
	tsa_rank1,
	species_grouping
), species_breakdown as (
SELECT
	CASE WHEN tsa_rank1 IN ('Arrowsmith TSA','Fraser TSA','GBR North TSA','GBR South TSA','Haida Gwaii TSA','North Island TSA','Soo TSA','Sunshine Coast TSA', 'Pacific TSA', 'Kalum TSA') THEN 'coast'
	ELSE 'interior'
	END AS area,
	tsa_rank1 as tsa,
	species_grouping,
	species_ha,
	harvested_ha,
	round((species_ha / sum(species_ha) OVER (partition by tsa_rank1)) * 100, 3) as pct_occurrence_in_landbase_fmlb,
	round((harvested_ha / sum(harvested_ha) OVER (partition by tsa_rank1)) * 100, 3) as pct_occurrence_in_harvested_fmlb,
	sum(species_ha) OVER (partition by tsa_rank1) as landbase_fmlb_ha,
	sum(harvested_ha) OVER (partition by tsa_rank1) as harvested_fmlb_ha
FROM
	species_grouping
WHERE 
	tsa_rank1 is not null
ORDER BY
	CASE WHEN tsa_rank1 IN ('Arrowsmith TSA','Fraser TSA','GBR North TSA','GBR South TSA','Haida Gwaii TSA','North Island TSA','Soo TSA','Sunshine Coast TSA', 'Pacific TSA', 'Kalum TSA') THEN 'coast'
	ELSE 'interior'
	END,
	tsa_rank1,
	round((species_ha / sum(species_ha) OVER (partition by tsa_rank1)) * 100, 1) DESC,
	round((harvested_ha / sum(harvested_ha) OVER (partition by tsa_rank1)) * 100, 1) DESC
)
SELECT
	*,
	CASE
		WHEN species_grouping in ('black spruce', 'other', 'whitebark pine') THEN species_grouping
		WHEN species_grouping in ('decid', 'trembling aspen', 'alder') THEN 
			CASE
				WHEN pct_occurrence_in_harvested_fmlb < (pct_occurrence_in_landbase_fmlb/2) OR harvested_ha = 0 THEN species_grouping || ' species, < 50% occurrence harvested'
				ELSE NULL
			END
		ELSE NULL
		END AS non_commercial
FROM 
	species_breakdown;"
run_sql_r(query, conn_list)


## exploration of non-commercial species
output_pdf <- "final/charts/species_TSA_BarGraphs_percent_occurrence_for_non_commercial_exploration_2024_12_12.pdf"
pdf(output_pdf, width = 10, height = 8)

# Unique TSAs
tsas <- unique(data$tsa)

# List to store plots
plot_list <- list()

# Loop through each TSA to create a plot
for (tsa in tsas) {
  # Filter data for the TSA
  tsa_data <- data %>% filter(tsa == !!tsa)
  y_range <- range(0,60)
  species_order <- c('balsam','cedar','cypress','fir','hemlock','larch','pine','sitka spruce','spruce','white pine','yellow pine','alder','trembling aspen','decid','other','black spruce','whitebark pine')
  highlight_species <- c('decid','other','black spruce','whitebark pine')
  highlight_color <- 'red'  # Color for highlighted labels
  title_text = glue('{tsa}, {tsa_data$area[1]}')
  # Reshape the data to long format for grouped bars
  tsa_long <- tsa_data %>%
    pivot_longer(cols = c(pct_occurrence_in_landbase_fmlb, pct_occurrence_in_harvested_fmlb), 
                 names_to = "category", 
                 values_to = "percentage") %>%
    mutate(category = recode(category, 
                             pct_occurrence_in_landbase_fmlb = "Landbase", 
                             pct_occurrence_in_harvested_fmlb = "Harvested"))
  
  landbase_leg_label <- paste0("Landbase: ", format(tsa_data$landbase_fmlb_ha[1], big.mark = ","), " ha")
  harvest_leg_label <- paste0("Harvested: ", format(tsa_data$harvested_fmlb_ha[1], big.mark = ","), " ha")
  
  # Create the plot
  p <- ggplot(tsa_long, aes(x = species_grouping, y = percentage, fill = category)) +
	geom_bar(stat = "identity", position = position_dodge(width = 0.6), width = 0.5) +
	scale_fill_manual(name = NULL, values = c("Landbase" = "green", 'Harvested' = "grey"), labels = c(harvest_leg_label, landbase_leg_label)) +
	labs(title = title_text, x = NULL, y = "% popn in unit") +
	theme_minimal() +
	theme(axis.text.x = element_text(
		angle = 45, 
		hjust = 1, 
		size = 6, 
		),
		axis.text.y = element_text(size = 6),
		plot.title = element_text(size = 8),
		axis.title.x = element_text(size = 8), 
		axis.title.y = element_text(size = 8),
        legend.key.size = unit(0.3, "cm"), # Adjust size of legend symbols
        legend.spacing.x = unit(0.2, "cm"), # Adjust space between legend items
        legend.spacing.y = unit(0.2, "cm"), # Adjust space between legend items
		legend.position = c(1, 1.05), # Upper-right corner
		legend.justification = c(1, 1),
		legend.text = element_text(size = 6)) + 
    scale_x_discrete(limits = species_order) + # Set the order of species on the y-axis
    coord_cartesian(ylim = y_range) 
  # Add plot to the list
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