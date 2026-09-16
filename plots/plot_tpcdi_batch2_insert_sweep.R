#!/usr/bin/env Rscript

script_file <- sub("^--file=", "", grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)[1])
script_dir <- dirname(normalizePath(script_file))
source(file.path(script_dir, "plot_common.R"))
RequirePlotPackages(c("ggplot2", "dplyr", "systemfonts"))

suppressPackageStartupMessages({
  library(ggplot2)
  library(dplyr)
})

base_font <- PaperFont()
paper_root <- script_dir
args <- commandArgs(trailingOnly = TRUE)
input_csv <- if (length(args) >= 1) args[1] else {
  file.path(paper_root, "data", "tpcdi_sf100_batch2_insert_sweep_run_33253661718.csv")
}
output_file <- if (length(args) >= 2) args[2] else {
  file.path(paper_root, "output", "tpcdi_sf100_batch2_insert_sweep.png")
}

display_names <- c(
  "databricks-enzyme" = "Enzyme",
  "duckdb" = "DuckDB",
  "duckdb-openivm" = "OpenIVM",
  "fabric-jvm-35" = "Fabric",
  "fabric-openivm-jvm-35" = "Fabric-OpenIVM",
  "feldera" = "Feldera",
  "spark" = "Spark",
  "spark-openivm" = "Spark-OpenIVM"
)
engine_colors <- c(
  "databricks-enzyme" = "#009E73",
  "duckdb" = "#4D4D4D",
  "duckdb-openivm" = "#E69F00",
  "fabric-jvm-35" = "#56B4E9",
  "fabric-openivm-jvm-35" = "#CC79A7",
  "feldera" = "#9467bd",
  "spark" = "#0072B2",
  "spark-openivm" = "#D55E00"
)

timings <- read.csv(input_csv, stringsAsFactors = FALSE) %>%
  mutate(
    display_name = factor(
      unname(display_names[engine]),
      levels = unname(display_names)
    )
  )

# Retain explicit missing sweep points so failed experiments break the lines.

plot <- ggplot(
  timings,
  aes(x = insertion_pct, y = seconds, color = display_name, group = display_name)
) +
  geom_line(linewidth = 0.75) +
  geom_point(size = 1.65) +
  scale_color_manual(
    values = setNames(unname(engine_colors), unname(display_names)),
    breaks = unname(display_names)
  ) +
  scale_x_continuous(
    breaks = seq(5, 50, by = 5),
    limits = c(5, 50),
    expand = expansion(mult = c(0.02, 0.02))
  ) +
  scale_y_continuous(
    breaks = seq(0, 1000, by = 200),
    labels = function(values) format(values, big.mark = ",", scientific = FALSE),
    limits = c(0, 1050),
    expand = expansion(mult = c(0, 0.02))
  ) +
  labs(x = "Batch 2 insertions (%)", y = "Runtime (seconds)") +
  theme_bw(base_size = 10, base_family = base_font) +
  theme(
    legend.position = "top",
    legend.title = element_blank(),
    legend.text = element_text(size = 7.2),
    legend.key.width = grid::unit(11, "pt"),
    legend.key.height = grid::unit(7, "pt"),
    legend.spacing.x = grid::unit(2, "pt"),
    legend.margin = margin(0, 0, 1, 0),
    legend.box.spacing = grid::unit(1, "pt"),
    panel.border = element_rect(linewidth = 0.6),
    panel.grid.major = element_line(linewidth = 0.3),
    panel.grid.minor = element_blank(),
    axis.text = element_text(size = 8),
    axis.title = element_text(size = 9),
    plot.margin = margin(2, 3, 2, 3)
  ) +
  guides(color = guide_legend(nrow = 2, byrow = TRUE))

dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)
ggsave(output_file, plot, width = 5.0, height = 2.75, dpi = 300, bg = "white")
message("Plot saved to: ", output_file)
