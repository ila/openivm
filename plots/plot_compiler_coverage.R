#!/usr/bin/env Rscript
# Plot incrementalization coverage over the common 500-query compiler-bench slice.

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
  file.path(paper_root, "data", "compiler_bench_coverage_500.csv")
}
output_dir <- if (length(args) >= 2) args[2] else file.path(paper_root, "output")

coverage <- read.csv(input_csv, check.names = FALSE, stringsAsFactors = FALSE) %>%
  mutate(
    percentage = 100 * incrementalizable / total,
    label = sprintf("%d / %d  (%.1f%%)", incrementalizable, total, percentage),
    engine = factor(
      engine,
      levels = c("Databricks Enzyme", "DuckDB-OpenIVM", "Feldera")
    )
  )

engine_colors <- c(
  "DuckDB-OpenIVM" = "#4a90d9",
  "Feldera" = "#ca46bf",
  "Databricks Enzyme" = "#c0392b"
)

coverage_plot <- ggplot(coverage, aes(x = percentage, y = engine, fill = engine)) +
  geom_col(width = 0.62, color = "white", linewidth = 0.35) +
  geom_text(
    aes(x = percentage / 2, label = label),
    color = "white",
    fontface = "bold",
    size = 3.4,
    family = base_font
  ) +
  scale_fill_manual(values = engine_colors, guide = "none") +
  scale_x_continuous(
    limits = c(0, 100),
    breaks = seq(0, 100, by = 20),
    labels = function(values) paste0(values, "%"),
    expand = expansion(mult = c(0, 0))
  ) +
  labs(x = "Incrementalizable queries", y = NULL) +
  theme_bw(base_size = 16, base_family = base_font) +
  theme(
    panel.border = element_rect(linewidth = 0.7),
    panel.grid.major.x = element_line(linewidth = 0.4),
    panel.grid.major.y = element_blank(),
    panel.grid.minor = element_blank(),
    axis.text.x = element_text(size = 9),
    axis.text.y = element_text(size = 9.5),
    axis.title.x = element_text(size = 10),
    plot.margin = margin(2, 5, 2, 3)
  )

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
output_file <- file.path(output_dir, "compiler_bench_incrementalizable_500.png")
ggsave(output_file, coverage_plot, width = 5.00, height = 1.65, dpi = 300, bg = "white")
message("Plot saved to: ", output_file)
message("font: ", base_font)
