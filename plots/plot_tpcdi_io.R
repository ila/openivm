#!/usr/bin/env Rscript
# Plot comparable physical I/O counters from an ivm-bench GCI artifact.

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
  file.path(paper_root, "data", "ivm_bench_sf100_io_run_30799417478.csv")
}
output_dir <- if (length(args) >= 2) args[2] else file.path(paper_root, "output")

io <- read.csv(input_csv, check.names = FALSE, stringsAsFactors = FALSE)
required_columns <- c(
  "engine", "display_name", "batch", "input_gb", "shuffle_read_gb",
  "shuffle_write_gb", "telemetry_status"
)
if (!all(required_columns %in% names(io))) {
  stop("I/O CSV must contain: ", paste(required_columns, collapse = ", "))
}

engine_levels <- c(
  "Databricks Enzyme", "Spark", "Spark-OpenIVM", "DuckDB",
  "DuckDB-OpenIVM", "Feldera", "Fabric", "Fabric-OpenIVM"
)

io <- io %>%
  mutate(
    display_name = factor(display_name, levels = rev(engine_levels)),
    batch_label = factor(paste("Batch", batch), levels = paste("Batch", 1:3)),
    total_io_gb = input_gb + shuffle_read_gb + shuffle_write_gb
  )

measured <- io %>% filter(telemetry_status == "measured")
unavailable <- io %>%
  filter(telemetry_status != "measured") %>%
  distinct(display_name, telemetry_status) %>%
  mutate(
    label = if_else(
      telemetry_status == "not_comparable",
      "ingest only",
      "not exposed"
    )
  )

batch_colors <- c("Batch 1" = "#3498db", "Batch 2" = "#e67e22", "Batch 3" = "#2ecc71")
axis_limit <- max(measured$total_io_gb) * 1.16

paper_theme <- theme_bw(base_size = 16, base_family = base_font) +
  theme(
    panel.border = element_rect(linewidth = 0.7),
    panel.grid.major.x = element_line(linewidth = 0.4),
    panel.grid.major.y = element_blank(),
    panel.grid.minor = element_blank(),
    axis.text.x = element_text(size = 9),
    axis.text.y = element_text(size = 9.5),
    axis.title.x = element_text(size = 10),
    axis.title.y = element_blank(),
    legend.position = "top",
    legend.direction = "horizontal",
    legend.text = element_text(size = 9.5),
    legend.box.spacing = grid::unit(2, "pt"),
    legend.margin = margin(0, 0, 0, 0),
    plot.margin = margin(2, 5, 2, 3)
  )

io_plot <- ggplot(measured, aes(x = total_io_gb, y = display_name, fill = batch_label)) +
  geom_col(
    width = 0.70,
    position = position_dodge(width = 0.76),
    color = "white",
    linewidth = 0.3
  ) +
  geom_text(
    aes(label = sprintf("%.0f", total_io_gb)),
    position = position_dodge(width = 0.76),
    hjust = -0.12,
    size = 3.0,
    family = base_font
  ) +
  geom_text(
    data = unavailable,
    aes(x = axis_limit * 0.03, y = display_name, label = label),
    inherit.aes = FALSE,
    hjust = 0,
    color = "#777777",
    fontface = "italic",
    size = 3.1,
    family = base_font
  ) +
  scale_fill_manual(values = batch_colors) +
  scale_x_continuous(
    limits = c(0, axis_limit),
    breaks = seq(0, 160, by = 40),
    expand = expansion(mult = c(0, 0))
  ) +
  guides(fill = guide_legend(title = NULL, nrow = 1)) +
  labs(x = "Reported physical I/O (GB)", y = NULL) +
  paper_theme

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
output_file <- file.path(output_dir, "tpcdi_sf100_reported_io.png")
ggsave(output_file, io_plot, width = 5.00, height = 3.05, dpi = 300, bg = "white")

message("font: ", base_font)
message("I/O plot saved to: ", output_file)
