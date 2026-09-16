#!/usr/bin/env Rscript
# Plot TPC-DI batch timings emitted by an ivm-bench GCI artifact.

script_file <- sub("^--file=", "", grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)[1])
script_dir <- dirname(normalizePath(script_file))
source(file.path(script_dir, "plot_common.R"))
RequirePlotPackages(c("ggplot2", "dplyr", "systemfonts"))

suppressPackageStartupMessages({
  library(ggplot2)
  library(dplyr)
})

base_font <- PaperFont()
args <- commandArgs(trailingOnly = TRUE)
input_csv <- if (length(args) >= 1) args[1] else {
  file.path(script_dir, "data", "ivm_bench_sf100_storage_run_30634033704.csv")
}
output_dir <- if (length(args) >= 2) args[2] else file.path(script_dir, "output")
scale_factor <- if (length(args) >= 3) {
  as.integer(args[3])
} else if (grepl("sf100", basename(input_csv), fixed = TRUE)) {
  100L
} else {
  10L
}

timings <- read.csv(input_csv, check.names = FALSE, stringsAsFactors = FALSE)

if ("batch_num" %in% names(timings)) {
  if (any(timings$batch_status != "completed")) {
    stop("Every plotted batch must have status=completed")
  }
  display_names <- c(
    "databricks-enzyme" = "Databricks Enzyme",
    "duckdb" = "DuckDB",
    "duckdb-openivm" = "DuckDB-OpenIVM",
    "fabric-jvm-35" = "Fabric",
    "fabric-openivm-jvm-35" = "Fabric-OpenIVM",
    "feldera" = "Feldera",
    "spark" = "Spark",
    "spark-openivm" = "Spark-OpenIVM"
  )
  timings_long <- timings %>%
    transmute(
      engine,
      display_name = unname(display_names[engine]),
      batch = factor(paste("Batch", batch_num), levels = paste("Batch", 1:3)),
      seconds = duration_s
    )
} else {
  required_columns <- c(
    "engine", "display_name", "batch_1_seconds", "batch_2_seconds", "batch_3_seconds"
  )
  if (!all(required_columns %in% names(timings))) {
    stop("Timing CSV must contain: ", paste(required_columns, collapse = ", "))
  }
  timings_long <- bind_rows(lapply(seq_len(nrow(timings)), function(row_index) {
    row <- timings[row_index, ]
    tibble(
      engine = row$engine,
      display_name = row$display_name,
      batch = factor(c("Batch 1", "Batch 2", "Batch 3"), levels = c("Batch 1", "Batch 2", "Batch 3")),
      seconds = c(row$batch_1_seconds, row$batch_2_seconds, row$batch_3_seconds)
    )
  }))
}

timings_long <- timings_long %>%
  mutate(
    rounded_seconds = as.integer(round(seconds)),
    duration = if_else(
      rounded_seconds >= 60,
      sprintf("%d:%02d", rounded_seconds %/% 60, rounded_seconds %% 60),
      sprintf("%ds", rounded_seconds)
    )
  )

engine_totals <- timings_long %>%
  group_by(engine, display_name) %>%
  summarise(total_seconds = sum(seconds), .groups = "drop") %>%
  arrange(desc(total_seconds)) %>%
  mutate(
    rounded_total_seconds = as.integer(round(total_seconds)),
    total_duration = sprintf(
      "%d:%02d",
      rounded_total_seconds %/% 60,
      rounded_total_seconds %% 60
    ),
    display_name = factor(display_name, levels = display_name)
  )

timings_long <- timings_long %>%
  left_join(engine_totals %>% select(engine, total_seconds), by = "engine") %>%
  arrange(engine, batch) %>%
  group_by(engine) %>%
  mutate(
    segment_end = cumsum(seconds),
    segment_midpoint = segment_end - seconds / 2
  ) %>%
  ungroup() %>%
  mutate(display_name = factor(display_name, levels = levels(engine_totals$display_name)))

global_max <- max(engine_totals$total_seconds)
axis_limit <- global_max * 1.30
label_offset <- global_max * 0.012
batch_colors <- c("Batch 1" = "#3498db", "Batch 2" = "#e67e22", "Batch 3" = "#2ecc71")

paper_theme <- theme_bw(base_size = 16, base_family = base_font) +
  theme(
    legend.position = "top",
    legend.direction = "horizontal",
    legend.text = element_text(size = 10),
    legend.box.spacing = grid::unit(4, "pt"),
    legend.margin = margin(0, 0, 0, 0),
    panel.border = element_rect(linewidth = 0.7),
    panel.grid.major.x = element_line(linewidth = 0.4),
    panel.grid.major.y = element_blank(),
    panel.grid.minor = element_blank(),
    axis.text.x = element_text(size = 9),
    axis.text.y = element_text(size = 9.5),
    axis.title.x = element_text(size = 10),
    axis.title.y = element_blank(),
    plot.margin = margin(2, 5, 2, 3)
  )

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
combined_plot <- ggplot(timings_long, aes(x = seconds, y = display_name, fill = batch)) +
  geom_col(
    width = 0.64,
    color = "white",
    linewidth = 0.35,
    position = position_stack(reverse = TRUE)
  ) +
  geom_text(
    aes(
      x = segment_midpoint,
      label = if_else(seconds >= global_max * 0.075, duration, "")
    ),
    color = "white",
    fontface = "bold",
    size = 3.2,
    family = base_font
  ) +
  geom_text(
    data = engine_totals,
    aes(
      x = total_seconds + label_offset,
      y = display_name,
      label = paste0("Total ", total_duration)
    ),
    inherit.aes = FALSE,
    hjust = 0,
    size = 3.3,
    family = base_font
  ) +
  scale_fill_manual(values = batch_colors) +
  scale_x_continuous(
    limits = c(0, axis_limit),
    breaks = seq(0, ceiling(axis_limit / 600) * 600, by = 600),
    labels = function(values) ifelse(values == 0, "0", values / 60),
    expand = expansion(mult = c(0, 0))
  ) +
  guides(fill = guide_legend(title = NULL, nrow = 1)) +
  labs(x = "Runtime (minutes)", y = NULL) +
  paper_theme

output_file <- file.path(
  output_dir,
  sprintf("tpcdi_sf%d_batch_duration_by_engine.png", scale_factor)
)
ggsave(output_file, combined_plot, width = 5.00, height = 3.10, dpi = 300, bg = "white")
message("Plot saved to: ", output_file)

message("font: ", base_font)
