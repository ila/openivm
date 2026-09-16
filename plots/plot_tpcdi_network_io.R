#!/usr/bin/env Rscript
# Estimate per-batch Net In/Net Out from ivm-bench's cumulative Docker counters.
# Counter values are interpolated at the recorded batch wall-clock boundaries
# and summed across the containers belonging to each engine. These counters do
# not represent remote-engine disk I/O.

script_file <- sub("^--file=", "", grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)[1])
script_dir <- dirname(normalizePath(script_file))
source(file.path(script_dir, "plot_common.R"))
RequirePlotPackages(c("ggplot2", "dplyr", "jsonlite", "systemfonts", "tidyr"))

suppressPackageStartupMessages({
  library(ggplot2)
  library(dplyr)
  library(jsonlite)
  library(tidyr)
})

base_font <- PaperFont()
paper_root <- script_dir
args <- commandArgs(trailingOnly = TRUE)
input_dir <- if (length(args) >= 1) args[1] else {
  stop("Usage: plot_tpcdi_network_io.R <artifact>/mount/stats/<sf> [output-dir]")
}
output_dir <- if (length(args) >= 2) args[2] else file.path(paper_root, "output")

stats_files <- list.files(
  input_dir,
  pattern = "^container_stats\\.jsonl$",
  recursive = TRUE,
  full.names = TRUE
)
if (length(stats_files) == 0) {
  stop("No container_stats.jsonl files found below: ", input_dir)
}

ReadStats <- function(path) {
  connection <- file(path, open = "r")
  on.exit(close(connection))
  stream_in(connection, verbose = FALSE) %>%
    mutate(engine = basename(dirname(path)))
}

stats <- bind_rows(lapply(stats_files, ReadStats))
required_columns <- c(
  "timestamp_s", "container", "net_in_mb", "net_out_mb", "engine"
)
if (!all(required_columns %in% names(stats))) {
  stop("Container stats must contain: ", paste(required_columns, collapse = ", "))
}

mount_dir <- dirname(dirname(input_dir))
batch_results_path <- file.path(
  mount_dir, "results", basename(input_dir), "dbt-server", "benchmark-results.json"
)
if (!file.exists(batch_results_path)) {
  stop("Batch timing file not found: ", batch_results_path)
}
batch_results <- fromJSON(batch_results_path, simplifyVector = FALSE)
batch_windows <- bind_rows(lapply(names(batch_results$engines), function(engine) {
  bind_rows(lapply(batch_results$engines[[engine]]$batches, function(batch) {
    tibble(
      engine = engine,
      batch_num = batch$batch_num,
      start_s = batch$extra$wall_window_start_ms / 1000,
      end_s = batch$extra$wall_window_end_ms / 1000
    )
  }))
}))

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
engine_levels <- c(
  "DuckDB", "Spark", "DuckDB-OpenIVM", "Spark-OpenIVM",
  "Feldera", "Databricks Enzyme", "Fabric", "Fabric-OpenIVM"
)
direction_levels <- c("Received", "Transmitted")

InterpolateCounter <- function(timestamp_s, cumulative_mb, boundary_s) {
  approx(
    timestamp_s,
    cumulative_mb,
    xout = boundary_s,
    method = "linear",
    rule = 2,
    ties = "ordered"
  )$y
}

network_io <- stats %>%
  arrange(engine, container, timestamp_s) %>%
  pivot_longer(
    cols = c(net_in_mb, net_out_mb),
    names_to = "direction",
    values_to = "counter_mb"
  ) %>%
  group_by(engine, container, direction) %>%
  mutate(
    cumulative_mb = cumsum(pmax(
      counter_mb - lag(counter_mb, default = first(counter_mb)),
      0
    ))
  ) %>%
  ungroup() %>%
  inner_join(batch_windows, by = "engine", relationship = "many-to-many") %>%
  group_by(engine, container, direction, batch_num) %>%
  summarise(
    megabytes = max(
      InterpolateCounter(timestamp_s, cumulative_mb, first(end_s)) -
        InterpolateCounter(timestamp_s, cumulative_mb, first(start_s)),
      0
    ),
    start_clamped = first(start_s) < min(timestamp_s),
    end_clamped = first(end_s) > max(timestamp_s),
    .groups = "drop"
  ) %>%
  group_by(engine, direction, batch_num) %>%
  summarise(
    megabytes = sum(megabytes),
    boundary_clamped = any(start_clamped | end_clamped),
    .groups = "drop"
  ) %>%
  mutate(
    display_name = factor(
      unname(display_names[engine]),
      levels = rev(engine_levels)
    ),
    direction = recode(
      direction,
      net_in_mb = "Received",
      net_out_mb = "Transmitted"
    ),
    direction = factor(direction, levels = direction_levels),
    batch = factor(
      paste("Batch", batch_num),
      levels = paste("Batch", 1:3)
    )
  ) %>%
  arrange(batch, display_name, direction)

if (any(is.na(network_io$display_name))) {
  stop("Missing display name for one or more engines")
}

engine_colors <- c(
  "DuckDB" = "#b8ad00",
  "Spark" = "#da571b",
  "DuckDB-OpenIVM" = "#4a90d9",
  "Spark-OpenIVM" = "#27ae60",
  "Feldera" = "#ca46bf",
  "Databricks Enzyme" = "#c0392b",
  "Fabric" = "#8e44ad",
  "Fabric-OpenIVM" = "#16a085"
)

paper_theme <- theme_bw(base_size = 16, base_family = base_font) +
  theme(
    panel.border = element_rect(linewidth = 0.7),
    panel.grid.major = element_line(linewidth = 0.3),
    panel.grid.minor = element_blank(),
    axis.text.x = element_text(size = 8),
    axis.text.y = element_text(size = 8),
    axis.title.x = element_text(size = 9),
    axis.title.y = element_blank(),
    strip.text = element_text(size = 9),
    legend.position = "top",
    legend.direction = "horizontal",
    legend.text = element_text(size = 8),
    legend.box.spacing = grid::unit(2, "pt"),
    legend.margin = margin(0, 0, 0, 0),
    plot.margin = margin(2, 3, 2, 3)
  )

io_plot <- ggplot(
  network_io,
  aes(
    x = megabytes,
    y = display_name,
    fill = display_name,
    alpha = direction
  )
) +
  geom_col(
    width = 0.68,
    position = position_dodge(width = 0.74),
    color = "white",
    linewidth = 0.2
  ) +
  facet_wrap(
    vars(batch),
    nrow = 1,
    scales = "free_x"
  ) +
  scale_fill_manual(values = engine_colors, guide = "none") +
  scale_alpha_manual(
    values = c("Received" = 1, "Transmitted" = 0.45),
    drop = FALSE
  ) +
  guides(alpha = guide_legend(title = NULL, nrow = 1)) +
  labs(x = "Sampled container network traffic (MB)", y = NULL) +
  paper_theme

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
output_file <- file.path(output_dir, "tpcdi_sf100_container_network_io.png")
data_file <- file.path(output_dir, "tpcdi_sf100_container_network_io.csv")
ggsave(output_file, io_plot, width = 7.2, height = 3.15, dpi = 300, bg = "white")
write.csv(network_io, data_file, row.names = FALSE)

message("font: ", base_font)
message("Network I/O plot saved to: ", output_file)
message("Per-batch data saved to: ", data_file)
