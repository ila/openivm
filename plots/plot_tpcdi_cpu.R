#!/usr/bin/env Rscript
# Estimate per-batch local-container CPU work from ivm-bench samples. Aggregate
# CPU percentage is integrated over each recorded wall-clock batch window and
# reported as core-minutes. Remote cloud-engine CPU is not observable here.

script_file <- sub("^--file=", "", grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)[1])
script_dir <- dirname(normalizePath(script_file))
source(file.path(script_dir, "plot_common.R"))
RequirePlotPackages(c("ggplot2", "dplyr", "jsonlite", "systemfonts"))

suppressPackageStartupMessages({
  library(ggplot2)
  library(dplyr)
  library(jsonlite)
})

base_font <- PaperFont()
paper_root <- script_dir
args <- commandArgs(trailingOnly = TRUE)
input_dir <- if (length(args) >= 1) args[1] else {
  stop("Usage: plot_tpcdi_cpu.R <artifact>/mount/stats/<sf> [output-dir]")
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
required_columns <- c("timestamp_s", "container", "cpu_pct", "engine")
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

IntegrateCoreMinutes <- function(timestamp_s, cpu_pct, start_s, end_s) {
  inner <- timestamp_s[timestamp_s > start_s & timestamp_s < end_s]
  integration_x <- c(start_s, inner, end_s)
  integration_y <- approx(
    timestamp_s,
    cpu_pct,
    xout = integration_x,
    method = "linear",
    rule = 2,
    ties = "ordered"
  )$y
  sum(diff(integration_x) * (head(integration_y, -1) + tail(integration_y, -1)) / 2) / 100 / 60
}

cpu_work <- stats %>%
  group_by(engine, timestamp_s) %>%
  summarise(cpu_pct = sum(cpu_pct), .groups = "drop") %>%
  inner_join(batch_windows, by = "engine", relationship = "many-to-many") %>%
  group_by(engine, batch_num) %>%
  summarise(
    core_minutes = IntegrateCoreMinutes(
      timestamp_s,
      cpu_pct,
      first(start_s),
      first(end_s)
    ),
    boundary_clamped = first(start_s) < min(timestamp_s) | first(end_s) > max(timestamp_s),
    .groups = "drop"
  ) %>%
  mutate(
    display_name = factor(
      unname(display_names[engine]),
      levels = rev(engine_levels)
    ),
    batch = factor(
      paste("Batch", batch_num),
      levels = paste("Batch", 1:3)
    )
  ) %>%
  arrange(batch, display_name)

if (any(is.na(cpu_work$display_name))) {
  stop("Missing display name for one or more engines")
}

paper_theme <- theme_bw(base_size = 16, base_family = base_font) +
  theme(
    panel.border = element_rect(linewidth = 0.7),
    panel.grid.major.x = element_line(linewidth = 0.3),
    panel.grid.major.y = element_blank(),
    panel.grid.minor = element_blank(),
    axis.text.x = element_text(size = 8),
    axis.text.y = element_text(size = 8),
    axis.title.x = element_text(size = 9),
    axis.title.y = element_blank(),
    strip.text = element_text(size = 9),
    plot.margin = margin(2, 3, 2, 3)
  )

cpu_plot <- ggplot(
  cpu_work,
  aes(x = core_minutes, y = display_name, fill = display_name)
) +
  geom_col(width = 0.62, color = "white", linewidth = 0.2) +
  facet_wrap(vars(batch), nrow = 1, scales = "free_x") +
  scale_fill_manual(values = engine_colors, guide = "none") +
  labs(x = "Sampled local-container CPU work (core-minutes)", y = NULL) +
  paper_theme

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
output_file <- file.path(output_dir, "tpcdi_sf100_container_cpu.png")
data_file <- file.path(output_dir, "tpcdi_sf100_container_cpu.csv")
ggsave(output_file, cpu_plot, width = 7.2, height = 3.15, dpi = 300, bg = "white")
write.csv(cpu_work, data_file, row.names = FALSE)

message("font: ", base_font)
message("CPU plot saved to: ", output_file)
message("Per-batch data saved to: ", data_file)
