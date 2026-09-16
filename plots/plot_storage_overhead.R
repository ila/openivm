#!/usr/bin/env Rscript
# Plot storage measurements emitted by an ivm-bench GCI artifact.

script_file <- sub("^--file=", "", grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)[1])
script_dir <- dirname(normalizePath(script_file))
source(file.path(script_dir, "plot_common.R"))
RequirePlotPackages(c("ggplot2", "dplyr", "scales", "systemfonts", "tidyr"))

suppressPackageStartupMessages({
  library(ggplot2)
  library(dplyr)
  library(scales)
  library(tidyr)
})

base_font <- PaperFont()
paper_root <- script_dir
args <- commandArgs(trailingOnly = TRUE)
input_csv <- if (length(args) >= 1) args[1] else {
  file.path(paper_root, "data", "ivm_bench_sf10_storage_run_30623326942.csv")
}
output_dir <- if (length(args) >= 2) args[2] else file.path(paper_root, "output")
scale_factor <- if (length(args) >= 3) {
  as.integer(args[3])
} else if (grepl("sf100_", basename(input_csv), fixed = TRUE)) {
  100L
} else {
  10L
}

storage <- read.csv(input_csv, check.names = FALSE, stringsAsFactors = FALSE)

if ("batch_num" %in% names(storage)) {
  if (any(storage$storage_status != "ok")) {
    stop("Every plotted storage snapshot must have status=ok")
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
  storage <- storage %>%
    transmute(
      engine,
      display_name = unname(display_names[engine]),
      batch = batch_num,
      visible_output_bytes,
      helper_data_bytes,
      metadata_bytes,
      source_bytes,
      total_bytes,
      source_mode = base_table_source_mode,
      shared_source_bytes = if_else(source_mode == "shared_raw", base_table_bytes, 0),
      state_observability = case_when(
        engine == "databricks-enzyme" ~ "inseparable",
        engine %in% c(
          "duckdb-openivm", "spark-openivm", "fabric-openivm-jvm-35", "feldera"
        ) ~ "observable",
        TRUE ~ "not_applicable"
      )
    )
}

storage <- storage %>%
  mutate(
    required_storage_bytes = total_bytes + shared_source_bytes,
    batch_label = factor(paste("Batch", batch), levels = paste("Batch", 1:3))
  )

required_columns <- c(
  "engine", "display_name", "batch", "visible_output_bytes", "helper_data_bytes",
  "metadata_bytes", "source_bytes", "total_bytes", "shared_source_bytes",
  "state_observability"
)
if (!all(required_columns %in% names(storage))) {
  stop("Storage CSV must contain: ", paste(required_columns, collapse = ", "))
}

pair_definitions <- tibble(
  family = c("DuckDB", "Spark", "Fabric"),
  ivm_engine = c("duckdb-openivm", "spark-openivm", "fabric-openivm-jvm-35"),
  baseline_engine = c("duckdb", "spark", "fabric-jvm-35")
) %>%
  filter(ivm_engine %in% storage$engine, baseline_engine %in% storage$engine)

paired <- bind_rows(lapply(seq_len(nrow(pair_definitions)), function(pair_index) {
  pair <- pair_definitions[pair_index, ]
  ivm <- storage %>%
    filter(engine == pair$ivm_engine) %>%
    select(batch, batch_label, ivm_bytes = required_storage_bytes)
  baseline <- storage %>%
    filter(engine == pair$baseline_engine) %>%
    select(batch, baseline_bytes = required_storage_bytes)

  inner_join(ivm, baseline, by = "batch") %>%
    mutate(
      family = pair$family,
      multiplier = ivm_bytes / baseline_bytes
    )
})) %>%
  mutate(family = factor(family, levels = c("DuckDB", "Spark", "Fabric")))

paper_theme <- theme_bw(base_size = 16, base_family = base_font) +
  theme(
    panel.border = element_rect(linewidth = 0.7),
    panel.grid.major.x = element_blank(),
    panel.grid.major.y = element_line(linewidth = 0.4),
    panel.grid.minor = element_blank(),
    axis.text = element_text(size = 9.5),
    axis.title = element_text(size = 10),
    legend.position = "top",
    legend.direction = "horizontal",
    legend.text = element_text(size = 9.5),
    legend.box.spacing = grid::unit(2, "pt"),
    legend.margin = margin(0, 0, 0, 0),
    plot.margin = margin(2, 5, 2, 3)
  )

family_colors <- c("DuckDB" = "#009900", "Spark" = "#3498db", "Fabric" = "#e67e22")

multiplier_plot <- ggplot(
  paired,
  aes(x = batch_label, y = multiplier, color = family, group = family)
) +
  geom_hline(yintercept = 1, color = "#555555", linewidth = 0.55, linetype = "dashed") +
  geom_line(linewidth = 0.9) +
  geom_point(size = 2.5) +
  geom_text(
    data = paired %>% filter(batch == 3),
    aes(label = sprintf("%.2f\u00d7", multiplier)),
    nudge_x = 0.13,
    hjust = 0,
    size = 3.1,
    family = base_font,
    show.legend = FALSE
  ) +
  scale_color_manual(values = family_colors) +
  scale_y_continuous(
    limits = c(0, 1.35),
    breaks = c(0, 0.5, 1, 1.25),
    labels = function(values) paste0(format(values, trim = TRUE), "\u00d7"),
    expand = expansion(mult = c(0, 0))
  ) +
  scale_x_discrete(expand = expansion(add = c(0.15, 0.42))) +
  guides(color = guide_legend(title = NULL, nrow = 1)) +
  labs(x = NULL, y = "Storage relative to full refresh") +
  paper_theme

ivm_engines <- c(
  "duckdb-openivm", "spark-openivm", "fabric-openivm-jvm-35",
  "feldera", "databricks-enzyme"
)
batch_one_storage <- storage %>%
  filter(batch == 1, engine %in% ivm_engines) %>%
  mutate(
    gigabytes = required_storage_bytes / 1e9,
    display_name = factor(display_name, levels = display_name[order(gigabytes)]),
    label = sprintf("%.1f GB", gigabytes)
  )

batch_one_components <- batch_one_storage %>%
  mutate(
    base_table_bytes = source_bytes + shared_source_bytes
  ) %>%
  select(
    engine, display_name, state_observability, label, gigabytes,
    base_table_bytes, visible_output_bytes, helper_data_bytes, metadata_bytes
  ) %>%
  pivot_longer(
    cols = c(
      base_table_bytes, visible_output_bytes, helper_data_bytes, metadata_bytes
    ),
    names_to = "component",
    values_to = "bytes"
  ) %>%
  mutate(
    component = recode(
      component,
      base_table_bytes = "Base tables",
      visible_output_bytes = "MVs",
      helper_data_bytes = "IVM state",
      metadata_bytes = "Metadata"
    ),
    component = factor(
      component,
      levels = c("Metadata", "IVM state", "MVs", "Base tables")
    ),
    component_gigabytes = bytes / 1e9
  )

storage_limit <- max(batch_one_storage$gigabytes) * 1.18
batch_one_plot <- ggplot(
  batch_one_components,
  aes(x = component_gigabytes, y = display_name, fill = component)
) +
  geom_col(width = 0.64, color = "white", linewidth = 0.3) +
  geom_text(
    data = batch_one_storage,
    aes(
      x = gigabytes + storage_limit * 0.015,
      y = display_name,
      label = label
    ),
    inherit.aes = FALSE,
    hjust = 0,
    size = 3.2,
    family = base_font
  ) +
  scale_fill_manual(values = c(
    "Base tables" = "#bdbdbd",
    "MVs" = "#4dff4d",
    "IVM state" = "#009900",
    "Metadata" = "#3498db"
  )) +
  scale_x_continuous(
    limits = c(0, storage_limit),
    breaks = breaks_pretty(n = 5),
    labels = function(values) paste0(values, " GB"),
    expand = expansion(mult = c(0, 0))
  ) +
  guides(fill = guide_legend(title = NULL, reverse = TRUE, nrow = 1)) +
  labs(x = "Total storage after Batch 1", y = NULL) +
  paper_theme +
  theme(
    legend.justification = "left",
    legend.box.just = "left",
    legend.margin = margin(0, 0, 0, -76),
    legend.text = element_text(size = 9.5),
    panel.grid.major.x = element_line(linewidth = 0.4),
    panel.grid.major.y = element_blank(),
    axis.title.y = element_blank(),
    axis.text.y = element_text(size = 9.5)
  )

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
multiplier_file <- file.path(
  output_dir,
  sprintf("tpcdi_sf%d_storage_multiplier.png", scale_factor)
)
batch_one_file <- file.path(
  output_dir,
  sprintf("tpcdi_sf%d_batch1_storage_gb.png", scale_factor)
)
ggsave(multiplier_file, multiplier_plot, width = 5.00, height = 2.65, dpi = 300, bg = "white")
ggsave(batch_one_file, batch_one_plot, width = 5.00, height = 2.75, dpi = 300, bg = "white")

write.csv(
  paired,
  file.path(output_dir, sprintf("tpcdi_sf%d_storage_multiplier.csv", scale_factor)),
  row.names = FALSE
)
write.csv(
  batch_one_components,
  file.path(output_dir, sprintf("tpcdi_sf%d_batch1_storage_gb.csv", scale_factor)),
  row.names = FALSE
)

message("font: ", base_font)
message("Storage multiplier plot saved to: ", multiplier_file)
message("Batch 1 storage plot saved to: ", batch_one_file)
