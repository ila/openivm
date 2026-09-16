#!/usr/bin/env Rscript
script_file <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE)[1])
script_dir <- dirname(normalizePath(script_file))
source(file.path(script_dir, "plot_common.R"))
RequirePlotPackages(c("ggplot2", "dplyr"))
suppressPackageStartupMessages({ library(ggplot2); library(dplyr) })
args <- commandArgs(TRUE)
input <- if (length(args) >= 1) args[1] else file.path(script_dir, "data", "tpcdi_augmented_sf100_repetitions.csv")
output <- if (length(args) >= 2) args[2] else file.path(script_dir, "output")
dir.create(output, recursive = TRUE, showWarnings = FALSE)
raw <- read.csv(input, stringsAsFactors = FALSE)
stopifnot(all(raw$scale_factor == 100), all(raw$batch_num == 2),
          !anyDuplicated(raw[c("window_pct", "engine", "repetition")]))
engines <- c("duckdb", "duckdb-openivm", "spark", "spark-openivm", "fabric-jvm-35",
             "fabric-openivm-jvm-35", "databricks-enzyme", "feldera")
families <- c("DuckDB", "DuckDB", "Spark", "Spark", "Fabric", "Fabric", "Databricks", "Feldera")
methods <- c("Full refresh", "OpenIVM", "Full refresh", "OpenIVM", "Full refresh", "OpenIVM", "Enzyme (AUTO)", "Feldera")
summary <- raw %>% group_by(window_pct, engine) %>%
  summarise(n = n(), low = if (n() == 2) min(seconds) else NA_real_,
            high = if (n() == 2) max(seconds) else NA_real_,
            seconds = if (n() == 2) mean(seconds) else NA_real_, .groups = "drop") %>%
  right_join(expand.grid(window_pct = c(5, 15, 25, 35, 45), engine = engines), by = c("window_pct", "engine")) %>%
  mutate(n = coalesce(n, 0L), family = factor(families[match(engine, engines)], levels = unique(families)),
         method = factor(methods[match(engine, engines)], levels = unique(methods)))
stopifnot(all(summary$n <= 2), is.na(summary$seconds[summary$engine == "databricks-enzyme" & summary$window_pct == 35]))
write.csv(summary, file.path(output, "tpcdi_augmented_sf100_summary.csv"), row.names = FALSE)
# Layout-only interpolation, deliberately separate from measurements and ratios.
neighbors <- summary %>% filter(engine == "databricks-enzyme", window_pct %in% c(25, 45)) %>% arrange(window_pct)
stopifnot(nrow(neighbors) == 2, all(neighbors$n == 2))
estimate <- data.frame(window_pct = c(25, 35, 45),
                       seconds = c(neighbors$seconds[1], mean(neighbors$seconds), neighbors$seconds[2]),
                       family = factor("Databricks", levels = levels(summary$family)))
write.csv(data.frame(window_pct = 35, engine = "databricks-enzyme", seconds = estimate$seconds[2],
                     status = "INTERPOLATED_PLACEHOLDER_NOT_A_MEASUREMENT"),
          file.path(output, "tpcdi_augmented_sf100_placeholders.csv"), row.names = FALSE)
theme_paper <- theme_bw(base_size = 9, base_family = PaperFont()) + theme(
  legend.position = "top", legend.title = element_blank(),
  legend.key.width = grid::unit(13, "pt"), legend.key.height = grid::unit(7, "pt"),
  legend.box.spacing = grid::unit(5, "pt"), legend.margin = margin(0, 0, 0, 0),
  panel.grid.minor = element_blank(), strip.background = element_rect(fill = "grey96"),
  plot.margin = margin(3, 4, 3, 3))
colors <- c("Full refresh" = "#4D4D4D", "OpenIVM" = "#D55E00", "Enzyme (AUTO)" = "#009E73", "Feldera" = "#9467BD")
p <- ggplot(summary, aes(window_pct, seconds, color = method, group = method)) +
  geom_line(linewidth = 0.55, na.rm = TRUE) +
  geom_errorbar(aes(ymin = low, ymax = high), width = 1.4, linewidth = 0.35, na.rm = TRUE) +
  geom_point(size = 1.6, na.rm = TRUE) + facet_wrap(~family, ncol = 3) +
  geom_line(data = estimate, aes(window_pct, seconds), inherit.aes = FALSE, color = "#009E73", linetype = "dashed", linewidth = 0.55) +
  geom_point(data = estimate[2, ], aes(window_pct, seconds), inherit.aes = FALSE, shape = 21, fill = "white", color = "#009E73", size = 2.1) +
  geom_text(data = estimate[2, ], aes(window_pct, seconds, label = "TODO: estimate"), inherit.aes = FALSE,
            vjust = -1.2, size = 2.3, family = PaperFont()) +
  scale_color_manual(values = colors, drop = FALSE) +
  scale_x_continuous(breaks = c(5, 15, 25, 35, 45)) +
  scale_y_continuous(limits = c(0, NA), expand = expansion(mult = c(0, 0.07))) +
  labs(x = "Share of augmented 365-day window (%)", y = "Batch 2 runtime (seconds)") + theme_paper
ggsave(file.path(output, "tpcdi_augmented_sf100_runtime.pdf"), p, width = 7.05, height = 3.2, device = cairo_pdf)
ggsave(file.path(output, "tpcdi_augmented_sf100_runtime.png"), p, width = 7.05, height = 3.2, dpi = 400)
relative <- inner_join(
  summary %>% filter(method == "Full refresh") %>% select(window_pct, family, full_seconds = seconds),
  summary %>% filter(method == "OpenIVM") %>% select(window_pct, family, ivm_seconds = seconds),
  by = c("window_pct", "family")) %>% mutate(speedup = full_seconds / ivm_seconds)
write.csv(relative, file.path(output, "tpcdi_augmented_sf100_speedup.csv"), row.names = FALSE)
p <- ggplot(relative, aes(window_pct, speedup, color = family, group = family)) +
  geom_hline(yintercept = 1, linetype = "dashed", color = "grey50") +
  geom_line(linewidth = 0.65) + geom_point(size = 1.6) +
  scale_color_manual(values = c(DuckDB = "#E69F00", Spark = "#0072B2", Fabric = "#CC79A7")) +
  scale_x_continuous(breaks = c(5, 15, 25, 35, 45)) +
  labs(x = "Share of augmented window (%)", y = "Full / OpenIVM runtime") + theme_paper
ggsave(file.path(output, "tpcdi_augmented_sf100_speedup.pdf"), p, width = 3.4, height = 2.05, device = cairo_pdf)
p <- ggplot() + xlim(0, 1) + ylim(0, 1) +
  annotate("rect", xmin = 0.01, xmax = 0.99, ymin = 0.03, ymax = 0.97, fill = "grey97", color = "grey65") +
  annotate("text", x = 0.5, y = 0.68, label = "TODO: mixed-DML evaluation", family = PaperFont(), size = 3.4) +
  annotate("text", x = 0.5, y = 0.39, label = "Insert / update / delete mixtures\nMatched change budgets; results pending", family = PaperFont(), size = 3) +
  theme_void() + theme(plot.margin = margin(0, 0, 0, 0))
ggsave(file.path(output, "tpcdi_mixed_workload_placeholder.pdf"), p, width = 3.4, height = 1.15, device = cairo_pdf)
