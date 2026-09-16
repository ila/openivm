#!/usr/bin/env Rscript

script_file <- sub("^--file=", "", grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)[1])
script_dir <- dirname(normalizePath(script_file))
source(file.path(script_dir, "plot_common.R"))
RequirePlotPackages(c("ggplot2", "dplyr", "scales", "systemfonts"))

suppressPackageStartupMessages({
  library(ggplot2)
  library(dplyr)
  library(scales)
})

base_font <- PaperFont()
args <- commandArgs(trailingOnly = TRUE)
repo_root <- if (length(args) >= 1) normalizePath(args[1]) else {
  stop("Usage: plot_fabric_characteristics.R <dbt-project> [output-prefix]")
}
output_prefix <- if (length(args) >= 2) {
  sub("[.]png$", "", args[2])
} else {
  file.path(script_dir, "output", "fabric_workload_characteristics")
}

model_dir <- file.path(repo_root, "models")
model_files <- sort(list.files(model_dir, pattern = "[.]sql$", recursive = TRUE, full.names = TRUE))
if (length(model_files) == 0) {
  stop("No dbt model SQL files found under: ", model_dir)
}

read_sql <- function(path) paste(readLines(path, warn = FALSE), collapse = "\n")
strip_sql_comments_and_literals <- function(sql) {
  sql <- gsub("(?s)/[*].*?[*]/", " ", sql, perl = TRUE)
  sql <- gsub("--[^\n]*", " ", sql, perl = TRUE)
  gsub("'(?:''|[^'])*'", "''", sql, perl = TRUE)
}
has_sql <- function(sql, pattern) grepl(pattern, sql, ignore.case = TRUE, perl = TRUE)
count_sql <- function(sql, pattern) {
  hits <- gregexpr(pattern, sql, ignore.case = TRUE, perl = TRUE)[[1]]
  if (hits[1] == -1) 0L else length(hits)
}
extract_refs <- function(sql) {
  pattern <- "\\bref\\s*\\(\\s*['\"][A-Za-z0-9_]+['\"]\\s*\\)"
  hits <- regmatches(sql, gregexpr(pattern, sql, ignore.case = TRUE, perl = TRUE))[[1]]
  literal_refs <- if (length(hits) == 0) character() else {
    sub(".*['\"]([A-Za-z0-9_]+)['\"].*", "\\1", hits, perl = TRUE)
  }
  concat_pattern <- "\\bref\\s*\\(\\s*'([A-Za-z0-9_]*)'\\s*~\\s*'([A-Za-z0-9_]*)'\\s*\\)"
  concat_hits <- regmatches(sql, gregexpr(concat_pattern, sql, ignore.case = TRUE, perl = TRUE))[[1]]
  concat_refs <- vapply(concat_hits, function(hit) {
    captures <- regmatches(hit, regexec(concat_pattern, hit, ignore.case = TRUE, perl = TRUE))[[1]]
    paste0(captures[2], captures[3])
  }, character(1))
  unique(c(literal_refs, concat_refs))
}

macro_files <- sort(list.files(file.path(repo_root, "macros"), pattern = "[.]sql$", full.names = TRUE))
macro_pattern <- paste0(
  "(?s)\\{%[-]?\\s*macro\\s+([A-Za-z_][A-Za-z0-9_]*)\\s*\\((.*?)\\)\\s*[-]?%\\}",
  "(.*?)\\{%[-]?\\s*endmacro\\s*[-]?%\\}"
)
macro_defs <- list()
for (macro_file in macro_files) {
  macro_sql <- read_sql(macro_file)
  blocks <- regmatches(macro_sql, gregexpr(macro_pattern, macro_sql, perl = TRUE))[[1]]
  for (block in blocks) {
    captures <- regmatches(block, regexec(macro_pattern, block, perl = TRUE))[[1]]
    params <- trimws(strsplit(captures[3], ",", fixed = TRUE)[[1]])
    params <- sub("=.*", "", params)
    params <- params[nzchar(params)]
    macro_defs[[captures[2]]] <- list(params = params, body = captures[4])
  }
}

split_args <- function(args) {
  if (!nzchar(trimws(args))) return(character())
  trimws(strsplit(args, ",", fixed = TRUE)[[1]])
}
expand_model_macros <- function(sql) {
  bodies <- character()
  for (macro_name in names(macro_defs)) {
    call_pattern <- paste0(
      "\\{\\{[-]?\\s*", macro_name,
      "\\s*\\((.*?)\\)\\s*[-]?\\}\\}"
    )
    calls <- regmatches(sql, gregexpr(call_pattern, sql, perl = TRUE))[[1]]
    for (call in calls) {
      captures <- regmatches(call, regexec(call_pattern, call, perl = TRUE))[[1]]
      args <- split_args(captures[2])
      body <- macro_defs[[macro_name]]$body
      params <- macro_defs[[macro_name]]$params
      for (i in seq_len(min(length(args), length(params)))) {
        if (grepl("^['\"][A-Za-z0-9_]+['\"]$", args[i])) {
          body <- gsub(
            paste0("\\b", params[i], "\\b"),
            args[i], body, perl = TRUE
          )
        }
      }
      if (macro_name == "arc_sql_vcores_ready_helix_query") {
        body <- paste(vapply(c("daily", "7d", "28d"), function(suffix) {
          gsub("\\bsuffix\\b", paste0("'", suffix, "'"), body, perl = TRUE)
        }, character(1)), collapse = "\n")
      }
      bodies <- c(bodies, body)
    }
  }
  paste(c(sql, bodies), collapse = "\n")
}

model_names <- tools::file_path_sans_ext(basename(model_files))
relative_paths <- substring(model_files, nchar(normalizePath(model_dir)) + 2)
type_dirs <- sub("/.*", "", relative_paths)
type_names <- c(
  staging = "Staging",
  intermediate = "Intermediate",
  dimensions = "Dimension",
  facts = "Fact",
  metadata = "Metadata"
)
model_types <- unname(type_names[type_dirs])
if (any(is.na(model_types))) {
  stop("Unknown model directory: ", paste(unique(type_dirs[is.na(model_types)]), collapse = ", "))
}

raw_sql <- setNames(lapply(model_files, read_sql), model_names)
expanded_sql <- lapply(raw_sql, expand_model_macros)
clean_sql <- lapply(expanded_sql, strip_sql_comments_and_literals)
join_counts <- vapply(clean_sql, count_sql, integer(1), pattern = "\\bjoin\\b")

feature_patterns <- c(
  "Filter" = "\\bwhere\\b",
  "Join" = "\\bjoin\\b",
  "Aggregate" = "\\b(?:count|sum|avg|min|max|stddev|variance)\\s*\\(",
  "Group by" = "\\bgroup\\s+by\\b",
  "Window" = "\\bover\\s*\\(|\\bwindow\\s+[A-Za-z_][A-Za-z0-9_]*\\s+as\\s*\\(",
  "Distinct" = "\\bdistinct\\b",
  "CTE" = "\\bwith\\s+(?:recursive\\s+)?[A-Za-z_][A-Za-z0-9_]*\\s+as\\s*\\(",
  "Outer join" = "\\b(?:left|right|full)(?:\\s+outer)?\\s+join\\b",
  "Union all" = "\\bunion\\s+all\\b",
  "Having" = "\\bhaving\\b"
)
feature_counts <- vapply(
  feature_patterns,
  function(pattern) sum(vapply(clean_sql, has_sql, logical(1), pattern = pattern)),
  integer(1)
)
feature_summary <- data.frame(
  feature = names(feature_counts),
  models = as.integer(feature_counts),
  percentage = as.integer(feature_counts) / length(model_names),
  stringsAsFactors = FALSE
) %>%
  arrange(percentage, models) %>%
  mutate(
    feature = factor(feature, levels = feature),
    label = sprintf("%d / %d", models, length(model_names))
  )

edge_rows <- lapply(model_names, function(model) {
  dependencies <- intersect(extract_refs(expanded_sql[[model]]), model_names)
  if (length(dependencies) == 0) return(NULL)
  data.frame(from = dependencies, to = model, stringsAsFactors = FALSE)
})
edges <- distinct(bind_rows(edge_rows), from, to)

parents <- split(edges$from, edges$to)
children <- split(edges$to, edges$from)
depth <- setNames(rep(NA_integer_, length(model_names)), model_names)
while (anyNA(depth)) {
  unresolved <- names(depth)[is.na(depth)]
  progress <- FALSE
  for (model in unresolved) {
    model_parents <- parents[[model]]
    if (is.null(model_parents) || length(model_parents) == 0) {
      depth[[model]] <- 0L
      progress <- TRUE
    } else if (all(!is.na(depth[model_parents]))) {
      depth[[model]] <- max(depth[model_parents]) + 1L
      progress <- TRUE
    }
  }
  if (!progress) stop("Model dependency graph contains a cycle")
}

descendants <- function(start) {
  seen <- character()
  frontier <- children[[start]]
  while (length(frontier) > 0) {
    frontier <- setdiff(frontier, seen)
    if (length(frontier) == 0) break
    seen <- c(seen, frontier)
    frontier <- unique(unlist(children[frontier], use.names = FALSE))
  }
  unique(seen)
}

fan_in <- table(factor(edges$to, levels = model_names))
fan_out <- table(factor(edges$from, levels = model_names))
blast_radius <- vapply(model_names, function(model) length(descendants(model)), integer(1))
type_levels <- c("Staging", "Intermediate", "Dimension", "Fact", "Metadata")
nodes <- data.frame(
  model = model_names,
  type = factor(model_types, levels = type_levels),
  depth = as.integer(depth[model_names]) + 1L,
  fan_in = as.integer(fan_in),
  fan_out = as.integer(fan_out),
  blast_radius = blast_radius,
  stringsAsFactors = FALSE
)

# A deterministic layered layout. Within each layer, sorting by parent
# barycenter reduces crossings without exposing model names in the figure.
nodes$y <- NA_real_
for (layer in sort(unique(nodes$depth))) {
  layer_models <- nodes$model[nodes$depth == layer]
  if (layer == 0) {
    ordering <- order(nodes$type[match(layer_models, nodes$model)], layer_models)
  } else {
    barycenter <- vapply(layer_models, function(model) {
      p <- parents[[model]]
      if (is.null(p) || length(p) == 0) return(0)
      mean(nodes$y[match(p, nodes$model)], na.rm = TRUE)
    }, numeric(1))
    ordering <- order(barycenter, nodes$type[match(layer_models, nodes$model)], layer_models)
  }
  ordered <- layer_models[ordering]
  positions <- seq_along(ordered) - (length(ordered) + 1) / 2
  nodes$y[match(ordered, nodes$model)] <- positions
}

edge_plot_data <- edges %>%
  left_join(nodes %>% select(model, x = depth, y), by = c("from" = "model")) %>%
  left_join(nodes %>% select(model, xend = depth, yend = y), by = c("to" = "model"))

paper_theme <- theme_bw(base_size = 16, base_family = base_font) +
  theme(
    panel.border = element_rect(linewidth = 0.6),
    panel.grid.minor = element_blank(),
    axis.text = element_text(size = 9),
    axis.title = element_text(size = 10),
    plot.margin = margin(3, 5, 3, 4)
  )

feature_plot <- ggplot(feature_summary, aes(x = percentage, y = feature)) +
  geom_col(width = 0.72, fill = "#4dff4d", color = "#2f7d32", linewidth = 0.25) +
  geom_text(aes(x = 1.08, label = label), hjust = 1, size = 3.2, family = base_font) +
  scale_x_continuous(
    labels = percent_format(accuracy = 1),
    limits = c(0, 1.10),
    breaks = c(0, 0.25, 0.5, 0.75, 1),
    expand = expansion(mult = c(0, 0))
  ) +
  labs(x = "Models containing feature", y = NULL) +
  paper_theme +
  theme(panel.grid.major.y = element_blank())

type_colors <- c(
  "Staging" = "#56B4E9",
  "Intermediate" = "#009E73",
  "Dimension" = "#E69F00",
  "Fact" = "#CC79A7",
  "Metadata" = "#4D4D4D"
)
dag_plot <- ggplot() +
  geom_segment(
    data = edge_plot_data,
    aes(x = x, y = y, xend = xend, yend = yend),
    color = "#777777", alpha = 0.22, linewidth = 0.28
  ) +
  geom_point(
    data = nodes,
    aes(x = depth, y = y, fill = type),
    shape = 21, color = "#333333", stroke = 0.25, size = 2.4
  ) +
  scale_fill_manual(values = type_colors, drop = FALSE) +
  scale_x_continuous(breaks = seq(1, max(nodes$depth)), expand = expansion(add = c(0.3, 0.3))) +
  labs(x = "Dependency depth", y = NULL, fill = NULL) +
  paper_theme +
  theme(
    axis.text.y = element_blank(),
    axis.ticks.y = element_blank(),
    panel.grid.major.y = element_blank(),
    panel.grid.major.x = element_line(linewidth = 0.35),
    legend.position = "top",
    legend.text = element_text(size = 9),
    legend.margin = margin(0, 0, 0, 0),
    legend.box.spacing = grid::unit(5, "pt"),
    legend.key.height = grid::unit(8, "pt"),
    legend.key.width = grid::unit(11, "pt")
  )

feature_png <- paste0(output_prefix, "_features.png")
dag_png <- paste0(output_prefix, "_dag.png")
feature_csv <- paste0(output_prefix, "_features.csv")
depth_csv <- paste0(output_prefix, "_depth.csv")
dir.create(dirname(output_prefix), recursive = TRUE, showWarnings = FALSE)
ggsave(feature_png, feature_plot, width = 5.00, height = 3.35, dpi = 300, bg = "white")
ggsave(dag_png, dag_plot, width = 7.20, height = 2.65, dpi = 400, bg = "white")
write.csv(feature_summary %>% mutate(feature = as.character(feature)), feature_csv, row.names = FALSE)
write.csv(count(nodes, depth, type, name = "models", .drop = FALSE), depth_csv, row.names = FALSE)

message("Models: ", nrow(nodes), " (", paste(names(table(nodes$type)), table(nodes$type), collapse = ", "), ")")
message("Edges: ", nrow(edges), "; max depth: ", max(nodes$depth),
        "; max fan-in: ", max(nodes$fan_in), "; max fan-out: ", max(nodes$fan_out),
        "; max blast radius: ", max(nodes$blast_radius))
message("Join models: ", sum(join_counts > 0), "; 10+ joins: ", sum(join_counts >= 10),
        "; maximum joins: ", max(join_counts))
message("Feature counts: ", paste(names(feature_counts), feature_counts, sep = "=", collapse = ", "))
message("Plots saved to: ", feature_png, " and ", dag_png)
