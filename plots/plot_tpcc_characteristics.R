#!/usr/bin/env Rscript
# Characterize the OpenIVM TPC-C coverage corpus from the JSON metadata stored
# in the first line of every query file.

script_file <- sub("^--file=", "", grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)[1])
script_dir <- dirname(normalizePath(script_file))
source(file.path(script_dir, "plot_common.R"))
RequirePlotPackages(c("ggplot2", "dplyr", "jsonlite", "scales", "systemfonts", "tibble"))

suppressPackageStartupMessages({
  library(ggplot2)
  library(dplyr)
  library(scales)
  library(tibble)
})

base_font <- PaperFont()
args <- commandArgs(trailingOnly = TRUE)
query_dir <- if (length(args) >= 1) args[1] else {
  stop("Usage: plot_tpcc_characteristics.R <pinned-query-corpus> [output-prefix]")
}
output_prefix <- if (length(args) >= 2) {
  sub("[.]png$", "", args[2])
} else {
  file.path(script_dir, "output", "tpcc_workload_characteristics")
}
feature_png <- paste0(output_prefix, "_features.png")
combination_png <- paste0(output_prefix, "_combinations.png")
nesting_csv <- paste0(output_prefix, "_nesting.csv")

if (!dir.exists(query_dir)) {
  stop("Query directory does not exist: ", query_dir)
}

query_files <- sort(list.files(query_dir, pattern = "[.]sql$", full.names = TRUE))
if (length(query_files) == 0) {
  stop("No SQL files found in: ", query_dir)
}

read_query <- function(path) {
  lines <- readLines(path, warn = FALSE)
  if (length(lines) == 0 || !startsWith(lines[1], "-- ")) {
    stop("Missing JSON metadata header in: ", path)
  }

  metadata <- tryCatch(
    jsonlite::fromJSON(sub("^-- ", "", lines[1])),
    error = function(e) stop("Invalid JSON metadata in ", path, ": ", conditionMessage(e))
  )

  operators <- trimws(strsplit(metadata$operators %||% "", ",", fixed = TRUE)[[1]])
  operators <- operators[nzchar(operators)]
  sql <- paste(lines[-1], collapse = "\n")

  list(
    query = basename(path),
    operators = operators,
    complexity = metadata$complexity %||% "unknown",
    has_nulls = isTRUE(metadata$has_nulls),
    has_cast = isTRUE(metadata$has_cast),
    has_case = isTRUE(metadata$has_case),
    sql = sql
  )
}

`%||%` <- function(value, fallback) {
  if (is.null(value) || length(value) == 0 || is.na(value)) fallback else value
}

queries <- lapply(query_files, read_query)
query_count <- length(queries)

has_operator <- function(query, candidates) {
  any(query$operators %in% candidates)
}

has_sql <- function(query, pattern) {
  grepl(pattern, query$sql, ignore.case = TRUE, perl = TRUE)
}

count_sql_matches <- function(sql, pattern) {
  locations <- gregexpr(pattern, sql, ignore.case = TRUE, perl = TRUE)[[1]]
  if (locations[1] == -1) 0L else length(locations)
}

count_query_nesting <- function(query) {
  # Ignore SQL text inside literals and comments before counting syntactic query blocks.
  sql <- gsub("(?s)/[*].*?[*]/", " ", query$sql, perl = TRUE)
  sql <- gsub("--[^\n]*", " ", sql, perl = TRUE)
  sql <- gsub("'(?:''|[^'])*'", "''", sql, perl = TRUE)

  # A named expression of the form `name [(columns)] AS (<query block>)` is a CTE.
  cte_count <- count_sql_matches(
    sql,
    paste0(
      "\\b[A-Za-z_][A-Za-z0-9_]*",
      "(?:\\s*\\([^)]*\\))?\\s+AS\\s+",
      "(?:(?:NOT\\s+)?MATERIALIZED\\s+)?",
      "\\(\\s*(?:SELECT|WITH|VALUES|TABLE)\\b"
    )
  )
  parenthesized_query_count <- count_sql_matches(sql, "\\(\\s*(?:SELECT|WITH|VALUES|TABLE)\\b")
  join_count <- count_sql_matches(sql, "\\bJOIN\\b")

  c(
    ctes = cte_count,
    subqueries = max(parenthesized_query_count - cte_count, 0L),
    joins = join_count
  )
}

nesting_counts <- t(vapply(
  queries,
  count_query_nesting,
  FUN.VALUE = c(ctes = 0L, subqueries = 0L, joins = 0L)
))
nesting_buckets <- c("0", "1", "2", "3", "4+")
bucket_nesting_count <- function(values) {
  factor(ifelse(values >= 4, "4+", as.character(values)), levels = nesting_buckets)
}

nesting_summary <- bind_rows(
  tibble(construct = "CTEs", bucket = bucket_nesting_count(nesting_counts[, "ctes"])),
  tibble(construct = "Subqueries", bucket = bucket_nesting_count(nesting_counts[, "subqueries"])),
  tibble(construct = "Joins", bucket = bucket_nesting_count(nesting_counts[, "joins"]))
) %>%
  count(construct, bucket, .drop = FALSE, name = "queries") %>%
  mutate(percentage = queries / query_count)

feature_definitions <- list(
  "SELECT" = function(query) has_sql(query, "\\bSELECT\\b"),
  "Filter" = function(query) has_operator(query, "FILTER"),
  "Inner join" = function(query) has_operator(query, "INNER_JOIN"),
  "Left/right outer join" = function(query) has_sql(
    query,
    "\\b(?:LEFT|RIGHT)(?:\\s+OUTER)?\\s+JOIN\\b"
  ),
  "Full outer join" = function(query) has_sql(query, "\\bFULL(?:\\s+OUTER)?\\s+JOIN\\b"),
  "Cross join" = function(query) has_operator(query, "CROSS_JOIN"),
  "Semi join" = function(query) has_operator(query, "SEMI_JOIN"),
  "Anti join" = function(query) has_operator(query, "ANTI_JOIN"),
  "Lateral join" = function(query) has_operator(query, "LATERAL"),
  "ASOF join" = function(query) has_operator(query, "ASOF_JOIN"),
  "Positional join" = function(query) has_operator(query, "POSITIONAL_JOIN"),
  "Aggregate" = function(query) has_operator(query, "AGGREGATE"),
  "GROUP BY" = function(query) has_sql(query, "\\bGROUP\\s+BY\\b"),
  "HAVING" = function(query) has_operator(query, "HAVING"),
  "DISTINCT" = function(query) has_operator(query, c("DISTINCT", "COUNT_DISTINCT")),
  "Window" = function(query) has_operator(query, "WINDOW"),
  "CTE" = function(query) has_operator(query, "CTE"),
  "Subquery" = function(query) has_operator(query, c("SUBQUERY", "SUBQUERY_FILTER", "CORRELATED_SUBQUERY")),
  "Set operation" = function(query) has_operator(
    query,
    c("UNION", "UNION_ALL", "INTERSECT", "INTERSECT_ALL", "EXCEPT", "EXCEPT_ALL")
  ),
  "ORDER BY" = function(query) has_operator(query, c("ORDER", "ORDER_BY")),
  "Top-K" = function(query) has_operator(query, "LIMIT"),
  "NULL handling" = function(query) query$has_nulls,
  "CASE" = function(query) query$has_case,
  "CAST" = function(query) query$has_cast
)

feature_matrix <- vapply(
  feature_definitions,
  function(predicate) vapply(queries, predicate, FUN.VALUE = logical(1)),
  FUN.VALUE = logical(query_count)
)

feature_summary <- tibble(
  feature = colnames(feature_matrix),
  queries = colSums(feature_matrix),
  percentage = queries / query_count
) %>%
  arrange(percentage, feature) %>%
  mutate(
    feature = factor(feature, levels = feature),
    label = if_else(
      percentage < 0.01,
      percent(percentage, accuracy = 0.1),
      percent(percentage, accuracy = 1)
    )
  )

combination_features <- c(
  "Filter", "Inner join", "Left/right outer join", "Full outer join",
  "Cross join", "Semi join", "Anti join", "Lateral join", "ASOF join", "Positional join",
  "Aggregate", "Window", "CTE", "Subquery", "Set operation", "Top-K"
)
combination_labels <- apply(feature_matrix[, combination_features, drop = FALSE], 1, function(row) {
  present <- combination_features[row]
  if (length(present) == 0) "Scan only" else paste(present, collapse = " + ")
})

combination_counts <- tibble(combination = combination_labels) %>%
  count(combination, name = "queries", sort = TRUE)

named_combination_count <- 30
other_pattern_count <- max(nrow(combination_counts) - named_combination_count, 0)
combination_summary <- combination_counts %>% slice_head(n = named_combination_count)
if (other_pattern_count > 0) {
  combination_summary <- bind_rows(
    combination_summary,
    tibble(
      combination = paste0("Other combinations (", other_pattern_count, " patterns)"),
      queries = sum(combination_counts$queries[(named_combination_count + 1):nrow(combination_counts)])
    )
  )
}

combination_summary <- combination_summary %>%
  mutate(
    percentage = queries / query_count,
    combination = gsub("Set operation", "Set op.", combination, fixed = TRUE),
    combination = gsub("Inner join", "IJ", combination, fixed = TRUE),
    combination = gsub("Left/right outer join", "OJ", combination, fixed = TRUE),
    combination = gsub("Full outer join", "FOJ", combination, fixed = TRUE),
    combination = gsub("Cross join", "CJ", combination, fixed = TRUE),
    combination = gsub("Semi join", "SJ", combination, fixed = TRUE),
    combination = gsub("Anti join", "AJ", combination, fixed = TRUE),
    combination = gsub("Lateral join", "LatJ", combination, fixed = TRUE),
    combination = gsub("ASOF join", "ASOF", combination, fixed = TRUE),
    combination = gsub("Positional join", "PJ", combination, fixed = TRUE),
    combination = factor(combination, levels = rev(combination)),
    label = percent(percentage, accuracy = 0.1)
  )

stopifnot(sum(combination_summary$queries) == query_count)

paper_theme <- theme_bw(base_size = 20, base_family = base_font) +
  theme(
    panel.border = element_rect(linewidth = 0.8),
    panel.grid.major.x = element_line(linewidth = 0.45),
    panel.grid.major.y = element_blank(),
    panel.grid.minor = element_blank(),
    axis.text.x = element_text(size = 10),
    axis.text.y = element_text(size = 10.5),
    axis.title = element_blank(),
    plot.margin = margin(3, 4, 3, 3)
  )

feature_plot <- ggplot(feature_summary, aes(x = percentage, y = feature)) +
  geom_col(width = 0.72, fill = "#4dff4d", color = "#2f7d32", linewidth = 0.25) +
  geom_text(aes(x = 1.20, label = label), hjust = 1, size = 3.4, family = base_font) +
  scale_x_continuous(
    labels = percent_format(accuracy = 1),
    limits = c(0, 1.22),
    breaks = c(0, 0.5, 1),
    expand = expansion(mult = c(0, 0))
  ) +
  labs(x = NULL, y = NULL) +
  paper_theme

combination_limit <- max(combination_summary$percentage) * 1.30
combination_plot <- ggplot(combination_summary, aes(x = percentage, y = combination)) +
  geom_col(width = 0.72, fill = "#009900", color = "#2f7d32", linewidth = 0.25) +
  geom_text(aes(x = combination_limit * 0.98, label = label), hjust = 1, size = 3.4, family = base_font) +
  scale_x_continuous(
    labels = percent_format(accuracy = 1),
    limits = c(0, combination_limit),
    expand = expansion(mult = c(0, 0))
  ) +
  labs(x = NULL, y = NULL) +
  paper_theme +
  theme(axis.text.y = element_text(size = 10.5, lineheight = 0.9))

dir.create(dirname(output_prefix), recursive = TRUE, showWarnings = FALSE)
ggsave(feature_png, feature_plot, width = 5.00, height = 4.15, dpi = 300, bg = "white")
ggsave(combination_png, combination_plot, width = 7.05, height = 4.40, dpi = 300, bg = "white")

write.csv(
  feature_summary %>% mutate(feature = as.character(feature)),
  paste0(output_prefix, "_features.csv"),
  row.names = FALSE
)
write.csv(
  combination_summary %>% mutate(combination = as.character(combination)),
  paste0(output_prefix, "_combinations.csv"),
  row.names = FALSE
)
write.csv(
  nesting_summary %>% mutate(bucket = as.character(bucket)),
  nesting_csv,
  row.names = FALSE
)

message("font: ", base_font)
message("queries analyzed: ", comma(query_count))
message("Feature plot saved to: ", feature_png)
message("Combination plot saved to: ", combination_png)
message("Nesting summary saved to: ", nesting_csv)
