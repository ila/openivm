#!/usr/bin/env Rscript
# Recover reported (0.1-second precision) timings from public GCI logs.
args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 3) stop("Usage: extract_augmented_sweep.R <589-log> <590-log> <output.csv>")
ReadRun <- function(path, run_id, keep_pct) {
  pct <- days <- repetition <- NA_integer_
  rows <- list()
  for (line in readLines(path, warn = FALSE)) {
    match <- regmatches(line, regexec("sf100-augmented-([0-9]+)pct-([0-9]+)days \\(SF=100\\)", line))[[1]]
    if (length(match)) {
      pct <- as.integer(match[2]); days <- as.integer(match[3]); repetition <- NA_integer_
    }
    match <- regmatches(line, regexec("Benchmark repetition ([0-9]+)/2", line))[[1]]
    if (length(match)) repetition <- as.integer(match[2])
    match <- regmatches(line, regexec("\\[([a-z0-9-]+)\\] Batch 2 completed in ([0-9.]+)s", line))[[1]]
    if (length(match) && !is.na(repetition) && pct %in% keep_pct) {
      rows[[length(rows) + 1]] <- data.frame(
        github_run_id = run_id, scale_factor = 100L, window_pct = pct,
        batch_2_days = days, engine = match[2], repetition = repetition,
        batch_num = 2L, seconds = as.numeric(match[3]),
        timing_source = "reported_batch_duration_log", validation = FALSE,
        refresh_policy = ifelse(match[2] == "databricks-enzyme", "AUTO", "not_applicable")
      )
    }
  }
  do.call(rbind, rows)
}
raw <- rbind(ReadRun(args[1], "34600019978", c(5, 15, 25)),
             ReadRun(args[2], "34851747973", c(35, 45)))
stopifnot(!anyDuplicated(raw[c("github_run_id", "window_pct", "engine", "repetition")]))
stopifnot(all(is.finite(raw$seconds)), all(raw$seconds > 0), all(raw$scale_factor == 100))
write.csv(raw, args[3], row.names = FALSE)
message("Recovered ", nrow(raw), " batch timings; incomplete pairs remain explicitly incomplete.")
