#!/usr/bin/env Rscript
# Generate one HTML statement per account for the previous UTC calendar day,
# written to dataDocker/storage/<domain-schema>/<YYYYMMDD>.daily/<account>_mail.html

suppressPackageStartupMessages({
  library(yaml)
  library(jsonlite)
  library(lubridate)
})
source('../common/helpFunctions.R') #load_config: read YAML + substitute ${VAR} from .env / env
source('task_TT_Statements.R')
cfg_path   <- "./configDocker/config.yaml"  # read-only bind mount
state_path <- "./dataDocker/logs/daily/state.json"     # read-write bind mount
last_run_path <- "./dataDocker/logs/daily/last_run.txt"     # read-write bind mount
task_exec_log_path <- "./dataDocker/logs/daily/task_exec_log.csv"
dir.create(dirname(state_path), recursive = TRUE, showWarnings = FALSE)
res <- NULL

# Restrict the run to specific accounts (e.g. for manual testing): edit this line, then run
# the script -- NULL processes every account. Ignored/overwritten below if run via
# `Rscript main_daily.R 1533790,17012781` (Docker/CLI use).
only_logins <- NULL   # e.g. only_logins <- c(22030134, 23007510, 24003980, 25000785, 36000004, 22702989)

# Generate for a specific UTC calendar day instead of "yesterday": edit this line, then run.
# NULL uses yesterday (the normal daily-cron behavior).
target_date <- NULL   # e.g. target_date <- "2026-08-01"
########################################################################

to_iso_utc <- function(x) strftime(x, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
from_iso_utc <- function(s) {
  if (is.null(s) || !nzchar(s)) return(NA_real_)
  as.POSIXct(strptime(s, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"))
}

t_start <- Sys.time()

# 1) Read config and print (pretty JSON for readability)
if (!file.exists(cfg_path)) stop("Config not found at: ", cfg_path)
cfg <- load_config(cfg_path)
cat("=== CONFIG ===\n", toJSON(redact_secrets(cfg), auto_unbox = TRUE, pretty = TRUE), "\n", sep = "")

# 2) Target UTC calendar day: [dayFrom, dayTo) -- yesterday by default, or `target_date` if set
if (!is.null(target_date)) {
  dayFrom <- as.POSIXct(target_date, tz = "UTC")
  dayTo   <- dayFrom + days(1)
} else {
  dayTo   <- floor_date(with_tz(Sys.time(), "UTC"), "day")
  dayFrom <- dayTo - days(1)
}
day_label <- format(dayFrom, "%Y-%m-%d")

storage_root <- "./dataDocker/storage"
period_folder <- paste0(format(dayFrom, "%Y%m%d"), ".daily")

# CLI override (Docker/batch use only -- commandArgs() is empty when run interactively line-by-line)
cli_args <- commandArgs(trailingOnly = TRUE)
if (length(cli_args) > 0 && nzchar(cli_args[1])) {
  only_logins <- as.numeric(trimws(strsplit(cli_args[1], ",")[[1]]))
}
if (!is.null(only_logins)) cat("Restricting run to logins:", paste(only_logins, collapse = ", "), "\n")

res <- execute_task_tt_statements(config = cfg, dayFrom = dayFrom, dayTo = dayTo, day_label = day_label,
                                   storage_root = storage_root, period_folder = period_folder,
                                   task_exec_log_path = task_exec_log_path, only_logins = only_logins,
                                   monitor_paths = cfg$monitoring$connection$paths[c(1, 3)],
                                   period_type = "Daily", summary_layout = "single_column")
task_period <- paste("Day:", day_label, "Statements written:", res[[4]])
cat(task_period, "\n")

# 3) Load previous state (if any) and compute time since last run
prev <- list()
if (file.exists(state_path)) {
  prev <- tryCatch(read_json(state_path, simplifyVector = TRUE),
                   error = function(e) { cat("WARN: cannot parse state.json (treat as empty)\n"); list() })
}
prev_finished <- from_iso_utc(prev$executed_at)
since_last_run_sec <- if (is.na(prev_finished)) NA_real_ else as.numeric(difftime(t_start, prev_finished, units = "secs"))

t_end <- Sys.time()
duration_sec <- as.numeric(difftime(t_end, t_start, units = "secs"))

# 4) Persist new state
state <- list(
  started_at         = to_iso_utc(t_start),
  executed_at        = to_iso_utc(t_end),
  duration_sec       = duration_sec,
  since_last_run_sec = since_last_run_sec,
  task_period        = task_period,
  run_count          = as.integer((prev$run_count %||% 0) + 1L)
)
write_json(state, state_path, auto_unbox = TRUE, pretty = TRUE)
writeLines(format(t_end, "%Y-%m-%dT%H:%M:%S%z"), last_run_path)
cat("State written: ", state_path, " (run_count=", state$run_count, ")\n", sep = "")
