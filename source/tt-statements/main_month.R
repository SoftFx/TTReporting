#!/usr/bin/env Rscript
# Generate one HTML statement per account for the previous UTC calendar month,
# written to dataDocker/storage/<domain-schema>/<YYYYMMDD>.monthly/<account>_mail.html
# (YYYYMMDD is the last day of the target month)

suppressPackageStartupMessages({
  library(yaml)
  library(jsonlite)
  library(lubridate)
})
source('../common/helpFunctions.R') #load_config: read YAML + substitute ${VAR} from .env / env
source('task_TT_Statements.R')
cfg_path   <- "./configDocker/config.yaml"  # read-only bind mount
state_path <- "./dataDocker/logs/monthly/state.json"     # read-write bind mount
last_run_path <- "./dataDocker/logs/monthly/last_run.txt"     # read-write bind mount
task_exec_log_path <- "./dataDocker/logs/monthly/task_exec_log.csv"
dir.create(dirname(state_path), recursive = TRUE, showWarnings = FALSE)
res <- NULL

# Restrict the run to specific accounts (e.g. for manual testing): edit this line, then run
# the script -- NULL processes every account. Ignored/overwritten below if run via
# `Rscript main_month.R 1533790,17012781` (Docker/CLI use).
only_logins <- NULL   # e.g. only_logins <- c(22030134, 23007510, 24003980, 25000785, 36000004, 22702989)

# Generate for a specific UTC calendar month instead of "previous month". Leave NULL here and
# drive it from outside (precedence note at step 2): CLI `--month=2026-07-01`, env
# TT_TARGET_MONTH, or config `backfill.target_month` (first day of the target month). Editing
# this line still works and wins. NULL everywhere = previous month (the normal monthly-cron).
target_month <- NULL   # e.g. target_month <- "2026-07-01"
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

# 2) Target UTC calendar month: [dayFrom, dayTo). Default = previous month. To backfill a past
#    month, set ONE of these -- precedence high -> low:
#      hardcoded `target_month` above  >  CLI `--month=YYYY-MM-01`  >  env TT_TARGET_MONTH  >
#      config `backfill.target_month`
#    CLI/env are per-invocation (safe). The config key is STICKY -- clear it after the backfill
#    or every scheduled run keeps regenerating that month.
cli_args  <- commandArgs(trailingOnly = TRUE)
cli_flags <- grep("^--", cli_args, value = TRUE)
cli_pos   <- grep("^--", cli_args, value = TRUE, invert = TRUE)   # positional arg = only_logins list
cli_month <- sub("^--month=", "", grep("^--month=", cli_flags, value = TRUE))
env_month <- Sys.getenv("TT_TARGET_MONTH")

if (is.null(target_month)) {
  target_month <- if (length(cli_month) == 1 && nzchar(cli_month)) cli_month
                  else if (nzchar(env_month))                      env_month
                  else                                            cfg$backfill$target_month  # NULL if absent
}

if (!is.null(target_month) && nzchar(target_month)) {
  dayFrom <- as.POSIXct(target_month, tz = "UTC")
  if (is.na(dayFrom)) stop("invalid target_month (expected YYYY-MM-01): ", target_month)
  dayFrom <- floor_date(dayFrom, "month")   # tolerate any day in the month, snap to the 1st
  dayTo   <- dayFrom + months(1)
  cat("Backfill: target month set to", format(dayFrom, "%Y-%m"), "\n")
} else {
  dayTo   <- floor_date(with_tz(Sys.time(), "UTC"), "month")
  dayFrom <- dayTo - months(1)
}
day_label <- format(dayFrom, "%Y-%m")

storage_root <- "./dataDocker/storage"
period_folder <- paste0(format(dayTo - days(1), "%Y%m%d"), ".monthly")

# only_logins from the positional CLI arg (Docker/batch use only -- empty when run interactively)
if (length(cli_pos) > 0 && nzchar(cli_pos[1])) {
  only_logins <- as.numeric(trimws(strsplit(cli_pos[1], ",")[[1]]))
}
if (!is.null(only_logins)) cat("Restricting run to logins:", paste(only_logins, collapse = ", "), "\n")

res <- execute_task_tt_statements(config = cfg, dayFrom = dayFrom, dayTo = dayTo, day_label = day_label,
                                   storage_root = storage_root, period_folder = period_folder,
                                   task_exec_log_path = task_exec_log_path, only_logins = only_logins,
                                   monitor_paths = cfg$monitoring$connection$paths[c(2, 3)],
                                   period_type = "Monthly", summary_layout = "single_column")
task_period <- paste("Month:", day_label, "Statements written:", res[[4]])
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
