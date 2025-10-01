#!/usr/bin/env Rscript
# Read a YAML config (RO), print it, then persist timing info (RW).
# Computes time since the previous run based on the prior state.json.

suppressPackageStartupMessages({
  library(yaml)
  library(jsonlite)
})
source('task_Big_Deals.R')
#cfg_path   <- "/config/config.yaml"  # read-only bind mount
#state_path <- "/data/state.json"     # read-write bind mount
res <- NULL
cfg_path   <- "./conf.yaml"  # read-only bind mount
state_path <- "../../run/state.json"     # read-write bind mount
`%||%` <- function(a,b) if (is.null(a)) b else a

to_iso_utc <- function(x) strftime(x, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
from_iso_utc <- function(s) {
  # Parse ISO8601 "YYYY-mm-ddTHH:MM:SSZ" as UTC; return POSIXct or NA
  if (is.null(s) || !nzchar(s)) return(NA_real_)
  as.POSIXct(strptime(s, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"))
}

t_start <- Sys.time()

# 1) Read config and print (pretty JSON for readability)
if (!file.exists(cfg_path)) stop("Config not found at: ", cfg_path)
#cfg <- yaml::read_yaml(cfg_path)
cfg <- yaml.load_file(cfg_path)
cat("=== CONFIG ===\n", toJSON(cfg, auto_unbox = TRUE, pretty = TRUE), "\n", sep = "")

res <- execute_task_big_deals(config = cfg)
res
# 2) Load previous state (if any) and compute time since last run
prev <- list()
if (file.exists(state_path)) {
  prev <- tryCatch(read_json(state_path, simplifyVector = TRUE),
                   error = function(e) { cat("WARN: cannot parse state.json (treat as empty)\n"); list() })
}
prev_finished <- from_iso_utc(prev$executed_at)
since_last_run_sec <- if (is.na(prev_finished)) NA_real_ else as.numeric(difftime(t_start, prev_finished, units = "secs"))

if (is.na(since_last_run_sec)) {
  cat("First run (no previous state).\n")
} else {
  cat("Seconds since last run: ", since_last_run_sec, "\n", sep = "")
}

# ... your job logic would go here ...

t_end <- Sys.time()
duration_sec <- as.numeric(difftime(t_end, t_start, units = "secs"))

# 3) Persist new state
state <- list(
  started_at         = to_iso_utc(t_start),
  executed_at        = to_iso_utc(t_end),
  duration_sec       = duration_sec,
  since_last_run_sec = since_last_run_sec,
  run_count          = as.integer((prev$run_count %||% 0) + 1L)
)
write_json(state, state_path, auto_unbox = TRUE, pretty = TRUE)
cat("State written: ", state_path, " (run_count=", state$run_count, ")\n", sep = "")

