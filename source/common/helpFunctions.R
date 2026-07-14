# --- Config loader: reads YAML, substitutes ${VAR} from .env / env vars ---
# Verbatim copy of the loader used in aggr-data_App/helpFunctions.R and
# user-data-App/helpFunc.R. Reads .env next to the project root (local use only;
# inside Docker the secrets arrive via the container environment, see compose).
load_config <- function(yaml_path, env_path = NULL) {
  # Parse .env → env vars (skip comments/empty, don't overwrite existing) — local use only (Docker gets vars from environment)
  env_path <- env_path %||% file.path(dirname(yaml_path), "..", ".env")
  if (file.exists(env_path)) {
    for (line in readLines(env_path, warn = FALSE)) {
      if (grepl("^\\s*#", line) || !nzchar(trimws(line))) next
      key <- trimws(sub("=.*", "", line))
      val <- trimws(sub("^[^=]+=", "", line))
      val <- sub('^"(.*)"$', '\\1', val)   # strip surrounding double quotes (allows special chars like # in passwords)
      val <- sub("^'(.*)'$", "\\1", val)   # strip surrounding single quotes
      if (Sys.getenv(key) == "") do.call(Sys.setenv, setNames(list(val), key))
    }
  }
  # Load YAML and recursively replace ${VAR} in all string values — substitutes env var values into the config
  cfg <- yaml::yaml.load_file(yaml_path)
  subst <- function(s) {
    # collect distinct var names from the ORIGINAL string; replace each once so
    # substituted values are never re-scanned (no mangle, always terminates)
    vars <- regmatches(s, gregexpr("\\$\\{\\w+\\}", s))[[1]]
    if (!length(vars)) return(s)
    vars <- unique(sub("^\\$\\{(\\w+)\\}$", "\\1", vars))
    for (v in vars) {
      s <- gsub(paste0("${", v, "}"), Sys.getenv(v, ""), s, fixed = TRUE)
    }
    s
  }
  resolve <- function(x) if (is.character(x)) subst(x) else if (is.list(x)) lapply(x, resolve) else x
  resolve(cfg)
}

# --- Redact secret fields (by key name) before printing/logging a config ---
# Recursively walks a parsed config list; any leaf whose KEY name looks secret-like
# (password / postgre_PASSWORD / productKey / secret / token, case-insensitive) is
# replaced with a sentinel ("***"). Structure is preserved, only matching leaves
# change. Pair with load_config():  cat(toJSON(redact_secrets(cfg), ...)).
redact_secrets <- function(x, sentinel = "***") {
  if (!is.list(x)) return(x)                  # atomic/vector leaf (no key) -> as-is
  for (i in seq_along(x)) {
    nm <- names(x)[i]                         # NA / "" for unnamed elements
    v  <- x[[i]]
    x[[i]] <- if (is.list(v)) redact_secrets(v, sentinel)
              else if (!is.na(nm) && nzchar(nm) &&
                       grepl("password|secret|token|productkey", nm, ignore.case = TRUE)) sentinel
              else v
  }
  x
}
