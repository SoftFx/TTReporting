library(data.table)
library(RPostgres)

# --- Config loader: reads YAML, substitutes ${VAR} from .env / env vars ---
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

DBExecuteWrapper <- function(conn, querry){
  if(is.null(conn))
    stop("No connection to DB")
  dbExecute(conn, querry)
}

GetDataFromDB <- function(conn, query) {
  if(is.null(conn))
    stop("No connection to DB")
  rs = dbSendQuery(conn, query)
  data = dbFetch(rs, -1)
  dbClearResult(rs)
  setDT(data)
  return(data)
}

# quoteString <- function(string){
#   return(paste0('\'', string, '\''))
# }

 quoteString <- function(con, value) {
   DBI::dbQuoteLiteral(con, value)
 }

setPathToSchema <- function(schemaName, con) {
  querry <- paste("SET search_path TO", paste0("\"",schemaName, "\""))
  DBExecuteWrapper(conn = con, querry)
}

setDefaultSchema <- function(con) {
  querry <- paste("RESET search_path")
  DBExecuteWrapper(conn = con, querry)
}


getLPexecdataAggr <- function(CredsAggr, From, To){
  con <- dbConnect(RPostgres::Postgres(), dbname = CredsAggr$postgre_DB, user = CredsAggr$postgre_USER, password = CredsAggr$postgre_PASSWORD, host = CredsAggr$postgre_HOST)
  on.exit(dbDisconnect(con))
  setPathToSchema(CredsAggr$postgre_SCHEMA, con)
  
  #querry1 <- paste('SELECT * from "lpexecution" WHERE "time" >=', quoteString(From), 'and "time" < ', quoteString(To))
  #res1 <- GetDataFromDB(con, querry1)
  res1 <- dbGetQuery(con, 'SELECT * from "lpexecution" WHERE "time" >= $1 AND "time" < $2', params = list(From, To))
  
  # querry2 <- paste('SELECT * from "lpexecutionfailed" WHERE "time" >=', quoteString(From), 'and "time" < ', quoteString(To))
  # res2 <- GetDataFromDB(con, querry2)
  res2 <- dbGetQuery(con, 'SELECT * from "lpexecutionfailed" WHERE "time" >= $1 AND "time" < $2', params = list(From, To))
  
  # querry3 <- paste('SELECT * from "lpexecutiondetails" WHERE "time" >=', quoteString(From), 'and "time" < ', quoteString(To))
  # res3 <- GetDataFromDB(con, querry3)
  res3 <- dbGetQuery(con, 'SELECT * from "lpexecutiondetails" WHERE "time" >= $1 AND "time" < $2', params = list(From, To))
  
  #setDefaultSchema(con) #use only if different Queries to different schemas
  
  res1 <- as.data.table(res1)[, aggrName := CredsAggr$postgre_SCHEMA]
  res2 <- as.data.table(res2)[, aggrName := CredsAggr$postgre_SCHEMA]
  res3 <- as.data.table(res3)[, aggrName := CredsAggr$postgre_SCHEMA]
  return(list(res1, res2, res3))
}

getUSERexecdataAggr <- function(CredsAggr, From, To){
  con <- dbConnect(RPostgres::Postgres(), dbname = CredsAggr$postgre_DB, user = CredsAggr$postgre_USER, password = CredsAggr$postgre_PASSWORD, host = CredsAggr$postgre_HOST)
  on.exit(dbDisconnect(con))
  setPathToSchema(CredsAggr$postgre_SCHEMA, con)
  
  # querry1 <- paste('SELECT * from "userexecution" WHERE "time" >=', quoteString(From), 'and "time" < ', quoteString(To))
  # res1 <- GetDataFromDB(con, querry1)
  res1 <- dbGetQuery(con, 'SELECT * from "userexecution" WHERE "time" >= $1 AND "time" < $2', params = list(From, To))
  
  # querry2 <- paste('SELECT * from "userexecutionfailed" WHERE "time" >=', quoteString(From), 'and "time" < ', quoteString(To))
  # res2 <- GetDataFromDB(con, querry2)
  res2 <- dbGetQuery(con, 'SELECT * from "userexecutionfailed" WHERE "time" >= $1 AND "time" < $2', params = list(From, To))
  
  # querry3 <- paste('SELECT * from "userexecutiondetails" WHERE "time" >=', quoteString(From), 'and "time" < ', quoteString(To))
  # res3 <- GetDataFromDB(con, querry3)
  res3 <- dbGetQuery(con, 'SELECT * from "userexecutiondetails" WHERE "time" >= $1 AND "time" < $2', params = list(From, To))
  
  #setDefaultSchema(con)
  
  res1 <- as.data.table(res1)[, aggrName := CredsAggr$postgre_SCHEMA]
  res2 <- as.data.table(res2)[, aggrName := CredsAggr$postgre_SCHEMA]
  res3 <- as.data.table(res3)[, aggrName := CredsAggr$postgre_SCHEMA]
  
  return(list(res1,res2,res3))
}

#get Symbol Properties from TT db
getAllSymbolsTT <- function(Credstt){
  con <- dbConnect(RPostgres::Postgres(), dbname = Credstt$postgre_DB, user = Credstt$postgre_USER, password = Credstt$postgre_PASSWORD, host = Credstt$postgre_HOST)
  on.exit(dbDisconnect(con))  
  setPathToSchema(Credstt$postgre_SCHEMA, con)


  querryS <-paste('select s."Name" as "symbol_name", 
      s."Precision", 
      s."Description", 
      s."ContractSize", 
      s."MarginMode", 
      sec."Name" as "security",
      cura."Name" as "MarginCurrency",
      curb."Name" as "ProfitCurrency",
      cura."Name" || \'/\' || curb."Name" as "AggrName"
      from "Symbols" as s
      join "Securities" as sec on s."SecurityFk" = sec."Id"
      join "Currencies" as cura on s."MarginCurrencyFk" = cura."Id"
      join "Currencies" as curb on s."ProfitCurrencyFk" = curb."Id"')
  res <- GetDataFromDB(con, querryS)
  #setDefaultSchema(con)

  res <- as.data.table(res)
  res[, DB := Credstt$postgre_SCHEMA]
  return(res)
}

#get Symbol Last PRICES from TT db 
getAllSymbolsPricesTT <- function(Credstt){
  con <- dbConnect(RPostgres::Postgres(), dbname = Credstt$postgre_DB, user = Credstt$postgre_USER, password = Credstt$postgre_PASSWORD, host = Credstt$postgre_HOST)
  on.exit(dbDisconnect(con))
  setPathToSchema(Credstt$postgre_SCHEMA, con)

  
    querry1 <-paste('select max("Timestamp")from "Rates"')
    maxtime <- GetDataFromDB(con, querry1) #return datatable
    querry2 <-paste('select * from "Rates" where "Timestamp" =',  quoteString(con, maxtime[[1]]))
    res <- GetDataFromDB(con, querry2) # two separated querries faaaster!!!!!
  #setDefaultSchema(con)

  res <- as.data.table(res)
  res[, DB := Credstt$postgre_SCHEMA]
  return(res)
}

#get Last RATE2USD from one TT server (schema)
# Queries MAX("Timestamp") first, then rows at that time (partitioned table: two queries are faster).
# Only rows with ToCurrencyName == "USD": in some schemas (tts_ttlive_eu, tts_ttlive_ukeu)
#   CurrencyConversion is to the domain report currency, NOT USD.
# Returns NULL (with warning) if the table is empty.
getRates2USDOneServer <- function(Credstt){
  con <- dbConnect(RPostgres::Postgres(), dbname = Credstt$postgre_DB, user = Credstt$postgre_USER, password = Credstt$postgre_PASSWORD, host = Credstt$postgre_HOST)
  on.exit(dbDisconnect(con))
  tbl <- paste0(DBI::dbQuoteIdentifier(con, Credstt$postgre_SCHEMA), '."CurrencyConversion"')

  maxtime <- dbGetQuery(con, paste('select max("Timestamp") as max_ts from', tbl))$max_ts
  if (length(maxtime) == 0 || is.na(maxtime[1])) {
    warning(paste0("getRates2USDOneServer: no CurrencyConversion data in ", Credstt$postgre_SCHEMA))
    return(NULL)
  }
  res <- dbGetQuery(con, paste('select * from', tbl, 'where "Timestamp" = $1 and "ToCurrencyName" = $2'),
                    params = list(maxtime[1], "USD"))
  res <- as.data.table(res)
  res[, DB := Credstt$postgre_SCHEMA]
  return(res)
}

#get Last RATE2USD from several TT servers, merged by priority
# serversCreds: NAMED list of server creds in PRIORITY order
#   (in the app: cfg$ReportingServer$servers[cfg$ReportingServer$to_usd_rates_servers]).
#   A single creds list (old call style) is also accepted.
# The first server is the main source (error there = stop); the next ones only ADD currencies
#   missing in the previous ones (error there = warning, server skipped).
# Result: one row per FromCurrencyName; columns DB = schema, Server = config name the rate came from.
getAllRates2USDTT <- function(serversCreds){
  # no servers = no conversion at all = error
  if (length(serversCreds) == 0) stop("no servers for conversion to USD")
  if (!is.null(serversCreds$postgre_SCHEMA)) serversCreds <- list(serversCreds)  # single server
  if (is.null(names(serversCreds))) names(serversCreds) <- sapply(serversCreds, `[[`, "postgre_SCHEMA")
  srvNames <- names(serversCreds)
  if (length(srvNames) == 0 || any(sapply(serversCreds, is.null))) {
    stop(paste("getAllRates2USDTT: unknown rates server(s):", paste(srvNames[sapply(serversCreds, is.null)], collapse = ", ")))
  }

  resList <- list()
  for (i in seq_along(serversCreds)) {
    srv <- srvNames[i]
    one <- if (i == 1) {
      getRates2USDOneServer(serversCreds[[i]])
    } else {
      tryCatch(getRates2USDOneServer(serversCreds[[i]]), error = function(e) {
        warning(paste0("getAllRates2USDTT: skip server ", srv, ": ", substr(e$message, 1, 150)))
        NULL
      })
    }
    if (!is.null(one)) resList[[srv]] <- one[, Server := srv]
  }

  res <- rbindlist(resList, use.names = TRUE, fill = TRUE)
  if (nrow(res) == 0) stop(paste("no USD rates found on servers:", paste(srvNames, collapse = ", ")))
  # keep the rate from the highest-priority server for each currency
  res[, prio := match(Server, srvNames)]
  setorder(res, prio)
  res <- unique(res, by = "FromCurrencyName")[, prio := NULL]

  added <- res[Server != srvNames[1]]
  if (nrow(added) > 0) {
    message("getAllRates2USDTT: ", nrow(res), " rates; added from other servers: ",
            paste(sapply(split(added$FromCurrencyName, added$Server), length), names(split(added$FromCurrencyName, added$Server)), collapse = ", "))
  }
  return(res)
}

##################