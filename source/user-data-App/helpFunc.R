library(data.table)
library(DBI)
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
      if (Sys.getenv(key) == "") do.call(Sys.setenv, setNames(list(val), key))
    }
  }
  # Load YAML and recursively replace ${VAR} in all string values — substitutes env var values into the config
  cfg <- yaml::yaml.load_file(yaml_path)
  subst <- function(s) {
    while (grepl("\\$\\{\\w+\\}", s)) {
      var <- sub(".*\\$\\{(\\w+)\\}.*", "\\1", s)
      s <- sub(paste0("\\$\\{", var, "\\}"), Sys.getenv(var, ""), s)
    }
    s
  }
  resolve <- function(x) if (is.character(x)) subst(x) else if (is.list(x)) lapply(x, resolve) else x
  resolve(cfg)
}

# --- Connection helpers ---
.make_con_postgres <- function(creds) {
  DBI::dbConnect(RPostgres::Postgres(),
    dbname   = creds$postgre_DB,
    user     = creds$postgre_USER,
    password = creds$postgre_PASSWORD,
    host     = creds$postgre_HOST
  )
}

.make_con_mysql <- function(creds) {
  DBI::dbConnect(RMariaDB::MariaDB(),
    dbname   = creds$dbname,
    user     = creds$user,
    password = creds$password,
    host     = creds$host,
    port     = as.integer(creds$port)
  )
}

# --- MySQL functions ---

META5Timeoffset <- function(dbCreds) {
  con <- .make_con_mysql(dbCreds)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  res <- DBI::dbGetQuery(con, "select * from mt5_time")
  return(res$TimeZone)
}

getMT4userTradesPeriod <- function(dbCreds, UserLogin, From, To, TimeOffset) {
  con <- .make_con_mysql(dbCreds)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  From <- From + minutes(TimeOffset)
  To <- To + minutes(TimeOffset)
  From_sql <- as.character(From)
  To_sql <- as.character(To)
  sql <- DBI::sqlInterpolate(con,
    "SELECT mt4_trades.TICKET, mt4_trades.OPEN_TIME, mt4_trades.CLOSE_TIME, mt4_trades.LOGIN, mt4_trades.SYMBOL, mt4_trades.CMD, mt4_trades.VOLUME,
                            mt4_trades.OPEN_PRICE, mt4_trades.CLOSE_PRICE,  mt4_trades.CONV_RATE1, mt4_trades.CONV_RATE2,
                            mt4_trades.COMMISSION, mt4_trades.SWAPS, mt4_trades.PROFIT, mt4_trades.COMMENT,
                            mt4_users.`NAME`, mt4_users.`GROUP`, mt4_users.CURRENCY, mt4_users.LEVERAGE, mt4_users.ID
                   FROM mt4_trades
                   LEFT JOIN mt4_users
                   ON mt4_trades.LOGIN = mt4_users.LOGIN
                   WHERE mt4_trades.LOGIN = ?UserLogin AND ((mt4_trades.`OPEN_TIME`>=?From AND mt4_trades.`OPEN_TIME`<?To) OR (mt4_trades.`CLOSE_TIME`>=?From AND mt4_trades.`CLOSE_TIME`<?To))
                   ", UserLogin = UserLogin, From = From_sql, To = To_sql)
  data <- DBI::dbGetQuery(con, sql)
  data <- as.data.table(data)
  data[!CMD%in%c(6,7) & grepl("cancelled", COMMENT)==F, VOLUMElot := VOLUME/100]
  # trade_status: relation of open/close to selected period
  data[!CMD %in% c(6,7) & !grepl("cancelled", COMMENT), trade_status := fcase(
    OPEN_TIME >= From & CLOSE_TIME < To & CLOSE_TIME != "1970-01-01 00:00:00", "open_close",
    OPEN_TIME >= From & (CLOSE_TIME >= To | CLOSE_TIME == "1970-01-01 00:00:00"), "open",
    OPEN_TIME < From, "close",
    default = NA_character_
  )]
  # DEAL_COUNT: 2 only if full cycle (open+close) in period, else 1
  data[!CMD %in% c(6,7) & !grepl("cancelled", COMMENT) & trade_status == "open_close", DEAL_COUNT := 2]
  data[!CMD %in% c(6,7) & !grepl("cancelled", COMMENT) & trade_status %in% c("open", "close"), DEAL_COUNT := 1]
  data[, DB := dbCreds$dbname]
  return(data)
}

getMT5userdealsPeriod <- function(dbCreds, UserLogin, From, To, TimeOffset) {
  con <- .make_con_mysql(dbCreds)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  From <- as.character(From + minutes(TimeOffset))
  To <- as.character(To + minutes(TimeOffset))
  sql <- DBI::sqlInterpolate(con,
    "select mt5_deals.Deal, mt5_deals.Order, mt5_deals.PositionID, mt5_deals.Time, mt5_deals.Login, mt5_deals.Symbol, mt5_deals.Action, mt5_deals.Entry, mt5_deals.Volume, mt5_deals.Price, mt5_deals.RateProfit, mt5_deals.RateMargin, mt5_deals.Commission, mt5_deals.Storage as Swap, mt5_deals.Profit, mt5_deals.Comment,
                         mt5_users.`Name`, mt5_users.`Group`, mt5_groups.Currency, mt5_users.Leverage, mt5_users.ID
                         from mt5_deals
                         left join mt5_users
                         on mt5_deals.Login = mt5_users.Login
                         left join mt5_groups
                         on mt5_users.`Group` = mt5_groups.`Group`
                         where mt5_deals.Login = ?UserLogin and mt5_deals.Time >= ?From and mt5_deals.Time < ?To",
    UserLogin = UserLogin, From = From, To = To)
  deals <- DBI::dbGetQuery(con, sql)
  deals <- as.data.table(deals)
  deals[Action <=1, VOLUMElot := Volume/10000]
  deals[Action <=1, DEAL_COUNT := 1]
  deals[, DB := dbCreds$dbname]
  return(deals)
}

getMT5dealsPeriod <- function(dbCreds, From, To, TimeOffset) {
  con <- .make_con_mysql(dbCreds)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  From <- as.character(From + minutes(TimeOffset))
  To <- as.character(To + minutes(TimeOffset))
  sql <- DBI::sqlInterpolate(con,
    "select mt5_deals.Deal, mt5_deals.Time, mt5_deals.Login, mt5_deals.Symbol, mt5_deals.Action, mt5_deals.Volume,
                         mt5_users.`Name`, mt5_users.`Group`, mt5_groups.Currency, mt5_users.Leverage, mt5_users.ID
                         from mt5_deals
                         left join mt5_users
                         on mt5_deals.Login = mt5_users.Login
                         left join mt5_groups
                         on mt5_users.`Group` = mt5_groups.`Group`
                         where mt5_deals.Time >= ?From and mt5_deals.Time < ?To and mt5_deals.Action <= 1",
    From = From, To = To)
  deals <- DBI::dbGetQuery(con, sql)
  deals <- as.data.table(deals)
  setnames(deals, old = c("Deal", "Time", "Login", "Symbol", "Action", "Volume", "Name", "Group", "Currency", "Leverage"),
                    new = c("TICKET", "DATE_TIME", "LOGIN", "SYMBOL", "CMD", "VOLUME", "NAME", "GROUP", "CURRENCY", "LEVERAGE"))
  deals[, VOLUMElot := VOLUME/10000]
  deals[, DEAL_COUNT := 1]
  deals[, ID := as.numeric(ID)]
  deals[, DB := dbCreds$dbname]
  return(deals)
}

# --- PostgreSQL (TT) functions ---

# Fetch rates + prices in one connection
getRefDataTT <- function(creds, con = NULL) {
  if (is.null(con)) { con <- .make_con_postgres(creds); on.exit(DBI::dbDisconnect(con), add = TRUE) }
  DBI::dbExecute(con, paste0('SET search_path TO ', DBI::dbQuoteIdentifier(con, creds$postgre_SCHEMA)))

  maxtime_rates <- DBI::dbGetQuery(con, 'select max("Timestamp") from "Rates"')
  symbolsPrices <- DBI::dbGetQuery(con, paste0('select * from "Rates" where "Timestamp" = ', DBI::dbQuoteLiteral(con, maxtime_rates[[1]])))
  symbolsPrices <- as.data.table(symbolsPrices)
  symbolsPrices[, DB := creds$postgre_SCHEMA]

  maxtime_cc <- DBI::dbGetQuery(con, 'select max("Timestamp") from "CurrencyConversion"')
  rate2usd <- DBI::dbGetQuery(con, paste0('select * from "CurrencyConversion" where "Timestamp" = ', DBI::dbQuoteLiteral(con, maxtime_cc[[1]])))
  rate2usd <- as.data.table(rate2usd)
  rate2usd[, DB := creds$postgre_SCHEMA]

  return(list(rate2usd = rate2usd, symbolsPrices = symbolsPrices))
}

getAllSymbolsTT <- function(creds, con = NULL) {
  if (is.null(con)) { con <- .make_con_postgres(creds); on.exit(DBI::dbDisconnect(con), add = TRUE) }
  DBI::dbExecute(con, paste0('SET search_path TO ', DBI::dbQuoteIdentifier(con, creds$postgre_SCHEMA)))
  sql <- 'select s."Name" as symbol_name, s."SymbolId" as symbol_id,
      s."Precision", s."Description", s."ContractSize", s."MarginMode",
      sec."Name" as security,
      cura."Name" as "MarginCurrency",
      curb."Name" as "ProfitCurrency",
      cura."Name" || \'/\' || curb."Name" as "AggrName"
      from "Symbols" as s
      join "Securities" as sec on s."SecurityFk" = sec."Id"
      join "Currencies" as cura on s."MarginCurrencyFk" = cura."Id"
      join "Currencies" as curb on s."ProfitCurrencyFk" = curb."Id"'
  res <- DBI::dbGetQuery(con, sql)
  res <- as.data.table(res)
  res[, DB := creds$postgre_SCHEMA]
  return(res)
}

getAllSymbolsPricesTT <- function(creds, con = NULL) {
  if (is.null(con)) { con <- .make_con_postgres(creds); on.exit(DBI::dbDisconnect(con), add = TRUE) }
  DBI::dbExecute(con, paste0('SET search_path TO ', DBI::dbQuoteIdentifier(con, creds$postgre_SCHEMA)))
  maxtime <- DBI::dbGetQuery(con, 'select max("Timestamp") from "Rates"')
  res <- DBI::dbGetQuery(con, paste0('select * from "Rates" where "Timestamp" = ', DBI::dbQuoteLiteral(con, maxtime[[1]])))
  res <- as.data.table(res)
  res[, DB := creds$postgre_SCHEMA]
  return(res)
}

getAllRates2USDTT <- function(creds, con = NULL) {
  if (is.null(con)) { con <- .make_con_postgres(creds); on.exit(DBI::dbDisconnect(con), add = TRUE) }
  DBI::dbExecute(con, paste0('SET search_path TO ', DBI::dbQuoteIdentifier(con, creds$postgre_SCHEMA)))
  maxtime <- DBI::dbGetQuery(con, 'select max("Timestamp") from "CurrencyConversion"')
  res <- DBI::dbGetQuery(con, paste0('select * from "CurrencyConversion" where "Timestamp" = ', DBI::dbQuoteLiteral(con, maxtime[[1]])))
  res <- as.data.table(res)
  res[, DB := creds$postgre_SCHEMA]
  return(res)
}

getAllPositionsTT <- function(creds, con = NULL) {
  if (is.null(con)) { con <- .make_con_postgres(creds); on.exit(DBI::dbDisconnect(con), add = TRUE) }
  DBI::dbExecute(con, paste0('SET search_path TO ', DBI::dbQuoteIdentifier(con, creds$postgre_SCHEMA)))
  sql <- paste('select a."Id" as "Login",
    case when a."Type" = 0 then \'Gross\' when a."Type" = 1 then \'Net\' end as "AccType",
    p.*, s."ContractSize"
    from "Positions" as p
    join "Symbols" as s on p."Symbol" = s."Name"
    join "Accounts" as a on p."AccountFk" = a."Id"')
  res <- DBI::dbGetQuery(con, sql)
  res <- as.data.table(res)
  res[, DB := creds$postgre_SCHEMA]
  return(res)
}

getTradeReportPeriod <- function(creds, UserLogin, From, To, con = NULL) {
  if (is.null(con)) { con <- .make_con_postgres(creds); on.exit(DBI::dbDisconnect(con), add = TRUE) }
  DBI::dbExecute(con, paste0('SET search_path TO ', DBI::dbQuoteIdentifier(con, creds$postgre_SCHEMA)))

  querry <- DBI::sqlInterpolate(con,
    'select tr."TrTime",
       tr."AccountFk" as "Login",
		   d."Name" as "Domain",
		   a."Type",
		   case
		   	when a."Type" = 0 then \'Gross\'
		   	when a."Type" = 1 then \'Net\'
		   end as "AccType",
		   a."Name",
		   a."Group",
		   tr."TrType",
		   tr."BalanceCurrency",
	       tr."PosId",
	       tr."OrderId",
	       tr."Side",
	       tr."Symbol",
	       s."ContractSize",
	       case
		   	when a."Type" = 0 then tr."PosLastAmount"*2/s."ContractSize"
		   	when a."Type" = 1 then tr."OrderLastFillAmount"/s."ContractSize"
		   end as "TVinLot",
	       case
		   	when a."Type" = 0 and tr."TrType" <> 5 then 2
		   	when a."Type" = 1 and tr."TrType" <> 5 then 1
		   end as "Ntrades",
	       case
		   	when a."Type" = 0 then tr."PosLastAmount"*2
		   	when a."Type" = 1 then tr."OrderLastFillAmount"
		   end as "TVmargincurr",
	       case
		   	when a."Type" = 0 then tr."PosLastAmount"*2*"MarginCurrencyToBalanceConversionRate"
		   	when a."Type" = 1 then tr."OrderLastFillAmount"*"MarginCurrencyToBalanceConversionRate"
		   end as "TVbalcur",
	       tr."OrderType",
	       tr."ParentOrderType",
	       tr."OrderLastFillAmount",
	       tr."PosLastAmount",
	       tr."Profit",
	       tr."Commission",
	       tr."Swap",
	       tr."BalanceMovement",
	       case
		   	when tr."TrType" = 5 and tr."TrReason" = 11 then tr."BalanceMovement"
		   end as "Dividend",
	       case
		   	when tr."TrType" = 5 and tr."TrReason" <> 11 then tr."BalanceMovement"
		   end as "Deposit",
	       tr."UserComment",
	       tr."ManagerComment",
	       tr."PosOpened",
	       tr."PosOpenPrice",
	       tr."PosClosed",
	       tr."PosClosePrice",
	       tr."ClientApp"
	from "TradeReports" as tr
	left join "Accounts" as a on tr."AccountFk" = a."Id"
	left join "Symbols" as s on tr."Symbol" = s."Name"
	left join "Groups" as g on a."GroupFk" = g."Id"
	left join "Domains" as d on g."DomainFk" = d."Id"
	where "TrTime" >= ?From and "TrTime" < ?To and tr."TrType" in(3,4,5) and tr."AccountFk" = ?UserLogin
		order by "TrTime" ASC',
    From = From, To = To, UserLogin = UserLogin)

  trades <- DBI::dbGetQuery(con, querry)
  trades <- as.data.table(trades)
  trades[, DB := creds$postgre_SCHEMA]
  return(trades)
}

# --- Open positions functions ---

getMT4userOpenPositions <- function(dbCreds, UserLogin) {
  con <- .make_con_mysql(dbCreds)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  sql <- DBI::sqlInterpolate(con,
    "SELECT u.`GROUP` AS `Group`,
                         t.LOGIN AS Login,
                         u.`NAME` AS Name,
                         u.CURRENCY AS BalanceCurrency,
                         t.SYMBOL AS Symbol,
                         SUM(t.SWAPS) AS SwapOpen
                  FROM mt4_trades t
                  JOIN mt4_users u ON u.LOGIN = t.LOGIN
                  WHERE t.LOGIN = ?UserLogin
                    AND t.CLOSE_TIME = '1970-01-01 00:00:00'
                    AND t.SWAPS <> 0
                    AND t.CMD NOT IN (6,7)
                  GROUP BY u.`GROUP`, t.LOGIN, u.`NAME`, u.CURRENCY, t.SYMBOL",
    UserLogin = UserLogin)
  res <- DBI::dbGetQuery(con, sql)
  res <- as.data.table(res)
  return(res)
}

getMT5userOpenPositions <- function(dbCreds, UserLogin) {
  con <- .make_con_mysql(dbCreds)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  sql <- DBI::sqlInterpolate(con,
    "SELECT u.`Group`,
                         p.Login,
                         u.`Name`,
                         g.Currency AS BalanceCurrency,
                         p.Symbol,
                         SUM(p.Storage) AS SwapOpen
                  FROM mt5_positions p
                  JOIN mt5_users u ON u.Login = p.Login
                  JOIN mt5_groups g ON g.`Group` = u.`Group`
                  WHERE p.Login = ?UserLogin
                    AND p.Storage <> 0
                  GROUP BY u.`Group`, p.Login, u.`Name`, g.Currency, p.Symbol",
    UserLogin = UserLogin)
  res <- DBI::dbGetQuery(con, sql)
  res <- as.data.table(res)
  return(res)
}

getTTuserOpenPositions <- function(creds, UserLogin, con = NULL) {
  if (is.null(con)) { con <- .make_con_postgres(creds); on.exit(DBI::dbDisconnect(con), add = TRUE) }
  DBI::dbExecute(con, paste0('SET search_path TO ', DBI::dbQuoteIdentifier(con, creds$postgre_SCHEMA)))
  sql <- DBI::sqlInterpolate(con,
    'SELECT a."Group",
                         a."Id" AS "Login",
                         a."Name",
                         a."Currency" AS "BalanceCurrency",
                         p."Symbol",
                         SUM(p."Swap") AS "SwapOpen"
                  FROM "Positions" p
                  JOIN "Accounts" a ON a."Id" = p."AccountFk"
                  WHERE a."Id" = ?UserLogin
                    AND p."Swap" <> 0
                  GROUP BY a."Group", a."Id", a."Name", a."Currency", p."Symbol"',
    UserLogin = UserLogin)
  res <- DBI::dbGetQuery(con, sql)
  res <- as.data.table(res)
  return(res)
}
