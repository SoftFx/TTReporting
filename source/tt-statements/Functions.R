library(RPostgres)
library(data.table)
library(lubridate)
source('../common/PostgresHost.R') #ConnectToDB/GetDataFromDB/setPathToSchema helpers

# Accounts: Id/Name/Country/Currency/Group/Leverage + AccType (Gross=Type0, Net=Type1) +
# TradingMode/StopOutMode (from Groups, via GroupFk -- not the Group name, which can be
# reused/renamed): together with Accounts.Leverage=1, TradingMode=1 AND StopOutMode=0 identifies
# an "investment account" -- the actual business definition (found 2026-08-26), broader than the
# initially-assumed Groups.PerformOvernight (which only flags the subset of investment accounts
# whose swap-equivalent happens to post as separate Overnight balance rows instead of a
# per-trade Swap that's ~always 0 for them anyway -- kept here too since it's still the right
# signal for THAT specific mechanism, just not for "is this an investment account").
getAccountsTT <- function(Credstt){
  setPathToSchema(Credstt$postgre_SCHEMA)
  querry <- paste('select a."Id" as "Login",
      a."Name",
      a."Country",
      a."Currency",
      a."Group",
      a."Leverage",
      case
        when a."Type" = 0 then \'Gross\'
        when a."Type" = 1 then \'Net\'
      end as "AccType",
      g."PerformOvernight",
      g."TradingMode",
      g."StopOutMode"
    from "Accounts" as a
    left join "Groups" as g on a."GroupFk" = g."Id"
    where not a."Archived" and not a."Deleted"')
  res <- GetDataFromDB(DBCON, querry)
  setDefaultSchema()
  res <- as.data.table(res)
  res[, DB := Credstt$postgre_SCHEMA]
  return(res)
}

# One EOD snapshot per account falling in [dayFrom, dayTo) -- Balance/Equity/Margin/MarginLevel as of that day
getDayAccountSnapshotsTT <- function(Credstt, dayFrom, dayTo){
  setPathToSchema(Credstt$postgre_SCHEMA)
  querry <- paste('select s."Id" as "SnapshotId",
      s."AccountFk" as "Login",
      s."Timestamp",
      s."Domain",
      s."Balance",
      s."BalanceCurrency",
      s."Equity",
      s."Margin",
      s."MarginLevel",
      s."Profit",
      s."Swap",
      s."Commission",
      s."MaxOverdraftAmount",
      s."UsedOverdraftAmount"
    from "DailyAccountSnapshots" as s
    where s."Timestamp" >= ', quoteString(dayFrom), ' and s."Timestamp" < ', quoteString(dayTo))
  res <- GetDataFromDB(DBCON, querry)
  setDefaultSchema()
  res <- as.data.table(res)
  res[, DB := Credstt$postgre_SCHEMA]
  return(res)
}

# Open positions as of the EOD snapshot for that day (used as "Open Trades"/"Open Positions" section)
getDayPositionSnapshotsTT <- function(Credstt, dayFrom, dayTo){
  setPathToSchema(Credstt$postgre_SCHEMA)
  querry <- paste('select p."Id" as "PositionId",
      s."AccountFk" as "Login",
      p."Symbol",
      p."Side",
      p."Amount",
      p."AveragePrice",
      p."Price",
      p."Swap",
      p."Commission",
      p."Profit",
      p."CurrentBestBid",
      p."CurrentBestAsk",
      p."TakeProfit",
      p."StopLoss",
      p."Created",
      sym."ContractSize",
      sym."Precision" as "SymbolPrecision"
    from "DailyPositionSnapshots" as p
    join "DailyAccountSnapshots" as s on p."SnapshotFk" = s."Id"
    left join "Symbols" as sym on p."Symbol" = sym."Name" or p."Symbol" = sym."SymbolId"
    where s."Timestamp" >= ', quoteString(dayFrom), ' and s."Timestamp" < ', quoteString(dayTo))
  res <- GetDataFromDB(DBCON, querry)
  setDefaultSchema()
  res <- as.data.table(res)
  res[, DB := Credstt$postgre_SCHEMA]
  return(res)
}

# Working (pending) orders as of the EOD snapshot for that day
getDayOrderSnapshotsTT <- function(Credstt, dayFrom, dayTo){
  setPathToSchema(Credstt$postgre_SCHEMA)
  querry <- paste('select o."Id" as "OrderId",
      s."AccountFk" as "Login",
      o."Symbol",
      o."Side",
      o."Type",
      o."Amount",
      o."RemainingAmount",
      o."Price",
      o."StopPrice",
      o."TakeProfit",
      o."StopLoss",
      o."Created",
      o."State",
      o."UserComment",
      sym."ContractSize",
      sym."Precision" as "SymbolPrecision"
    from "DailyOrderSnapshots" as o
    join "DailyAccountSnapshots" as s on o."SnapshotFk" = s."Id"
    left join "Symbols" as sym on o."Symbol" = sym."Name" or o."Symbol" = sym."SymbolId"
    where s."Timestamp" >= ', quoteString(dayFrom), ' and s."Timestamp" < ', quoteString(dayTo))
  res <- GetDataFromDB(DBCON, querry)
  setDefaultSchema()
  res <- as.data.table(res)
  res[, DB := Credstt$postgre_SCHEMA]
  return(res)
}

# TrTime-scoped TradeReports for the UTC day. TrType is exclusive per account type:
#   3 = OrderFilled     (Net accounts only: one row per fill, opening or reducing exposure)
#   4 = PositionClosed  (Gross accounts only: one row per fully closed position, open+close pair)
#   5 = Balance         (deposit/withdrawal/dividend/fee -- either account type)
getDayTradeReportsTT <- function(Credstt, dayFrom, dayTo){
  setPathToSchema(Credstt$postgre_SCHEMA)
  querry <- paste('select tr."Id",
      tr."AccountFk" as "Login",
      tr."TrTime",
      tr."TrType",
      tr."TrReason",
      tr."OrderId",
      tr."PosId",
      tr."Side",
      tr."Symbol",
      tr."OrderLastFillAmount",
      tr."PosLastAmount",
      tr."OrderFillPrice",
      tr."PosOpened",
      tr."PosOpenPrice",
      tr."PosClosed",
      tr."PosClosePrice",
      tr."Sl",
      tr."Tp",
      tr."SymbolPrecision",
      tr."Profit",
      tr."Commission",
      tr."Swap",
      tr."Taxes",
      tr."BalanceMovement",
      tr."Balance",
      tr."UserComment",
      a."Name",
      a."Currency",
      a."Leverage",
      a."Group",
      case
        when a."Type" = 0 then \'Gross\'
        when a."Type" = 1 then \'Net\'
      end as "AccType",
      sym."ContractSize"
    from "TradeReports" as tr
    join "Accounts" as a on tr."AccountFk" = a."Id"
    left join "Symbols" as sym on tr."Symbol" = sym."Name" or tr."Symbol" = sym."SymbolId"
    where tr."TrTime" >= ', quoteString(dayFrom), ' and tr."TrTime" < ', quoteString(dayTo), '
      and tr."TrType" in (3, 4, 5)
    order by tr."AccountFk", tr."TrTime"')
  res <- GetDataFromDB(DBCON, querry)
  setDefaultSchema()
  res <- as.data.table(res)
  res[, DB := Credstt$postgre_SCHEMA]
  return(res)
}
