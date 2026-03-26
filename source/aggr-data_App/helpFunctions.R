library(data.table)
library(RPostgres)

#get aggr data from postgre
getLPexecdataAggr <- function(CredsAggr, From, To){
  ConnectToDB(dbname = CredsAggr$postgre_DB, user = CredsAggr$postgre_USER, password = CredsAggr$postgre_PASSWORD, host = CredsAggr$postgre_HOST)
  setPathToSchema(CredsAggr$postgre_SCHEMA)
  #querry <- paste('SELECT * from', paste0("\"",table_name, "\""), 'WHERE "time" >=', quoteString(from), 'and "time" < ', quoteString(to))
  querry1 <- paste('SELECT * from "lpexecution" WHERE "time" >=', quoteString(From), 'and "time" < ', quoteString(To))
  res1 <- GetDataFromDB(DBCON, querry1)
  
  querry2 <- paste('SELECT * from "lpexecutionfailed" WHERE "time" >=', quoteString(From), 'and "time" < ', quoteString(To))
  res2 <- GetDataFromDB(DBCON, querry2)
  
  querry3 <- paste('SELECT * from "lpexecutiondetails" WHERE "time" >=', quoteString(From), 'and "time" < ', quoteString(To))
  res3 <- GetDataFromDB(DBCON, querry3)
  
  setDefaultSchema()
  
  DissconnectFromDB()
  
  res1 <- as.data.table(res1)[, aggrName := CredsAggr$postgre_SCHEMA]
  res2 <- as.data.table(res2)[, aggrName := CredsAggr$postgre_SCHEMA]
  res3 <- as.data.table(res3)[, aggrName := CredsAggr$postgre_SCHEMA]

  return(list(res1,res2,res3))
}

#get Symbol Properties from TT db 
getAllSymbolsTT <- function(Credstt){
  ConnectToDB(dbname = Credstt$postgre_DB, user = Credstt$postgre_USER, password = Credstt$postgre_PASSWORD, host = Credstt$postgre_HOST)
  setPathToSchema(Credstt$postgre_SCHEMA)
  #querry <- paste('SELECT * from', paste0("\"","Symbols", "\""))
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
  res <- GetDataFromDB(DBCON, querryS)
  setDefaultSchema()
  DissconnectFromDB()
  res <- as.data.table(res)
  res[, DB := Credstt$postgre_SCHEMA]
  return(res)
}

#get Symbol Last PRICES from TT db 
getAllSymbolsPricesTT <- function(Credstt){
  ConnectToDB(dbname = Credstt$postgre_DB, user = Credstt$postgre_USER, password = Credstt$postgre_PASSWORD, host = Credstt$postgre_HOST)
  setPathToSchema(Credstt$postgre_SCHEMA)
  # querryS <-paste('select * from "Rates" where "Timestamp" = (select max("Timestamp")from "Rates")')
  # res <- GetDataFromDB(DBCON, querryS)
    querry1 <-paste('select max("Timestamp")from "Rates"')
    maxtime <- GetDataFromDB(DBCON, querry1) #return datatable
    querry2 <-paste('select * from "Rates" where "Timestamp" =',  quoteString(maxtime[[1]]))
    res <- GetDataFromDB(DBCON, querry2) # two separated querries faaaster!!!!!
  setDefaultSchema()
  DissconnectFromDB()
  res <- as.data.table(res)
  res[, DB := Credstt$postgre_SCHEMA]
  return(res)
}

#get Last RATE2USD from TT db 
getAllRates2USDTT <- function(Credstt){
  ConnectToDB(dbname = Credstt$postgre_DB, user = Credstt$postgre_USER, password = Credstt$postgre_PASSWORD, host = Credstt$postgre_HOST)
  setPathToSchema(Credstt$postgre_SCHEMA)
  # querryS <-paste('select * from "CurrencyConversion" where "Timestamp" = (select max("Timestamp")from "CurrencyConversion")')
  # res <- GetDataFromDB(DBCON, querryS)
    querry1 <-paste('select max("Timestamp") from "CurrencyConversion"')
    maxtime <- GetDataFromDB(DBCON, querry1) #return datatable
    querry2 <-paste('select * from "CurrencyConversion" where "Timestamp" =',  quoteString(maxtime[[1]]))
    res <- GetDataFromDB(DBCON, querry2) # two separated querries faaaster!!!!! 
  setDefaultSchema()
  DissconnectFromDB()
  res <- as.data.table(res)
  res[, DB := Credstt$postgre_SCHEMA]
  return(res)
}

##################