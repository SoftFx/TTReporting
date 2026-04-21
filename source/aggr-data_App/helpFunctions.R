library(data.table)
library(RPostgres)

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

#get Last RATE2USD from TT db 
getAllRates2USDTT <- function(Credstt){
  con <- dbConnect(RPostgres::Postgres(), dbname = Credstt$postgre_DB, user = Credstt$postgre_USER, password = Credstt$postgre_PASSWORD, host = Credstt$postgre_HOST)
  on.exit(dbDisconnect(con))
  setPathToSchema(Credstt$postgre_SCHEMA, con)


    querry1 <-paste('select max("Timestamp") from "CurrencyConversion"')
    maxtime <- GetDataFromDB(con, querry1) #return datatable
    querry2 <-paste('select * from "CurrencyConversion" where "Timestamp" =',  quoteString(con, maxtime[[1]]))
    res <- GetDataFromDB(con, querry2) # two separated querries faaaster!!!!! 
  #setDefaultSchema(con)

  res <- as.data.table(res)
  res[, DB := Credstt$postgre_SCHEMA]
  return(res)
}

##################