

#get Symbol Properties from TT db (after db connection)
getAllSymbolsTT <- function(Credstt){
  ConnectToDB(dbname = Credstt$postgre_DB, user = Credstt$postgre_USER, password = Credstt$postgre_PASSWORD, host = Credstt$postgre_HOST)
  setPathToSchema(Credstt$postgre_SCHEMA)
  #querry <- paste('SELECT * from', paste0("\"","Symbols", "\""))
  querry <-paste('select s."Name" as "symbol_name", 
      s."Precision", 
      s."Description", 
      s."ContractSize", 
      s."MarginMode", 
      sec."Name" as "security",
      cura."Name" as "MarginCurrency",
      curb."Name" as "ProfitCurrency"
      from "Symbols" as s
      join "Securities" as sec on s."SecurityFk" = sec."Id"
      join "Currencies" as cura on s."MarginCurrencyFk" = cura."Id"
      join "Currencies" as curb on s."ProfitCurrencyFk" = curb."Id"')
  res <- GetDataFromDB(DBCON, querry)
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

###TT equities
getTTuserInfo<- function(Credstt, To){
  ConnectToDB(dbname = Credstt$postgre_DB, user = Credstt$postgre_USER, password = Credstt$postgre_PASSWORD, host = Credstt$postgre_HOST)
  setPathToSchema(Credstt$postgre_SCHEMA)
  querry1 <-paste('select max("Timestamp") from "Equities"')
  maxtime <- GetDataFromDB(DBCON, querry1) #return datatable
  #get last equities
  querry2 <-paste('select eq."AccountFk" as "LOGIN",
                          a."Name" as "NAME",
                          a."Group" as "GROUP",
                          a."Currency" as "CURRENCY",
                          a."Leverage" as "LEVERAGE",
                          a."InternalComment" as "ID",
                          eq."Equity" as "EQUITY",
                          a."Balance" as "BALANCE",
                          eq."Timestamp"
                  from "Equities" as eq
                  left join "Accounts" as a on eq."AccountFk" = a."Id"
                  where "Timestamp" =',  quoteString(maxtime[[1]]), 'and eq."Equity" < 0' )
  data <- GetDataFromDB(DBCON, querry2) # two separated querries faaaster!!!!! 
  setDefaultSchema()
  DissconnectFromDB() 
  data <- as.data.table(data)  
  data[, DB := Credstt$postgre_SCHEMA]
  return(list(data, maxtime[[1]]))
  }


### MT4 equities
getMT4userInfo <- function(dbCreds, To){
  dbcon<-dbConnect(RMariaDB::MariaDB(), dbname=dbCreds$dbname, user=dbCreds$user, password=dbCreds$password, host=dbCreds$host, port=dbCreds$port)
  query1 <- sprintf("SELECT mt4_users.LOGIN, mt4_users.`NAME`, mt4_users.`GROUP`, mt4_users.CURRENCY, mt4_users.LEVERAGE, mt4_users.ID, mt4_users.EQUITY, mt4_users.BALANCE
                   FROM mt4_users
                   WHERE mt4_users.EQUITY < 0 ")
  rs1 = dbSendQuery(dbcon, query1)
  data = dbFetch(rs1, -1)
  dbClearResult(rs1)
  dbDisconnect(dbcon)
  data <- as.data.table(data)
  data[, Timestamp := To]
  data[, DB := dbCreds$dbname]
  return(data)
}


### MT5 equities
getMT5userInfo <- function(dbCreds, To){
  dbcon<-dbConnect(RMariaDB::MariaDB(), dbname=dbCreds$dbname, user=dbCreds$user, password=dbCreds$password, host=dbCreds$host, port=dbCreds$port)
  query <- sprintf("select mt5_users.Login, mt5_users.`Name`, mt5_users.`Group`, mt5_groups.Currency, mt5_users.Leverage, mt5_users.ID, mt5_accounts.Equity, mt5_accounts.Balance
                         from mt5_users
                         left join mt5_accounts
                         on mt5_users.Login = mt5_accounts.Login
                         left join mt5_groups
                         on mt5_users.`Group` = mt5_groups.`Group`
                         where mt5_accounts.Equity < 0 ")
  
  rs <- dbSendQuery(dbcon, query)
  data <- dbFetch(rs, -1)
  dbClearResult(rs)
  dbDisconnect(dbcon)
  data <- as.data.table(data)
  setnames(data, old = c("Login", "Name", "Group", "Currency", "Leverage", "Equity", "Balance"), 
           new = c( "LOGIN", "NAME", "GROUP", "CURRENCY", "LEVERAGE", "EQUITY", "BALANCE"))
  data[, Timestamp := To]
  data[, DB := dbCreds$dbname]
  return(data)
}
##################
