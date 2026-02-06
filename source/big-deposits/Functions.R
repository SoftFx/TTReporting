#get meta time offset (from mt5)
META5Timeoffset <- function(dbCreds){
  db<-dbConnect(RMariaDB::MariaDB(), dbname=dbCreds$dbname, user=dbCreds$user, password=dbCreds$password, host=dbCreds$host, port=dbCreds$port)
  query <- sprintf("select * from mt5_time")
  rs = dbSendQuery(db, query)
  mt5_time = dbFetch(rs, -1)
  TimeOffset <- mt5_time$TimeZone
  dbClearResult(rs)
  dbDisconnect(db)
  return(TimeOffset) 
}

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

###TT deposits
getttdepositPeriod<- function(Credstt, From, To){
  ConnectToDB(dbname = Credstt$postgre_DB, user = Credstt$postgre_USER, password = Credstt$postgre_PASSWORD, host = Credstt$postgre_HOST)
  setPathToSchema(Credstt$postgre_SCHEMA)
  #get deposits in period
  querry <- paste('select tr."TrTime", 
     tr."OrderId",
     tr."AccountFk" as "Login",
	   a."Name",
	   a."Country",
	   a."InternalComment",
	   tr."BalanceCurrency",
	   tr."BalanceMovement"
from "TradeReports" as tr	   
join "Accounts" as a on tr."AccountFk" = a."Id"
where "TrTime" >= ', quoteString(From),' and "TrTime" < ', quoteString(To),' and tr."TrType" in(5) and tr."TrReason" <>11
order by "TrTime" DESC')
  deposits <- GetDataFromDB(DBCON, querry)
  setDefaultSchema()
  DissconnectFromDB() 
  
  deposits <- as.data.table(deposits)  
  TTdeposits <- deposits[, .("TICKET"= OrderId, "DATE_TIME" = TrTime, "LOGIN" = Login, "DEPOSIT" = BalanceMovement,  "CURRENCY"= BalanceCurrency,
                          "NAME" = Name, "COUNTRY" = Country, "ID" = InternalComment)][order(DATE_TIME)]
  TTdeposits[, ID := as.numeric(ID)]
  TTdeposits[, TYPE := ifelse(DEPOSIT>0, "Deposit", "Withdraw")]
  TTdeposits[, DB := Credstt$postgre_SCHEMA]
  return(TTdeposits)
}


### MT4 
getMT4depositPeriod <- function(dbCreds, From, To, TimeOffset){
  dbcon<-dbConnect(RMariaDB::MariaDB(), dbname=dbCreds$dbname, user=dbCreds$user, password=dbCreds$password, host=dbCreds$host, port=dbCreds$port)
  From <- as.character(From + minutes(TimeOffset))  
  To <- as.character(To + minutes(TimeOffset))
  query1 <- sprintf("SELECT mt4_trades.TICKET, mt4_trades.CLOSE_TIME as DATE_TIME, mt4_trades.LOGIN, mt4_trades.PROFIT as DEPOSIT,
                            mt4_users.CURRENCY, mt4_users.`NAME`, mt4_users.COUNTRY, mt4_users.ID
                   FROM mt4_trades
                   LEFT JOIN mt4_users
                   ON mt4_trades.LOGIN = mt4_users.LOGIN
                   WHERE mt4_trades.OPEN_TIME >= '%s 'AND mt4_trades.OPEN_TIME < '%s' AND mt4_trades.CMD = 6", From, To) # AND mt4_users.`GROUP` not like 'cent-%%' and mt4_users.`GROUP` not like 'in-%%'  double %% !!!! 
  rs1 = dbSendQuery(dbcon, query1)
  depositsMT4 = dbFetch(rs1, -1)
  dbClearResult(rs1)
  dbDisconnect(dbcon)
  
  depositsMT4 <- as.data.table(depositsMT4)
  depositsMT4[, ID := as.numeric(ID)]
  depositsMT4[, TYPE := ifelse(DEPOSIT>0, "Deposit", "Withdraw")]
  depositsMT4[, DB := dbCreds$dbname]
  return(depositsMT4)
}



### MT5
getMT5depositPeriod <- function(dbCreds, From, To, TimeOffset){
  dbcon<-dbConnect(RMariaDB::MariaDB(), dbname=dbCreds$dbname, user=dbCreds$user, password=dbCreds$password, host=dbCreds$host, port=dbCreds$port)
  From <- as.character(From + minutes(TimeOffset))  
  To <- as.character(To + minutes(TimeOffset))
  query <- sprintf(paste("select mt5_deals.Deal as TICKET, mt5_deals.Time as DATE_TIME, mt5_deals.Login as LOGIN, mt5_deals.Profit as DEPOSIT, 
                         mt5_groups.Currency as CURRENCY, mt5_users.`Name` as NAME, mt5_users.`Country` as COUNTRY, mt5_users.ID
                         from mt5_deals
                         left join mt5_users
                         on mt5_deals.Login = mt5_users.Login
                         left join mt5_groups
                         on mt5_users.`Group` = mt5_groups.`Group`
                         where mt5_deals.Action = 2 and mt5_deals.Time >= '%s' and mt5_deals.Time < '%s'"),
                    From, To)
  rs <- dbSendQuery(dbcon, query)
  depositsMT5 <- dbFetch(rs, -1)
  dbClearResult(rs)
  dbDisconnect(dbcon)
  
  depositsMT5 <- as.data.table(depositsMT5)
  
  depositsMT5[, ID := as.numeric(ID)]
  depositsMT5[, TYPE := ifelse(DEPOSIT>0, "Deposit", "Withdraw")]
  depositsMT5[, DB := dbCreds$dbname]
  return(depositsMT5)
}

##################
