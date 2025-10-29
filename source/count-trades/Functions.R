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

#get all Opened positions (net and gross)
getAllPositionsTT <- function(Credstt){
  ConnectToDB(dbname = Credstt$postgre_DB, user = Credstt$postgre_USER, password = Credstt$postgre_PASSWORD, host = Credstt$postgre_HOST)
  setPathToSchema(Credstt$postgre_SCHEMA)
  querry <-paste('select a."Id" as "Login",
	   case 
	   	when a."Type" = 0 then \'Gross\'
	   	when a."Type" = 1 then \'Net\'
	   end as "AccType",
	   p.*,
	   s."ContractSize",
from "Positions" as p
join "Symbols" as s on p."Symbol" = s."Name"
join "Accounts" as a on p."AccountFk" = a."Id"')
  res <- GetDataFromDB(DBCON, querry)
  setDefaultSchema()
  DissconnectFromDB()
  res <- as.data.table(res)
  res[, DB := Credstt$postgre_SCHEMA]
  return(res)
}


###TT all deals in period
getttdealsPeriod<- function(Credstt, From, To){
  ConnectToDB(dbname = Credstt$postgre_DB, user = Credstt$postgre_USER, password = Credstt$postgre_PASSWORD, host = Credstt$postgre_HOST)
  setPathToSchema(Credstt$postgre_SCHEMA)
  #get all opened pos from gross acc in period
  querry1 <- paste('select a."Id" as "Login",
    a."Name",
    a."Group",
    a."Currency",
    a."Leverage",
    a."InternalComment",
	   case 
	   	when a."Type" = 0 then \'Gross\'
	   	when a."Type" = 1 then \'Net\'
	   end as "AccType",
	  p.*,
	  s."ContractSize"
from "Positions" as p
join "Symbols" as s on p."Symbol" = s."Name"
join "Accounts" as a on p."AccountFk" = a."Id"')
  openedpos <- GetDataFromDB(DBCON, querry1)
  openedpos <- as.data.table(openedpos)
  NEWorders <- openedpos[AccType=="Gross" & Created >= From & Created < To,
                      .("TICKET"= Id, "DATE_TIME" = Created, "LOGIN"=Login, "SYMBOL"=Symbol, "CMD" = Side, "VOLUME" = Amount, 
                        "NAME" = Name, "GROUP" = Group, "CURRENCY" = Currency, "LEVERAGE" = Leverage, "ID" = InternalComment, "VOLUMElot" = Amount/ContractSize, "DEAL_COUNT" = 1)]
  
  #get trades in period
  querry2 <- paste('select tr."TrTime", 
     tr."AccountFk" as "Login",
     case 
	   	when a."Type" = 0 then \'Gross\'
	   	when a."Type" = 1 then \'Net\'
	   end as "AccType",
	   a."Name",
	   a."Group",
	   a."Leverage",
	   a."InternalComment",
	   s."ContractSize",
	   tr."TrType", 
	   tr."BalanceCurrency",
     tr."PosId",
	   tr."OrderId",
	   tr."Side", 
	   tr."Symbol", 
	   tr."OrderType",
	   tr."ParentOrderType",
	   tr."OrderLastFillAmount", 
	   tr."PosLastAmount", 
	   tr."Profit", 
	   tr."Commission",
	   tr."Swap",
	   tr."BalanceMovement",
	   tr."UserComment",
	   tr."PosOpened",
	   tr."PosOpenPrice",
	   tr."PosClosed",
	   tr."PosClosePrice"
from "TradeReports" as tr	   
join "Accounts" as a on tr."AccountFk" = a."Id"
join "Symbols" as s on tr."Symbol" = s."Name"
where "TrTime" >= ', quoteString(From),' and "TrTime" < ', quoteString(To),' and tr."TrType" in(3,4)
order by "TrTime" DESC')
  trades <- GetDataFromDB(DBCON, querry2)
  trades <- as.data.table(trades)  
  trades[AccType=="Gross", DEAL_COUNT := 2]
  trades[AccType=="Net", DEAL_COUNT := 1] 
  trades[, VOLUME := ifelse(AccType=="Gross", PosLastAmount, OrderLastFillAmount)]
  trades <- trades[, .("TICKET"= OrderId,"DATE_TIME" = TrTime, "LOGIN"=Login, "SYMBOL"=Symbol, "CMD" = Side, VOLUME, 
                                           "NAME" = Name, "GROUP" = Group, "CURRENCY" = BalanceCurrency, "LEVERAGE" = Leverage, "ID" = InternalComment, "VOLUMElot" = VOLUME/ContractSize, DEAL_COUNT)]
  setDefaultSchema()
  DissconnectFromDB()
 
  TTdeals <- rbind(NEWorders, trades)
  TTdeals[, ID := as.numeric(ID)]
  TTdeals[, DATE_TIME := as.character(DATE_TIME)]
  TTdeals[, DB := Credstt$postgre_SCHEMA]
  return(TTdeals)
}


### MT4 all trades in period
getMT4tradesPeriod <- function(dbCreds, From, To, TimeOffset){
  dbcon<-dbConnect(RMariaDB::MariaDB(), dbname=dbCreds$dbname, user=dbCreds$user, password=dbCreds$password, host=dbCreds$host, port=dbCreds$port)
  From <- as.character(From + minutes(TimeOffset))  
  To <- as.character(To + minutes(TimeOffset))
  query1 <- sprintf("SELECT mt4_trades.TICKET, mt4_trades.OPEN_TIME, mt4_trades.CLOSE_TIME as DATE_TIME, mt4_trades.LOGIN, mt4_trades.SYMBOL, mt4_trades.CMD, mt4_trades.VOLUME,
                            mt4_users.`NAME`, mt4_users.`GROUP`, mt4_users.CURRENCY, mt4_users.LEVERAGE, mt4_users.ID
                   FROM mt4_trades
                   LEFT JOIN mt4_users
                   ON mt4_trades.LOGIN = mt4_users.LOGIN
                   WHERE ((mt4_trades.`OPEN_TIME`>='%s' AND mt4_trades.`OPEN_TIME`<'%s') OR (mt4_trades.`CLOSE_TIME`>='%s' AND mt4_trades.`CLOSE_TIME`<'%s')) 
                   AND mt4_trades.CMD <= 1", From, To, From, To) 
  rs1 = dbSendQuery(dbcon, query1)
  data = dbFetch(rs1, -1)
  dbClearResult(rs1)
  dbDisconnect(dbcon)
  data <- as.data.table(data)
  data[, ID := as.numeric(ID)]
  data[, VOLUMElot := VOLUME/100]
  data[, DEAL_COUNT := ifelse(OPEN_TIME < From | DATE_TIME== "1970-01-01 00:00:00", 1, 2)]
  data[, DB := dbCreds$dbname]
  data[, OPEN_TIME := NULL]
  return(data)
}

### MT5 all deals in period
getMT5dealsPeriod <- function(dbCreds, From, To, TimeOffset){
  dbcon<-dbConnect(RMariaDB::MariaDB(), dbname=dbCreds$dbname, user=dbCreds$user, password=dbCreds$password, host=dbCreds$host, port=dbCreds$port)
  From <- as.character(From + minutes(TimeOffset))  
  To <- as.character(To + minutes(TimeOffset))
  query <- sprintf(paste("select mt5_deals.Deal, mt5_deals.Time, mt5_deals.Login, mt5_deals.Symbol, mt5_deals.Action, mt5_deals.Volume, 
                         mt5_users.`Name`, mt5_users.`Group`, mt5_groups.Currency, mt5_users.Leverage, mt5_users.ID
                         from mt5_deals
                         left join mt5_users
                         on mt5_deals.Login = mt5_users.Login
                         left join mt5_groups
                         on mt5_users.`Group` = mt5_groups.`Group`
                         where mt5_deals.Time >= '%s' and mt5_deals.Time < '%s' and mt5_deals.Action <= 1"),
                   From, To) 
  rs <- dbSendQuery(dbcon, query)
  deals <- dbFetch(rs, -1)
  dbClearResult(rs)
  dbDisconnect(dbcon)
  deals <- as.data.table(deals)
  setnames(deals, old = c("Deal", "Time", "Login", "Symbol", "Action", "Volume", "Name", "Group", "Currency", "Leverage"), 
           new = c("TICKET", "DATE_TIME", "LOGIN", "SYMBOL", "CMD", "VOLUME", "NAME", "GROUP", "CURRENCY", "LEVERAGE"))
  deals[, VOLUMElot := VOLUME/10000]
  deals[, DEAL_COUNT := 1]
  deals[, ID := as.numeric(ID)]
  deals[, DB := dbCreds$dbname]
  return(deals)
}
##################
