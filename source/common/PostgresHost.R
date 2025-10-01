library(data.table)
library(RPostgres)

DBCON <<- NULL

ConnectToDB <- function(dbname, user, password, host){
  if(is.null(DBCON)){
    DBCON <<- dbConnect(RPostgres::Postgres(),dbname=dbname, user = user, password = password, host = host)
  }else{
    print("Connect alreadey established")
  }
}

DissconnectFromDB <- function() {
  if(!is.null(DBCON)){
    dbDisconnect(DBCON)
    DBCON <<- NULL
  }
}

DBExecuteWrapper <- function(conn, querry){
  if(is.null(conn))
    stop("No connection to DB")
  dbExecute(conn = conn, querry)
}

GetDataFromDB <- function(db, query) {
  if(is.null(db))
    stop("No connection to DB")
  rs = dbSendQuery(db, query)
  data = dbFetch(rs, -1)
  dbClearResult(rs)
  setDT(data)
  return(data)
}

quoteString <- function(string){
  return(paste0('\'', string, '\''))
}

quotePostgresProp <- function(string) {
  return(paste0('\"', string, '\"'))
}

setPathToSchema <- function(schemaName) {
  querry <- paste("SET search_path TO", paste0("\"",schemaName, "\""))
  DBExecuteWrapper(conn = DBCON, querry)
}

setDefaultSchema <- function() {
  querry <- paste("RESET search_path")
  DBExecuteWrapper(conn = DBCON, querry)
}

getCountOfNotes <- function(aggrName, table_name) {
  setPathToSchema(aggrName)
  querry <- paste('SELECT count(*) from', paste0("\"",table_name, "\""))
  res <- GetDataFromDB(DBCON, querry)
  setDefaultSchema()
  return(res)
}

getAllShema <- function() {
  querry <- "SELECT schema_name FROM information_schema.schemata;"
  result <- GetDataFromDB(DBCON, querry)
  result <- result[!(schema_name %in% c("pg_catalog", "public", "information_schema"))]
  return(result$schema_name)
}



