#library(RMySQL)
library(RMariaDB)
library(RPostgres)
library(data.table)
library(lubridate)
#library(yaml)
source('../common/PostgresHost.R') #help functions
source('Functions.R') #get data from MySQL and Postgres
source('../common/RMonitoringClient.R') #send data to HSM
options(warn = -1)
options(digits = 4)
options(scipen=999)
Sys.setenv("TZ" = "UTC")

execute_task_negative_equity <- function(config, last_run_path, task_exec_log_path){
#config <- yaml.load_file("./conf.yaml")
  if (file.exists(last_run_path)){
    last_run <- as.POSIXct(readLines(last_run_path), format = "%Y-%m-%dT%H:%M:%S%z", tz = "UTC")} else{
      file.create(last_run_path)
      last_run <- NA
    }
statusError <- NULL
To <- now()


#get symbols from TT
# print("get symbols from TT:")
# if (length(config$database_connections[["tt"]])>0){
#   tryCatch({ 
#     SymbolsDT <- getAllSymbolsTT(config$database_connections$tt[[1]])}
#     , error = function(e){
#       print(e)
#       statusError <<- append(statusError, paste("Errors:", substr(e$message, 1, 50), "..."))
#     })
# } else {SymbolsDT <- data.table()}

#get rate2USD from TT
print("get rates to USD from TT:")
if (length(config$database_connections[["tt"]])>0){
  tryCatch({ 
    Rates2USD <- getAllRates2USDTT(config$database_connections$tt[[1]])}
    , error = function(e){
      print(e)
      statusError <<- append(statusError, paste("Errors:", substr(e$message, 1, 50), "..."))
    })
} else {Rates2USD <- data.table()}

print("Get equities data:")
### MT4
print("MT4:")
resultsmt4 <- data.table()
resmt4 <- data.table()
if (length(config$database_connections[["mt4"]])>0){
  for(db in names(config$database_connections[["mt4"]])) {
tryCatch({
Credsmt4 <- config$database_connections$mt4[[db]] 
resmt4 <- getMT4userInfo(Credsmt4, To)
print(db)
print(nrow(resmt4))
resultsmt4 <- rbind(resultsmt4, resmt4)
         }, error = function(e){
    print(e)
    statusError <<- append(statusError, paste(db, substr(e$message, 1, 70), "..."))
                               } #e
        ) #tryCatch
 } #for
} #if

### MT5
print("MT5:")
resultsmt5 <- data.table()
resmt5 <- data.table()
if (length(config$database_connections[["mt5"]])>0){
  for(db in names(config$database_connections[["mt5"]])) {
tryCatch({
Credsmt5 <- config$database_connections$mt5[[db]] 
resmt5 <- getMT5userInfo(Credsmt5, To)
print(db)
print(nrow(resmt5))
resultsmt5 <- rbind(resultsmt5, resmt5)
         }, error = function(e){
     print(e)
     statusError <<- append(statusError, paste(db, substr(e$message, 1, 70), "..."))
                               } #e
        ) #tryCatch
 } #for
} #if


###TTlive
print("TT:")
res <- NULL
resultstt <- data.table()
restt <- data.table()
if (length(config$database_connections[["tt"]])>0){
  for(db in names(config$database_connections[["tt"]])) {
    tryCatch({
      Credstt <- config$database_connections$tt[[db]]
      res <- getTTuserInfo(Credstt, To)
      restt <- res[[1]]
      print(db)
      print(nrow(restt))
      resultstt <- rbind(resultstt, restt)
      
      eq_last_update <- res[[2]]
      day_pass <- round(as.numeric(difftime(To, eq_last_update, units = "days")), 0)
      print(paste("Current timestamp:", To))
      print(paste("Last equity update:", eq_last_update))
      if (day_pass >0){statusError <<- append(statusError, paste0(db, ": Equity last update was ", day_pass, " days ago"))}
    }, error = function(e){
      print(e)
      statusError <<- append(statusError, paste(db, substr(e$message, 1, 70), "..."))
    } #e
    ) #tryCatch
  } #for
} #if


Totalresults <- rbind(resultstt, resultsmt4, resultsmt5)

Totalresults[Rates2USD, c("ratetoUSD") := .(i.Value), on = .(CURRENCY = FromCurrencyName)][CURRENCY=="USD", ratetoUSD := 1]
Totalresults[, EQUITY_USD := EQUITY*ratetoUSD]
# Apply filters from setups
Totalresults <- Totalresults[!LOGIN %in% config$business_parameters$excluded_users]
NegativeEquityDT <- Totalresults[EQUITY_USD<=config$business_parameters$equity_valueUSD_threshold, 
                              .(DB, LOGIN, ID, EQUITY, CURRENCY, Timestamp)][order(-EQUITY)]
NegativeEquityDT[, DataText := paste(LOGIN, DB, paste0("ID=", ID), paste0("Equity=", EQUITY), CURRENCY)]

###################################
print("Task result:")
print(paste("Timestamp:", To))
print("NegativeEquityDT:")
print(NegativeEquityDT)
print("statusError:")
print(statusError)

 if (nrow(NegativeEquityDT)== 0) {
   task_res <- data.table(
     DB        = NA_character_,
     LOGIN     = NA_integer_, 
     ID        = NA_real_,
     EQUITY   = NA_real_, 
     CURRENCY = NA_character_,
     Timestamp = To,
     DataText  = NA_character_, 
     err = paste(statusError, collapse = "; "))
    } else { 
   task_res <- NegativeEquityDT[, `:=`(err = paste(statusError, collapse = "; "))]
   }
#### SEND TO HSM 

tryCatch({
  if(length(statusError) > 0){ 
    UpdateBoolSensorValue(productKey = config$monitoring$connection$productKey,  address = config$monitoring$connection$address, port = config$monitoring$connection$port, 
                          path = config$monitoring$connection$path[2],
                          TRUE,
                          status = 1, comment = paste("!!!!NEW__NEGATIVE_Equity:\n", paste(statusError, collapse = "; "))) 
  } #send hsm notify only if was some errors (OK green)
  
  if(nrow(NegativeEquityDT) > 0){
    addcomment <- paste("Negative Equity:")
    UpdateIntSensorValue(productKey = config$monitoring$connection$productKey,  address = config$monitoring$connection$address, port = config$monitoring$connection$port, 
                         path = config$monitoring$connection$path[1], 
                         value = nrow(NegativeEquityDT),
                         status = 1, comment = paste(addcomment, "\n", paste(NegativeEquityDT[, DataText], collapse= ";\n"), sep = "")
    )   
  } else {  #if nrow(resulttable)==0
    
    UpdateIntSensorValue(productKey = config$monitoring$connection$productKey,  address = config$monitoring$connection$address, port = config$monitoring$connection$port, 
                         path = config$monitoring$connection$path[1], 
                         value = 0, status = 1, comment = "") #No big lots
  }
}, error = function(e){
  print("HSMconnErr:")
  print(e)
}) #tryCatch

writeLines(format(To, "%Y-%m-%dT%H:%M:%S%z"), last_run_path)

if (file.exists(task_exec_log_path)){
  fwrite(task_res, task_exec_log_path, append = TRUE)} else {
    fwrite(task_res, task_exec_log_path)
  }
######

return(list(TRUE, To))
}