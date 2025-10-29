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

execute_task_count_of_deals <- function(config, last_run_path, task_exec_log_path){
#config <- yaml.load_file("./conf.yaml")
  if (file.exists(last_run_path)){
    last_run <- as.POSIXct(readLines(last_run_path), format = "%Y-%m-%dT%H:%M:%S%z", tz = "UTC")} else{
      file.create(last_run_path)
      last_run <- NA
    }
statusError <- NULL
To <- now()

#last run time validation: if no data - set current time 
if (!is.na(last_run)&&length(last_run)==1) {
  From <- last_run} else {From <- To}

#get meta time offset
if (length(config$database_connections[["mt5"]])>0){
  tryCatch({ 
    TimeOffset <- META5Timeoffset(config$database_connections$mt5[[1]])}
    , error = function(e){
      print(e)
      statusError <<- append(statusError, paste("Errors:", substr(e$message, 1, 50), "..."))
    })
} else {TimeOffset <- 0L}


### MT4 
resultsmt4 <- data.table()
resmt4 <- data.table()
if (length(config$database_connections[["mt4"]])>0){
  for(db in names(config$database_connections[["mt4"]])) {
tryCatch({
Credsmt4 <- config$database_connections$mt4[[db]] 
resmt4 <- getMT4tradesPeriod(Credsmt4, From, To, TimeOffset)
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
resultsmt5 <- data.table()
resmt5 <- data.table()
if (length(config$database_connections[["mt5"]])>0){
  for(db in names(config$database_connections[["mt5"]])) {
tryCatch({
Credsmt5 <- config$database_connections$mt5[[db]] 
resmt5 <- getMT5dealsPeriod(Credsmt5, From, To, TimeOffset)
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
resultstt <- data.table()
restt <- data.table()
if (length(config$database_connections[["tt"]])>0){
  for(db in names(config$database_connections[["tt"]])) {
    tryCatch({
      Credstt <- config$database_connections$tt[[db]]
      restt <- getttdealsPeriod(Credstt, From, To)
      print(db)
      print(nrow(restt))
      resultstt <- rbind(resultstt, restt)
    }, error = function(e){
      print(e)
      statusError <<- append(statusError, paste(db, substr(e$message, 1, 70), "..."))
    } #e
    ) #tryCatch
  } #for
} #if


Totalresults <- rbind(resultstt, resultsmt4, resultsmt5)
# Apply filters from setups
Totalresults <- Totalresults[!LOGIN %in% config$business_parameters$excluded_users]

summary <- Totalresults[, .("TradesCOUNT" = sum(DEAL_COUNT, na.rm = T)), by = .(DB, LOGIN, ID)]
#summary[, PERIOD := difftime(To, From)]

CountOfDealsDT <- summary[TradesCOUNT>=config$business_parameters$deals_count_threshold, .(DB, LOGIN, ID, TradesCOUNT)][order(DB, -TradesCOUNT)]
#CountOfDealsDT[, PERIOD := as.character(seconds_to_period(round(as.numeric(difftime(To, From), units = "secs"), 0)))]
CountOfDealsDT[, DataText:= paste(LOGIN, DB, TradesCOUNT, "deals", "ID=", ID)]

print(CountOfDealsDT)
 if (nrow(CountOfDealsDT)== 0) {
   task_res <- data.table(
     DB        = NA_character_,
     LOGIN     = NA_integer_, 
     TradesCOUNT = NA_integer_,
     ID        = NA_real_,
     DataText = NA_character_, 
     from = From, to = To, err = paste(statusError, collapse = "; "))
    } else { 
   task_res <- CountOfDealsDT[, `:=`(from = From, to = To, err = paste(statusError, collapse = "; "))]
   }
#### SEND TO HSM 
#[3], 3 -test  [1] real
tryCatch({
  if(length(statusError) > 0){ 
    UpdateBoolSensorValue(productKey = config$monitoring$connection$productKey,  address = config$monitoring$connection$address, port = config$monitoring$connection$port, 
                          path = config$monitoring$connection$path[2],
                          TRUE,
                          status = 1, comment = paste("!!!!NEW__COUNT_of_DEALS:\n", paste(statusError, collapse = "; "))) 
  } #send hsm notify only if was some errors (OK green)
  
  if(nrow(CountOfDealsDT) > 0){
    addcomment <- paste("Deals Count >", config$business_parameters$deals_count_threshold, "for the previous", as.character(seconds_to_period(round(as.numeric(difftime(To, From), units = "secs"), 0))), ":")
    UpdateIntSensorValue(productKey = config$monitoring$connection$productKey,  address = config$monitoring$connection$address, port = config$monitoring$connection$port, 
                         path = config$monitoring$connection$path[3], #3 -test
                         value = nrow(CountOfDealsDT),
                         status = 1, comment = paste(addcomment, "\n", paste(CountOfDealsDT[, DataText], collapse= ";\n"), sep = "")
    )   
  } else {  #if nrow(resulttable)==0
    
    UpdateIntSensorValue(productKey = config$monitoring$connection$productKey,  address = config$monitoring$connection$address, port = config$monitoring$connection$port, 
                         path = config$monitoring$connection$path[3], #3 -test
                         value = 0, status = 1, comment = "") #No big N
  }
}, error = function(e){
  print(e)
}) #tryCatch

writeLines(format(To, "%Y-%m-%dT%H:%M:%S%z"), last_run_path)

if (file.exists(task_exec_log_path)){
  fwrite(task_res, task_exec_log_path, append = TRUE)} else {
    fwrite(task_res, task_exec_log_path)
  }
######

print(From)
print(To)
return(list(TRUE, From, To))
}
