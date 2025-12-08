
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

execute_task_diff_prices_lpexec <- function(config, last_run_path, symbols_setup_path, types_setup_path, commands_setup_path, task_exec_log_path, task_exec_file){
#config <- yaml.load_file("./conf.yaml")
  if (file.exists(last_run_path)){
    last_run <- as.POSIXct(readLines(last_run_path), format = "%Y-%m-%dT%H:%M:%S%z", tz = "UTC")} else{
      file.create(last_run_path)
      last_run <- NA
    }
symbolsetups <- fread(symbols_setup_path)
typessetups <- fread(types_setup_path)
commandssetups <- fread(commands_setup_path)
statusError <- NULL
To <- now()

#last run time validation: if no data - set current time 
if (!is.na(last_run)&&length(last_run)==1) {
  From <- last_run} else {From <- To}


###lpExec dt from aggrdb
resultsaggr <- data.table()
resaggr <- data.table()
TotalLPexec_res <- data.table()

if (length(config$database_connections[["aggregator"]])>0){
  for(db in names(config$database_connections[["aggregator"]])) {
    tryCatch({
      Credsaggr <- config$database_connections$aggregator[[db]]
      resaggr <- getaggrlpexecPeriod(Credsaggr, From, To)
      print(db)
      print(nrow(resaggr))
      resultsaggr <- rbind(resultsaggr, resaggr)
    }, error = function(e){
      print(e)
      statusError <<- append(statusError, paste(db, substr(e$message, 1, 70), "..."))
    } #e
    ) #tryCatch
  } #for
} #if

if(ncol(resultsaggr) > 0){
#add info  
  resultsaggr[typessetups, c("OrderType") := .(i.typeName), on = .(order_type = typeId)]
  resultsaggr[commandssetups, c("Command") := .(i.commandName), on = .(command_name = commandId)]
  resultsaggr[symbolsetups, c("SlippageThreshold") := .(i.max_slippage), on = .(symbol=SYMBOL)]
  resultsaggr[, perc_diff := round((execution_price/lp_best_price)*100 - 100, 2)]  #ExecPrice/lpBestPrice
  resultsaggr[, slipp_2 := ifelse(grepl("BUY", Command),(execution_price-slippage_requested_price)*(-1), execution_price-slippage_requested_price)]
  
  print("Excluded LP:")
  setdiff(unique(resultsaggr[, bank_name]), config$business_parameters$selected_lp)
# Apply filters from setups
  resultsaggr <- resultsaggr[bank_name %in% config$business_parameters$selected_lp, 
                             .(AGGR, time, "LP"=bank_name, symbol, OrderType, Command, lp_best_price, slippage_requested_price, user_price, execution_price, requested_volume, filled_volume,
                               lp_execution_slippage, perc_diff, slipp_2, SlippageThreshold, request_id, lp_request_id)]  
  
  print("missed Symbols:")
  sort(unique(resultsaggr[is.na(SlippageThreshold), symbol]))
  missed_symb <-paste0(sort(unique(resultsaggr[is.na(SlippageThreshold), symbol])), collapse = "," )
  if(length(missed_symb)>1) {statusError <<- append(statusError, paste0("missed Symbols in config:", missed_symb))}
  
 # Diff_Prices_perc
  res1 <- resultsaggr[abs(perc_diff) >= config$business_parameters$price_threshold_percent][, sensor:="Diff_Prices_perc"]
      res1[, DataText := paste0("aggrName: ", AGGR, " \n",
                                "TimeUTC: ", time," \n",
                                "Symbol: ", symbol, " \n",
                                "FilledVolume: ", filled_volume, " \n",
                                "OrderId: ", request_id, " \n",
                                "OrderType: ", OrderType, ", ", Command," \n",
                                "LP: ", LP, " \n",                              
                                "LpBestPrice: ", lp_best_price, " \n",
                                "ExecutPrice: ", execution_price, " \n",
                                "perc_diff: ", perc_diff, "%", " \n")]
 # LP_slippage_negative
  res2 <- resultsaggr[(abs(lp_execution_slippage)>SlippageThreshold) & (lp_execution_slippage<0)][order(-abs(lp_execution_slippage))][, sensor:="LP_slippage_negative"]
      res2[, DataText := paste0("aggrName: ", AGGR, " \n",
                                "TimeUTC: ", time," \n",
                                "Symbol: ", symbol, " \n",
                                "FilledVolume: ", filled_volume, " \n",
                                "OrderId: ", request_id, " \n",
                                "OrderType: ", OrderType, ", ", Command," \n",
                                "LP: ", LP, " \n",                              
                                "LpBestPrice: ", lp_best_price, " \n",
                                "ExecutPrice: ", execution_price, " \n",
                                "LP-slippage: ", lp_execution_slippage, " \n",
                                "(threshold: ", SlippageThreshold, ") \n")]
 # exec_slippage_negative
  res3 <- resultsaggr[slipp_2<0][, sensor:="exec_slippage_negative"]
      res3[, DataText := paste0("aggrName: ", AGGR, " \n",
                                "TimeUTC: ", time," \n",
                                "Symbol: ", symbol, " \n",
                                "FilledVolume: ", filled_volume, " \n",
                                "OrderId: ", request_id, " \n",
                                "OrderType: ", OrderType, ", ", Command," \n",
                                "LP: ", LP, " \n",                              
                                "SlippageRequestedPrice: ", slippage_requested_price, " \n",
                                "ExecutPrice: ", execution_price, " \n",
                                "Slippage-exec: ", slipp_2, " \n")]
  TotalLPexec_res <- rbind(res1, res2, res3)
  fwrite(TotalLPexec_res, task_exec_file)
}

print(TotalLPexec_res)

 if (nrow(TotalLPexec_res)==0) {
   task_res <- data.table(
     AGGR      = NA_character_,
     time      = as.POSIXct(NA_real_, tz = "UTC"),
     LP        = NA_character_,
     symbol    = NA_character_,
     OrderType = NA_character_,
     Command   = NA_character_,
     lp_best_price   = NA_real_,
     slippage_requested_price   = NA_real_,
     user_price   = NA_real_,
     execution_price   = NA_real_,
     requested_volume   = NA_real_,
     filled_volume   = NA_real_,
     lp_execution_slippage   = NA_real_,
     perc_diff   = NA_real_,
     slipp_2   = NA_real_,
     SlippageThreshold   = NA_real_,
     request_id   = NA_character_,
     lp_request_id      = NA_character_,
     sensor = NA_character_,
     DataText = "no data",
     from = From, to = To, err = paste(statusError, collapse = "; "))
   } else {
   task_res <- TotalLPexec_res[, `:=`(from = From, to = To, err = paste(statusError, collapse = "; "))]
   
   }
if (file.exists(task_exec_log_path)){
  fwrite(task_res, task_exec_log_path, append = TRUE)} else {
    fwrite(task_res, task_exec_log_path)
  }

#### SEND TO HSM 
tryCatch({
  if(length(statusError) > 0){ 
    UpdateBoolSensorValue(productKey = config$monitoring$connection$productKey,  address = config$monitoring$connection$address, port = config$monitoring$connection$port, 
                          path = config$monitoring$connection$path[5],
                          TRUE,
                          status = 1, comment = paste("!!!!NEW__DIFF_PRICES:\n", paste(statusError, collapse = "; "))) 
  } #send hsm notify only if was some errors (OK green)
  
  UpdateFileSensolValue(productKey = config$monitoring$connection$productKey,  address = config$monitoring$connection$address, port = config$monitoring$connection$port, 
                        path = config$monitoring$connection$path[4], 
                        filePath = task_exec_file,
                        status = 1, comment = paste0(nrow(TotalLPexec_res), "rows") )
  
  if(nrow(res1) > 0){
    #addcomment <- paste("Diff Pices for the previous", as.character(seconds_to_period(round(as.numeric(difftime(To, From), units = "secs"), 0))), ":")
    addcomment <- "Diff_Prices_perc:"
    UpdateIntSensorValue(productKey = config$monitoring$connection$productKey,  address = config$monitoring$connection$address, port = config$monitoring$connection$port, 
                         path = config$monitoring$connection$path[1], #3 -test
                         value = nrow(res1),
                         status = 1, comment = paste(addcomment, "\n", paste(res1[, DataText], collapse= ";\n"), sep = "")
    )   
  } else {  #if nrow(resulttable)==0
    UpdateIntSensorValue(productKey = config$monitoring$connection$productKey,  address = config$monitoring$connection$address, port = config$monitoring$connection$port, 
                         path = config$monitoring$connection$path[1], #3 -test
                         value = 0, status = 1, comment = "") #No big lots
  }
  
  if(nrow(res2) > 0){
    #addcomment <- paste("Diff Pices for the previous", as.character(seconds_to_period(round(as.numeric(difftime(To, From), units = "secs"), 0))), ":")
    addcomment <- "LP_slippage_negative:"
    UpdateIntSensorValue(productKey = config$monitoring$connection$productKey,  address = config$monitoring$connection$address, port = config$monitoring$connection$port, 
                         path = config$monitoring$connection$path[2], #3 -test
                         value = nrow(res2),
                         status = 1, comment = paste(addcomment, "\n", paste(res2[, DataText], collapse= ";\n"), sep = "")
    )   
  } else {  #if nrow(resulttable)==0
    UpdateIntSensorValue(productKey = config$monitoring$connection$productKey,  address = config$monitoring$connection$address, port = config$monitoring$connection$port, 
                         path = config$monitoring$connection$path[2], #3 -test
                         value = 0, status = 1, comment = "") #No big lots
  }
  
  if(nrow(res3) > 0){
    #addcomment <- paste("Diff Pices for the previous", as.character(seconds_to_period(round(as.numeric(difftime(To, From), units = "secs"), 0))), ":")
    addcomment <- "exec_slippage_negative:"
    UpdateIntSensorValue(productKey = config$monitoring$connection$productKey,  address = config$monitoring$connection$address, port = config$monitoring$connection$port, 
                         path = config$monitoring$connection$path[3], #3 -test
                         value = nrow(res3),
                         status = 1, comment = paste(addcomment, "\n", paste(res3[, DataText], collapse= ";\n"), sep = "")
    )   
  } else {  #if nrow(resulttable)==0
    UpdateIntSensorValue(productKey = config$monitoring$connection$productKey,  address = config$monitoring$connection$address, port = config$monitoring$connection$port, 
                         path = config$monitoring$connection$path[3], #3 -test
                         value = 0, status = 1, comment = "") #No big lots
  }
  
  
  
}, error = function(e){
  print(e)
}) #tryCatch

writeLines(format(To, "%Y-%m-%dT%H:%M:%S%z"), last_run_path)

# if (file.exists(task_exec_log_path)){
#   fwrite(task_res, task_exec_log_path, append = TRUE)} else {
#     fwrite(task_res, task_exec_log_path)
#   }
######

print(From)
print(To)
return(list(TRUE, From, To))
}