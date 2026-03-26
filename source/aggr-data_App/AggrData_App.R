library(yaml)
library(jsonlite)
library(shiny)
library(shinyjs)
library(plotly)#
library(DT)
library(shinyWidgets)
library(shinyBS)
#library(RGraphics)
#library(htmltools)
library(bsplus)
library(shinycssloaders)
library(lubridate)
#source('AggrPostgresHost.R')
source('../common/PostgresHost.R') #help functions
source('helpFunctions.R')

options(digits.secs = 10)
options(scipen=999)
Sys.setenv("TZ" = "UTC")

cfg <- yaml.load_file('./configDocker/dbCon_config.yaml')
print(names(cfg$ReportingServer[["aggrs"]]))
OrderTypesMap <- fread("./configDocker/orderTypes.csv")
CommandNamesMap <- fread("./configDocker/commands.csv")

as.sunburstDF <- function(DF, value_column = NULL, add_root = FALSE){
  require(data.table)
  
  colNamesDF <- names(DF)
  
  if(is.data.table(DF)){
    DT <- copy(DF)
  } else {
    DT <- data.table(DF, stringsAsFactors = FALSE)
  }
  
  if(add_root){
    DT[, root := "Total"]
  }
  
  colNamesDT <- names(DT)
  hierarchy_columns <- setdiff(colNamesDT, value_column)
  DT[, (hierarchy_columns) := lapply(.SD, as.factor), .SDcols = hierarchy_columns]
  
  if(is.null(value_column) && add_root){
    setcolorder(DT, c("root", colNamesDF))
  } else if(!is.null(value_column) && !add_root) {
    setnames(DT, value_column, "values", skip_absent=TRUE)
    setcolorder(DT, c(setdiff(colNamesDF, value_column), "values"))
  } else if(!is.null(value_column) && add_root) {
    setnames(DT, value_column, "values", skip_absent=TRUE)
    setcolorder(DT, c("root", setdiff(colNamesDF, value_column), "values"))
  }
  
  hierarchyList <- list()
  
  for(i in seq_along(hierarchy_columns)){
    current_columns <- colNamesDT[1:i]
    if(is.null(value_column)){
      currentDT <- unique(DT[, ..current_columns][, values := .N, by = current_columns], by = current_columns)
    } else {
      currentDT <- DT[, lapply(.SD, sum, na.rm = TRUE), by=current_columns, .SDcols = "values"]
    }
    setnames(currentDT, length(current_columns), "labels")
    hierarchyList[[i]] <- currentDT
  }
  
  hierarchyDT <- rbindlist(hierarchyList, use.names = TRUE, fill = TRUE)
  
  parent_columns <- setdiff(names(hierarchyDT), c("labels", "values", value_column))
  hierarchyDT[, parents := apply(.SD, 1, function(x){fifelse(all(is.na(x)), yes = NA_character_, no = paste(x[!is.na(x)], sep = ":", collapse = " - "))}), .SDcols = parent_columns]
  hierarchyDT[, ids := apply(.SD, 1, function(x){paste(x[!is.na(x)], collapse = " - ")}), .SDcols = c("parents", "labels")]
  hierarchyDT[, c(parent_columns) := NULL]
  return(hierarchyDT)
}



ui <- fluidPage(useShinyjs(), use_bs_tooltip(),use_bs_popover(),
                navbarPage(id = "TABS", 
                           strong("AGGREGATOR DB:"),  # page name   
                           tabPanel("LPexecution", #icon = icon("table"),
                                    wellPanel(
                                      
                                      fluidRow(column(2, selectInput("aggr_name_input", "Select source (AggrName):", choices = names(cfg$ReportingServer[["aggrs"]]))),
                                               column(2, dateRangeInput("dateRange_lpexec", "Select Period (Y-m-d 00:00:00 UTC):", weekstart = 1)), #start = Sys.Date() - 2, end = Sys.Date() - 1, max = Sys.Date() - 1, weekstart = 1)
                                               column(2, actionButton("get_LPexec_data", "Get Data", class = "btn btn-success", style = "width: 50%; margin-top: 25px") 
                                                      #,downloadButton("downloadReport", "html", style = "margin-top: 25px")
                                               )
                                      ) #fluidRow
                                    ), #wellPanel
                                    mainPanel(width = 12,
                                              fluidRow(column(1),        
                                                       column(10, uiOutput("LPexec")),#resUIrating
                                                       column(1))
                                    ) #mainPanel
                           )#tabPanel"LPexecution"
                           
                ),#navbarPage
                conditionalPanel(condition="$('html').hasClass('shiny-busy')",
                                 withSpinner(uiOutput("spinnerDummyID1"), type = 6, color = "#0c7dc9", size = 0.6)
                                 #tags$div("Loading...",id="loadmessage")
                )
)#fluidPage ui

server <- function(input, output, session) {
  #update period when click reload page
  updateDateRangeInput(session, "dateRange_lpexec", start = now() - lubridate::days(6), end = now() + lubridate::days(1), max = now() + lubridate::days(1))
  
  getData <- eventReactive(input$get_LPexec_data, {
    
    selectedAggr <- input$aggr_name_input #ttmaster
    From <- input$dateRange_lpexec[1] # To-days(6)
    To <- input$dateRange_lpexec[2] #today() #
    Warning <- NULL
    
    tryCatch({
      tryCatch({ 
        rate2usd <- getAllRates2USDTT(cfg$ReportingServer$servers[["ttlive"]])
        #symbprop <- getAllSymbolsTT(cfg$ReportingServer$servers[["ttlive"]])
      }, error = function(e){
        print(e)
        Warning <<- append(Warning, paste("GetRatesToUSDerror:", substr(e$message, 1, 50), "..."))
      })
      if (length(Warning)>0 ) { stop(paste("!!", Warning, collapse= ", "))}
      
      CredsAggr <- cfg$ReportingServer$aggrs[[selectedAggr]]
      aggrLpExec <- getLPexecdataAggr(CredsAggr, From, To)
    }, error = function(e){
      print(e)
      Warning <<- append(Warning, paste("GetAggrDataError:", substr(e$message, 1, 50), "..."))
    })
    if (length(Warning)>0 ) { stop(paste("!!", Warning, collapse= ", "))}
    
    lpexec <- aggrLpExec[[1]]
    lpexecErr <- aggrLpExec[[2]]
    lpexecDetails <- aggrLpExec[[3]]
    
    lpexecErr_temp <- data.table()
    
    setnames(lpexec, c("bank_name"), c("LP"))
    setnames(lpexecErr, c("lp", "config_symbol"), c("LP", "symbol"))
    
    #data validation 
    if (nrow(lpexec[filled_volume==0]) > 0) {
      lpexecErr_temp <- lpexec[filled_volume==0]
      lpexec <- lpexec[filled_volume>0]
      lpexecErr <- rbind(lpexecErr[, .(log_date, time, LP, symbol, command_name, lp_best_price, slippage_requested_price, user_price, requested_volume, request_id, aggrName)],
                         lpexecErr_temp[, .(log_date, time, LP, symbol, command_name, lp_best_price, slippage_requested_price, user_price, requested_volume, request_id, aggrName)])
    }
    
    lpexec[, MarginCurr := sub("/.*","", symbol)]
    lpexec[, ProfitCurr := sub(".*/","", symbol)]
    lpexec[rate2usd, c("MCtoUSD") := .(i.Value), on = .(MarginCurr = FromCurrencyName)][MarginCurr=="USD", MCtoUSD := 1]
    lpexec[rate2usd, c("PCtoUSD") := .(i.Value), on = .(ProfitCurr = FromCurrencyName)][ProfitCurr=="USD", PCtoUSD := 1]
    lpexec[, TradedVolumeUSD := filled_volume * MCtoUSD]
    lpexec[, LpSlippageUSD := lp_execution_slippage * filled_volume * PCtoUSD]
    #lpexec[, `lp_exec_slipp_%` := round((execution_price/lp_best_price)*100 - 100, 2)]  #ExecPrice/lpBestPrice
    lpexec[, `lp_exec_slipp_%` := round((lp_execution_slippage/lp_best_price)*100, 2)]  #same value,  lp_execution_slippagee=execution_price-lp_best_price
    lpexec[, SlippageType := c("negative", "exact", "positive")[sign(lp_execution_slippage)+2 ]]  #sing() return -1 0 +1, when +2 we get 1,2,3 (indices for vector)
    lpexec[OrderTypesMap, c("OrderType") := .(i.OrderType), on = .(order_type = order_type)]
    lpexec[CommandNamesMap, c("Command") := .(i.commandName), on = .(command_name = commandId)]
    lpexec[, slipp_2 := ifelse(grepl("BUY", Command),(execution_price-slippage_requested_price)*(-1), execution_price-slippage_requested_price)]
    lpexec[, timestep := floor_date(time, "minute")]
    
    lpexecErr[, timestep := floor_date(time, "minute")]
    
    #SUMMARY by LP
    summaryLP <- lpexec[, .("VolumeUSD" = round(sum(TradedVolumeUSD),2),
                            "TotalSlippageUSD" = round(sum(LpSlippageUSD),2), 
                            "SlippageUSD(+/-)" = paste(round(sum(pmax(LpSlippageUSD, 0), na.rm = TRUE),2), "/", round(sum(pmin(LpSlippageUSD, 0), na.rm = TRUE),2) ),
                            "ApprovedCount" = .N), by = .(LP)]
    failedLP <- lpexecErr[, .("FailedCount" = .N), by = .(LP)]
    summaryLP <- merge(summaryLP, failedLP, by = "LP", all = TRUE)[order(-VolumeUSD)]
    summaryLP[is.na(FailedCount), FailedCount := 0]
    summaryLP[, `Approved%` := round(ApprovedCount/(ApprovedCount+FailedCount)*100,2)]
    summaryLP[, `Volume%` := round(VolumeUSD/sum(VolumeUSD, na.rm = T)*100,2)]
    
    # by LP & SYMBOL
    LPsymbol <- lpexec[, .("VolumeUSD" = round(sum(TradedVolumeUSD),2), 
                           "VolumeMargin" = round(sum(filled_volume),2), 
                           "ApprovedCount" = .N), by = .(LP, symbol)]
    LPsymbolsunb <- as.sunburstDF(LPsymbol[, .(LP, symbol, VolumeUSD)], value_column = "VolumeUSD", add_root = TRUE)
    failedLPsymbol <- lpexecErr[, .("FailedCount" = .N), by = .(LP, symbol)]
    LPsymbol <- merge(LPsymbol, failedLPsymbol, by = c("LP", "symbol"), all = TRUE)[order(-VolumeUSD)]
    
    # Approved/failed timeline
    lpExecQuality <- rbind(lpexec[, .("Status" = "Approved", "Count" = .N), by= .(timestep)], 
                           lpexecErr[, .("Status" = "Failed", "Count" = .N), by= .(timestep)])[order(timestep)]
    lpExecQuality1 <- lpExecQuality[CJ(timestep = unique(lpExecQuality$timestep), Status = unique(lpExecQuality$Status)), on = .(timestep, Status)][is.na(Count), Count := 0]
    
    # SLIPPAGE by LP (by Type) for chart
    slippageLP <- lpexec[, .("SlippageUSD" = round(sum(LpSlippageUSD, na.rm = T),2), 
                             "Count" = .N), by = .(LP, SlippageType)][order(LP, SlippageType)]
    slippageLP[, `Count%` := round(Count/sum(Count, na.rm = T)*100,2), by = LP]
    
    # SLIPPAGE LP & Symbol (by Type)
    slippageLPsymbol <- lpexec[, .("SlippageUSD" = round(sum(LpSlippageUSD, na.rm = T),2),
                                   "Slippage%(mean)" = round(mean(`lp_exec_slipp_%`),2),
                                   "Count" = .N), by = .(LP, symbol, SlippageType)][order(-abs(SlippageUSD))]
    # SLIPPAGE by LP (by OrderType) for chart
    slippageLPb_orderType <- lpexec[, .("SlippageUSD" = round(sum(LpSlippageUSD, na.rm = T),2),
                                        "Count" = .N), by = .(LP, OrderType)][order(LP, OrderType)]
    slippageLPb_orderType[, `Count%` := round(Count/sum(Count, na.rm = T)*100,2), by = LP]  
    
    # SLIPPAGE by LP (by OrderType+SlippType)
    slippageLPb_orderAndSlipType <- lpexec[, .("SlippageUSD" = round(sum(LpSlippageUSD, na.rm = T),2), 
                                               "Slippage%(mean)" = round(mean(`lp_exec_slipp_%`),2),
                                               "Count" = .N), by = .(LP, OrderType, SlippageType)][order(-abs(SlippageUSD))]
    #top 
    topSlippageUSDTransactions <- lpexec[, .(time , LP, symbol, OrderType, Command, lp_best_price, slippage_requested_price, user_price, execution_price, lp_execution_slippage, `lp_exec_slipp_%`,
                                             "LpSlippageUSD"=round(LpSlippageUSD,2), SlippageType, requested_volume, filled_volume,  "TradedVolumeUSD"=round(TradedVolumeUSD,2), 
                                             request_id, lp_request_id, order_id, aggrName)][order(-(abs(LpSlippageUSD)))][1:20][!is.na(time)]
    topSlippagePercTransactions <- lpexec[, .(time , LP, symbol, OrderType, Command, lp_best_price, slippage_requested_price, user_price, execution_price, lp_execution_slippage, `lp_exec_slipp_%`,
                                              "LpSlippageUSD"=round(LpSlippageUSD,2), SlippageType, requested_volume, filled_volume,  "TradedVolumeUSD"=round(TradedVolumeUSD,2), 
                                              request_id, lp_request_id, order_id, aggrName)][order(-(abs(`lp_exec_slipp_%`)))][1:20][!is.na(time)]
    slippage2_dt <- lpexec[slipp_2<0, .(time , LP, symbol, OrderType, Command, lp_best_price, slippage_requested_price, user_price, execution_price, lp_execution_slippage, `lp_exec_slipp_%`,
                                        "negative_execution"=signif(slipp_2,5),
                                        "LpSlippageUSD"=round(LpSlippageUSD,2), SlippageType, requested_volume, filled_volume,  "TradedVolumeUSD"=round(TradedVolumeUSD,2), 
                                        request_id, lp_request_id, order_id, aggrName)][order(time)][!is.na(time)]
    
    INFO <- paste("Aggr: ", selectedAggr, 'from', From, 'to',  To, "(loaded", nrow(lpexec)+nrow(lpexecErr), "rows from db)")
    
    
    return(list(INFO, Warning, summaryLP, LPsymbol, LPsymbolsunb, 
                lpExecQuality1, slippageLP, slippageLPsymbol, slippageLPb_orderType, slippageLPb_orderAndSlipType,
                topSlippageUSDTransactions, topSlippagePercTransactions, slippage2_dt))
    
  }) #getdata()
  
  
  output$LPexec <-  renderUI({
    result <- getData()
    #step <- median(diff(result[[6]]$timestep))
    
    if (nrow(result[[3]]) > 0) {
      tagList(
        h4(strong(result[[1]])),
        renderText(result[[2]]),
        h3(strong('Common statistics by LP')),
        #summary dt by LP,
        DT::renderDT({
          DT::datatable(result[[3]], escape = FALSE, rownames = FALSE, width = '100%',
                        extensions = c('Scroller',  'Buttons'),
                        options = list(
                          buttons =list(list(extend = 'collection', buttons = c('excel','csv'), text = as.character(icon("download-alt", lib = "glyphicon")), titleAttr = 'Save as...')),
                          dom = 'Brfti',
                          deferRender = TRUE,
                          initComplete = JS(
                            "function(settings, json) { $(this.api().table().header()).css({'background-color': '#e5e5e5', 'color': '#000'}); }")
                        )
          ) %>% formatRound(columns = c("VolumeUSD", "TotalSlippageUSD"),  digits = 2, mark = " ") %>% formatRound(columns = c("ApprovedCount", "FailedCount"),  digits = 0, mark = " ")
        }, server = FALSE),
        h3(strong('Traded Volume in USD by LP&Symbol')),
        #sunburst and dt by LP+symb
        fluidRow(
          column(4, 
                 renderPlotly(plot_ly() %>% add_trace(
                   ids = result[[5]]$ids,
                   labels = result[[5]]$labels,
                   parents = result[[5]]$parents,
                   values=result[[5]]$values,
                   type = 'sunburst',
                   branchvalues = 'total',
                   domain = list(row = 0, column = 1),
                   hovertemplate = "%{label}<br>%{value:,.0f}$<extra></extra>")
                 )
          ),
          column(8,
                 DT::renderDT({
                   DT::datatable(result[[4]], escape = FALSE, rownames = FALSE, width = '100%',
                                 extensions = c('Scroller',  'Buttons'),
                                 options = list(
                                   buttons =list(list(extend = 'collection', buttons = c('excel','csv'), text = as.character(icon("download-alt", lib = "glyphicon")), titleAttr = 'Save as...')),
                                   dom = 'Brfti',
                                   deferRender = TRUE,
                                   scroller = TRUE,
                                   scrollY = ifelse(nrow(result[[4]])<4,120,300),
                                   initComplete = JS("function(settings, json) {
                          var table = this.api();
                          $(this.api().table().header()).css({'background-color': '#e5e5e5', 'color': '#000'});
                          $(this.api().table().container()).css({'font-size': '80%'}); 
                          table.columns.adjust(); }")
                                 )
                   ) %>% formatRound(columns = c("VolumeUSD", "VolumeMargin"),  digits = 2, mark = " ") %>% formatRound(columns = c("ApprovedCount", "FailedCount"),  digits = 0, mark = " ")
                 }, server = FALSE),
          )
        ), #fluidrow
        #density
        h3(strong('Transactions density')),
        ggplotly(height = 350, tooltip = c("text"), 
                 ggplot(result[[6]], aes(x = timestep, y = Count, colour =  Status, fill =  Status,
                                         text= paste0("datetime: ", timestep,"<br>",
                                                      "Count/min: ", Count,"<br>",
                                                      "Status: ", Status)                 
                 ))
                 #+ geom_col(width = step * 0.3)
                 + geom_col()
                 + scale_y_continuous(labels=function(x) format(x, big.mark = " ", scientific = FALSE))
                 + scale_colour_manual('Order status', values=c('Failed' = "red", 'Approved' = "darkgreen"))
                 + scale_fill_manual('Order status', values=c('Failed' = "red", 'Approved' = "darkgreen"))
                 + labs(title = "", x = "", y = "transactions per 1min", fill = "Status"), dynamicTicks = TRUE
        ), #density
        h3(strong('LP Slippage')),
        fluidRow(
          column(5, 
                 ggplotly(height = 350, tooltip = c("text"), 
                          ggplot(result[[7]], aes(y = reorder(LP, Count, sum, na.rm = T), x = `Count%`, fill =  reorder(SlippageType, -Count, sum, na.rm = T),
                                                  text= paste0("LP: ", LP,"<br>",
                                                               "SlippageType: ", SlippageType,"<br>",
                                                               "Slippage_USD: ", SlippageUSD,"<br>",
                                                               "TransactionCount: ", Count, " (", `Count%`, "%)")                 
                          ))
                          + geom_col() + geom_text(aes(label = `Count%`), position = position_stack(vjust=0.5), size = 3)
                          + scale_fill_manual('SlippageType:', values=c('exact' = "#4DA3FF", 'positive' = "#2ECC71", 'negative' = "#FA8072"))
                          + scale_x_continuous(labels = NULL)
                          + labs(title = "Transaction percentage by SlippageType", x = "%", y = "", fill = "Type")
                 )
          ),
          column(7,
                 DT::renderDT({
                   DT::datatable(result[[8]], escape = FALSE, rownames = FALSE, width = '100%',
                                 caption = "Slippage by LP & Symbol & Type:",
                                 extensions = c('Scroller',  'Buttons'),
                                 options = list(
                                   buttons =list(list(extend = 'collection', buttons = c('excel','csv'), text = as.character(icon("download-alt", lib = "glyphicon")), titleAttr = 'Save as...')),
                                   dom = 'Brfti',
                                   deferRender = TRUE,
                                   scroller = TRUE,
                                   scrollY = ifelse(nrow(result[[8]])<4,120,300),
                                   initComplete = JS("function(settings, json) {
                          $(this.api().table().header()).css({'background-color': '#e5e5e5', 'color': '#000'});
                          $(this.api().table().container()).css({'font-size': '80%'}); }")
                                 )
                   ) %>% formatStyle("SlippageUSD", color = styleInterval(0, c('red', 'green')))
                 }, server = FALSE),
          )
        ), #fluidrow
        
        fluidRow(
          column(5, 
                 ggplotly(height = 350, tooltip = c("text"), 
                          ggplot(result[[9]], aes(y = reorder(LP, Count, sum, na.rm = T), x = `Count%`, fill =  reorder(OrderType, -Count, sum, na.rm = T),
                                                  text= paste0("LP: ", LP,"<br>",
                                                               "OrderType: ", OrderType,"<br>",
                                                               "Slippage_USD: ", SlippageUSD,"<br>",
                                                               "TransactionCount: ", Count, " (", `Count%`, "%)")                 
                          ))
                          + geom_col() + geom_text(aes(label = `Count%`), position = position_stack(vjust=0.5), size = 3) 
                          + scale_fill_brewer(palette = "Set3")
                          + scale_x_continuous(labels = NULL)
                          + labs(title = "Transaction percentage by OrderType", x = "%", y = "", fill = "OrderType")
                 )
          ),
          column(7,
                 DT::renderDT({
                   DT::datatable(result[[10]], escape = FALSE, rownames = FALSE, width = '100%',
                                 caption = "Slippage by LP & OrderType:",
                                 extensions = c('Scroller',  'Buttons'),
                                 options = list(
                                   buttons =list(list(extend = 'collection', buttons = c('excel','csv'), text = as.character(icon("download-alt", lib = "glyphicon")), titleAttr = 'Save as...')),
                                   dom = 'Brfti',
                                   deferRender = TRUE,
                                   scroller = TRUE,
                                   scrollY = ifelse(nrow(result[[10]])<4,120,300),
                                   initComplete = JS("function(settings, json) {
                          $(this.api().table().header()).css({'background-color': '#e5e5e5', 'color': '#000'});
                          $(this.api().table().container()).css({'font-size': '80%'}); }")
                                 )
                   ) %>% formatStyle("SlippageUSD", color = styleInterval(0, c('red', 'green')))
                 }, server = FALSE),
          )
        ), #fluidrow      
        h3(strong('Details')),
        
        #topSlippageTransactions USD
        DT::renderDT({
          DT::datatable(result[[11]], escape = FALSE, rownames = F, width = '100%',#server=FALSE, 
                        caption = "Top20 transactions with the largest (+/-) slippagesUSD ( = lp_execution_slippage * filled_volume * ProfitCurrToUSDrate)",
                        extensions = c('Scroller',  'Buttons'),
                        options = list(
                          dom = 'Blfrtip',
                          deferRender = TRUE,
                          scrollX = TRUE,
                          scrollY = ifelse(nrow(result[[11]])<7,200,400), #600px high
                          scroller = TRUE,
                          autoWidth = FALSE,
                          buttons =list(list(extend = 'collection', buttons = c('excel','csv'), text = as.character(icon("download-alt", lib = "glyphicon")), titleAttr = 'Save as...')),
                          initComplete = JS("function(settings, json) {
                          var table = this.api();
                          $(this.api().table().header()).css({'background-color': '#e5e5e5', 'color': '#000'});
                          $(this.api().table().container()).css({'font-size': '80%'}); 
                          table.columns.adjust(); }")
                        )#options
          ) %>% formatStyle("LpSlippageUSD", color = styleInterval(0, c('red', 'green')))#dt
        }, server = FALSE),
        
        #topSlippageTransactions %
        DT::renderDT({
          DT::datatable(result[[12]], escape = FALSE, rownames = F, width = '100%',#server=FALSE, 
                        caption = "Top20 transactions with the largest slippage % ( = lp_execution_slippage / lp_best_price *100)",
                        extensions = c('Scroller',  'Buttons'),
                        options = list(
                          dom = 'Blfrtip',
                          deferRender = TRUE,
                          scrollX = TRUE,
                          scrollY = ifelse(nrow(result[[12]])<7,200,400), #600px high
                          scroller = TRUE,
                          autoWidth = FALSE,
                          buttons =list(list(extend = 'collection', buttons = c('excel','csv'), text = as.character(icon("download-alt", lib = "glyphicon")), titleAttr = 'Save as...')),
                          initComplete = JS("function(settings, json) {
                          var table = this.api();
                          $(this.api().table().header()).css({'background-color': '#e5e5e5', 'color': '#000'});
                          $(this.api().table().container()).css({'font-size': '80%'}); 
                          table.columns.adjust(); }")
                        )#options
          ) %>% formatStyle("lp_exec_slipp_%", color = styleInterval(0, c('red', 'green')))#dt
        }, server = FALSE),
        
        #Slippage_2
        if (nrow(result[[13]])>0){
          DT::renderDT({
            DT::datatable(result[[13]], escape = FALSE, rownames = F, width = '100%',#server=FALSE, 
                          caption = "Negative exec slippage ( = execution_price - slippage_requested_price)",
                          extensions = c('Scroller',  'Buttons'),
                          options = list(
                            dom = 'Blfrtip',
                            deferRender = TRUE,
                            scrollX = TRUE,
                            scrollY = ifelse(nrow(result[[13]])<7,200,400), #600px high
                            scroller = TRUE,
                            autoWidth = FALSE,
                            buttons =list(list(extend = 'collection', buttons = c('excel','csv'), text = as.character(icon("download-alt", lib = "glyphicon")), titleAttr = 'Save as...')),
                            initComplete = JS("function(settings, json) {
                          var table = this.api();
                          $(this.api().table().header()).css({'background-color': '#e5e5e5', 'color': '#000'});
                          $(this.api().table().container()).css({'font-size': '80%'}); 
                          table.columns.adjust(); }")
                          )#options
            ) %>% formatStyle("negative_execution", color = styleInterval(0, c('red', 'green')))#dt
          }, server = FALSE)
        },
        
      )#taglist
    } #if  data is
    else{tagList(
      h4(strong(result[[1]])),
      h4(span("No data", style = "color: #0a6ed1")),
      renderText(result[[2]]))}
  })
} #shinyServer

shinyApp(ui, server)

