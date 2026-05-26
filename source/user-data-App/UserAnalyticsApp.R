library(yaml)
library(shiny)
library(shinyjs)
library(plotly)
library(DT)
library(shinyWidgets)
library(shinyBS)
library(htmltools)
library(bsplus)
library(shinycssloaders)
library(lubridate)
source('helpFunc.R')

#setwd("C:/Users/oksana.hilevich/Desktop/RTools/RShiny/UserAnalytics")
options(digits.secs = 10)
options(scipen=999)
Sys.setenv("TZ" = "UTC")

round_up <- function(x, digits) {
  f <- 10^digits
  ceiling(x * f) / f
}

fmt_m   <- function(x, d = 2) if (is.na(x)) "—" else formatC(x, format = "f", digits = d, big.mark = ",")
sgn_col <- function(x) if (is.na(x) || x == 0) "#555" else if (x > 0) "#2a7a2a" else "#cc3333"
mrow <- function(label, value, accent = FALSE, total = FALSE, d = 2) {
  col <- sgn_col(value)
  bg  <- if (total) "background:#f2f2f2;" else ""
  brd <- if (total) "border-top:2px solid #aaa;" else "border-bottom:1px solid #eee;"
  wt  <- if (total || accent) "font-weight:600;" else ""
  sz  <- if (total) "font-size:1.04em;" else ""
  tags$tr(style = paste0(bg, brd),
    tags$td(label,           style = paste0("padding:3px 8px 3px 2px;color:#333;", wt, sz)),
    tags$td(fmt_m(value, d), style = paste0("padding:3px 2px 3px 8px;text-align:right;color:", col, ";", wt, sz))
  )
}
mtable <- function(title, ...) tagList(
  tags$p(strong(title), style = "color:#555;font-size:0.78em;text-transform:uppercase;letter-spacing:0.05em;margin:0 0 4px 0;"),
  tags$table(style = "width:100%;border-collapse:collapse;font-size:0.87em;", ...)
)
prow <- function(label, cval = NA, bval = NA, accent = FALSE, total = FALSE, d = 2) {
  bg  <- if (total) "background:#f2f2f2;" else ""
  brd <- if (total) "border-top:2px solid #aaa;" else "border-bottom:1px solid #eee;"
  wt  <- if (total || accent) "font-weight:600;" else ""
  sz  <- if (total) "font-size:1.04em;" else ""
  cell <- function(v, extra = "") {
    has_val <- length(v) > 0 && !is.na(v)
    tags$td(if (has_val) fmt_m(v, d) else "—",
            style = paste0("padding:3px 4px;text-align:right;", wt, sz, extra,
                           "color:", if (has_val) sgn_col(v) else "#bbb", ";"))
  }
  tags$tr(style = paste0(bg, brd),
    tags$td(label, style = paste0("padding:3px 8px 3px 2px;color:#333;", wt, sz)),
    cell(cval),
    cell(bval)
  )
}
ptable <- function(...) tagList(
  tags$table(style = "width:100%;border-collapse:collapse;font-size:0.87em;",
    tags$thead(tags$tr(
      tags$th("", style = "padding:2px 8px 5px 2px;"),
      tags$th("Broker profit (USD)", style = "padding:2px 4px 5px;text-align:right;color:#666;font-size:0.9em;font-weight:normal;border-bottom:1px solid #ccc;"),
      tags$th("Client (USD)",  style = "padding:2px 2px 5px 4px;text-align:right;color:#666;font-size:0.9em;font-weight:normal;border-bottom:1px solid #ccc;")
    )),
    ...
  )
)

cfg <- yaml.load_file('configDocker/dbCon_config.yaml')

ui <- fluidPage(useShinyjs(), use_bs_tooltip(),use_bs_popover(),
            navbarPage(id = "TABS", 
                       strong("USER ANALYTICS:"),  # page name   
                       tabPanel("User summary", icon = icon("table"),
                                wellPanel(
                                  fluidRow(
                                    column(5,
                                      div(style = "border: 1px solid #ccc; border-radius: 4px; padding: 8px 12px;",
                                        fluidRow(
                                          column(5, dateRangeInput("dateRange_trading", "Period:", weekstart = 1)),
                                          column(4, textInput("login_input", "Login:")),
                                          column(3, selectInput("platform_input", "Platform:", choices = names(cfg)))
                                        )
                                      )
                                    ),
                                    column(4,
                                      div(style = "border: 1px solid #ccc; border-radius: 4px; padding: 8px 12px;",
                                        fluidRow(
                                          column(4, numericInput("price_markup", "Price mkp %:", value = 0.0009, step = 0.0001, min = 0, max = 100)),
                                          column(4, numericInput("swap_markup", "Swap mkp %:", value = 20, step = 1, min = 0, max = 99)),
                                          #column(4, numericInput("index_coeff", "Index coeff %:", value = 10, step = 1, min = 0, max = 99)),
                                          column(4, numericInput("lp_commis", "LP commis $/M:", value = 8, step = 1, min = 0))
                                        )
                                      )
                                    ),
                                    column(3,
                                      actionButton("getData", "Get Data", style = "color: black; background-color: #3d7194; width: 100%; margin-top: 25px")
                                    )
                                  )
                                ),
                                mainPanel(width = 12,
                                          fluidRow(column(1),        
                                                   column(10, uiOutput("resUI_user_summary")),#resUIrating
                                                   column(1))
                                )
                       )#tabPanel"USER ANALYTICS"
         
            ),#navbarPage
            conditionalPanel(condition="$('html').hasClass('shiny-busy')",
                             withSpinner(uiOutput("spinnerDummyID1"), type = 6, color = "#0c7dc9", size = 0.6)
                             #tags$div("Loading...",id="loadmessage")
            )
  )#fluidPage
  




server <- function(input, output, session) {


  updateDateRangeInput(session, "dateRange_trading", start = now() - days(6), end = now() + days(1), max = Sys.Date() + days(1))

  #### Get data
  getData <- eventReactive(input$getData, {
    
    Platform <- input$platform_input #"TT"
    UserLogin <- as.numeric(input$login_input) #24003459 uk
    From <- input$dateRange_trading[1] # To-days(6)
    To <- input$dateRange_trading[2] #today() #
 
   
    refData <- getRefDataTT(cfg[["TT"]]$servers[["ttlive"]])
    rate2usd <- refData$rate2usd
    symbolsPrices <- refData$symbolsPrices
    tt_symbols <- getAllSymbolsTT(cfg[["TT"]]$servers[["ttlive"]])

    all_results <- list()
    
    # --- Fetch & normalize per platform ---
    if (Platform == "TT") {
      for (db in names(cfg[[Platform]]$servers)) {
        tryCatch({
          res <- getTradeReportPeriod(cfg[[Platform]]$servers[[db]], UserLogin, From, To)
          print(paste(db, nrow(res)))
          if (nrow(res) > 0) {
            setnames(res, c("Ntrades", "TVinLot"), c("DEAL_COUNT", "VOLUMElot"))
            res[, Date := as.Date(TrTime)]
            all_results[[db]] <- res
          }
        }, error = function(e) print(e))
      }
    } else if (Platform == "MT4") {
      TimeOffset <- META5Timeoffset(cfg[["MT5"]]$servers[["mt5_live"]])
      for (db in names(cfg[[Platform]]$servers)) {
        tryCatch({
          res <- getMT4userTradesPeriod(cfg[[Platform]]$servers[[db]], UserLogin, From, To, TimeOffset)
          print(paste(db, nrow(res)))
          if (nrow(res) > 0) {
            setnames(res, c("LOGIN","NAME","GROUP","SYMBOL","CURRENCY","PROFIT","COMMISSION","SWAPS"),
                          c("Login","Name","Group","Symbol","BalanceCurrency","Profit","Commission","Swap"))
            res[grepl("Dividend", COMMENT), Dividend := round(Profit, 2)]
            res[CMD %in% c(6,7) & !grepl("Dividend", COMMENT), Deposit := round(Profit, 2)]
            res[CMD %in% c(6,7), Profit := 0]
            res[grepl("cancelled", COMMENT), Commission := 0]
            res[, Date := as.Date(ifelse(CLOSE_TIME > "1970-01-01 00:00:00", as.Date(CLOSE_TIME), as.Date(OPEN_TIME)), origin = "1970-01-01")]
            all_results[[db]] <- res
          }
        }, error = function(e) print(e))
      }
    } else if (Platform == "MT5") {
      TimeOffset <- META5Timeoffset(cfg[["MT5"]]$servers[["mt5_live"]])
      for (db in names(cfg[[Platform]]$servers)) {
        tryCatch({
          res <- getMT5userdealsPeriod(cfg[[Platform]]$servers[[db]], UserLogin, From, To, TimeOffset)
          print(paste(db, nrow(res)))
          if (nrow(res) > 0) {
            setnames(res, "Currency", "BalanceCurrency")
            res[grepl("Dividend", Comment), Dividend := round(Profit, 2)]
            res[Action == 2 & !grepl("Dividend", Comment), Deposit := round(Profit, 2)]
            res[Action == 2, Profit := 0]
            res[, Date := as.Date(Time)]
            all_results[[db]] <- res
          }
        }, error = function(e) print(e))
      }
    }

    results <- rbindlist(all_results, fill = TRUE)

    if (nrow(results) == 0) {
      return(list(data.table(), data.table(), data.table(), From, To, data.table()))
    }

    # --- Mark closed/open ---
    results[, is_closed := TRUE]
    if (Platform == "MT4") {
      results[CLOSE_TIME == "1970-01-01 00:00:00" & !CMD %in% c(6,7), is_closed := FALSE]
    }
    # Zero out swap for open positions (swap open fetched separately)
    results[is_closed == FALSE, Swap := 0]

    # --- Common: join tt_symbols ---
    results[tt_symbols, c("MarginCurrency","ProfitCurrency","ContractSize","CalcMode","Precision","security") :=
      .(i.MarginCurrency, i.ProfitCurrency, i.ContractSize, i.MarginMode, i.Precision, i.security),
      on = .(Symbol = symbol_name)]

    # --- Platform-specific TV calculations (after tt_symbols join) ---
    if (Platform == "MT4") {
      results[!CMD %in% c(6,7), TVbalcur := round(ifelse(CalcMode==0,
        VOLUMElot*ContractSize*(CONV_RATE1+CONV_RATE2),
        VOLUMElot*ContractSize*(OPEN_PRICE*CONV_RATE1+CLOSE_PRICE*CONV_RATE2)), 2)]
      results[, TVmargincurr := round(DEAL_COUNT * VOLUMElot * ContractSize, 2)]
      results[, TVlot_ := VOLUMElot * DEAL_COUNT]
    } else if (Platform == "MT5") {
      results[Action <= 1, TVbalcur := ifelse(CalcMode==0,
        VOLUMElot*ContractSize*RateMargin,
        VOLUMElot*ContractSize*RateMargin*Price)]
      results[, TVmargincurr := round(VOLUMElot * ContractSize, 2)]
      results[, TVlot_ := VOLUMElot]
    } else {
      # TT: TVbalcur and TVmargincurr already from SQL
      results[, TVlot_ := VOLUMElot]
    }

    # --- Common: join prices & rates ---
    results[symbolsPrices, SymbolPrice := i.BidPrice, on = .(Symbol = SymbolName)]
    results[rate2usd, BCtoUSD := i.Value, on = .(BalanceCurrency = FromCurrencyName)]
    results[BalanceCurrency == "USD", BCtoUSD := 1]
    results[rate2usd, PCtoUSD := i.Value, on = .(ProfitCurrency = FromCurrencyName)]
    results[ProfitCurrency == "USD", PCtoUSD := 1]

    # --- Common: calculated columns ---
    results[, TVusd := round(TVbalcur * BCtoUSD, 0)]
    price_mkp <- input$price_markup / 100
    results[, calcPMusd := round(TVmargincurr * round_up(SymbolPrice * price_mkp, Precision) * PCtoUSD, 2)]
    results[, pm := round_up(SymbolPrice * price_mkp, Precision)]

    # --- Swap markup ---
    swap_mkp <- input$swap_markup / 100
    # idx_coeff <- input$index_coeff / 100
    # results[, effective_mkp := fcase(
    #   !is.na(security) & grepl("Index", security), idx_coeff,
    #   default = swap_mkp
    # )]
    results[, swap_markup_profit := fcase(
      is_closed == TRUE & Swap < 0, round((Swap / (1 + swap_mkp)) - Swap, 2),
      is_closed == TRUE & Swap > 0, round((Swap / (1 - swap_mkp)) - Swap, 2),
      default = 0
    )]
    results[, swap_markup_usd := round(swap_markup_profit * BCtoUSD, 2)]

    # --- Common: summaries ---
    summary <- results[, .(
      TVbalcur = round(sum(TVbalcur, na.rm = T), 0),
      TVusd = round(sum(TVusd, na.rm = T), 0),
      calcLPcommissusd = round(-(sum(TVusd, na.rm = T) / 1000000 * input$lp_commis), 2),
      calcPMusd = round(sum(calcPMusd, na.rm = T), 0),
      calcSwapMusd = round(sum(swap_markup_usd, na.rm = T), 2),
      BCtoUSD = head(BCtoUSD[!is.na(BCtoUSD)], 1),
      Commiss = round(sum(Commission, na.rm = T), 2),
      SwapClosed = round(sum(Swap, na.rm = T), 2),
      Dividend = round(sum(Dividend, na.rm = T), 2),
      Profit = round(sum(Profit, na.rm = T), 2),
      NetDeposit = round(sum(Deposit, na.rm = T), 2),
      TradesCount = round(sum(DEAL_COUNT, na.rm = T), 2)
    ), by = .(Group, Login, Name)]

    by_symb <- results[Symbol != "", .(
      TVbalcur = round(sum(TVbalcur, na.rm = T), 0),
      TVusd = round(sum(TVusd, na.rm = T), 0),
      calcPMusd = round(sum(calcPMusd, na.rm = T), 0),
      calcSwapMusd = round(sum(swap_markup_usd, na.rm = T), 2),
      Markup = input$price_markup,
      TVmarcur = round(sum(TVmargincurr, na.rm = T), 0),
      TVlot = round(sum(TVlot_, na.rm = T), 2),
      Commiss = round(sum(Commission, na.rm = T), 2),
      SwapClosed = round(sum(Swap, na.rm = T), 2),
      Dividend = round(sum(Dividend, na.rm = T), 2),
      Profit = round(sum(Profit, na.rm = T), 2),
      TradesCount = round(sum(DEAL_COUNT, na.rm = T), 2)
    ), by = .(Group, Login, Symbol)][order(-TVbalcur)]

    by_date <- results[Symbol != "", .(
      TVbalcur = round(sum(TVbalcur, na.rm = T), 0),
      Profit = round(sum(Profit, na.rm = T), 2)
    ), by = .(Date, Symbol)][order(Date, Symbol)]

    print(by_symb)
    print(by_date)

    # --- Open positions ---
    open_results <- list()
    if (Platform == "TT") {
      for (db in names(cfg[[Platform]]$servers)) {
        tryCatch({
          res <- getTTuserOpenPositions(cfg[[Platform]]$servers[[db]], UserLogin)
          open_results[[db]] <- res
        }, error = function(e) print(e))
      }
    } else if (Platform == "MT4") {
      for (db in names(cfg[[Platform]]$servers)) {
        tryCatch({
          res <- getMT4userOpenPositions(cfg[[Platform]]$servers[[db]], UserLogin)
          open_results[[db]] <- res
        }, error = function(e) print(e))
      }
    } else if (Platform == "MT5") {
      for (db in names(cfg[[Platform]]$servers)) {
        tryCatch({
          res <- getMT5userOpenPositions(cfg[[Platform]]$servers[[db]], UserLogin)
          open_results[[db]] <- res
        }, error = function(e) print(e))
      }
    }

    open_dt <- rbindlist(open_results, fill = TRUE)
    if (nrow(open_dt) > 0) {
      open_dt[rate2usd, BCtoUSD := i.Value, on = .(BalanceCurrency = FromCurrencyName)]
      open_dt[BalanceCurrency == "USD", BCtoUSD := 1]

      open_dt[, calcSwapMusdOpen := fcase(
        SwapOpen < 0, round(((SwapOpen / (1 + swap_mkp)) - SwapOpen) * BCtoUSD, 2),
        SwapOpen > 0, round(((SwapOpen / (1 - swap_mkp)) - SwapOpen) * BCtoUSD, 2),
        default = 0
      )]

      open_summary <- open_dt[, .(
        SwapOpen = round(sum(SwapOpen, na.rm = T), 2),
        calcSwapMusdOpen = round(sum(calcSwapMusdOpen, na.rm = T), 2)
      ), by = .(Group, Login, Name)]
      summary <- merge(summary, open_summary, by = c("Group", "Login", "Name"), all = TRUE)

      open_by_symb <- open_dt[, .(
        SwapOpen = round(sum(SwapOpen, na.rm = T), 2),
        calcSwapMusdOpen = round(sum(calcSwapMusdOpen, na.rm = T), 2)
      ), by = .(Group, Login, Symbol)]
      by_symb <- merge(by_symb, open_by_symb, by = c("Group", "Login", "Symbol"), all = TRUE)
    }
    if (!"SwapOpen" %in% names(summary)) {
      summary[, SwapOpen := NA_real_]
      summary[, calcSwapMusdOpen := NA_real_]
    }
    summary[, calcCommissProfitusd := round(-Commiss * BCtoUSD + calcLPcommissusd, 2)]
    summary[, TotalcalcProfitUSD := round(calcCommissProfitusd + calcPMusd + calcSwapMusd + ifelse(is.na(calcSwapMusdOpen), 0, calcSwapMusdOpen), 2)]
    summary[, SwapClosedUSD := round(SwapClosed * BCtoUSD, 2)]
    summary[, CommissUSD    := round(Commiss    * BCtoUSD, 2)]
    summary[, SwapOpenUSD   := round(ifelse(is.na(SwapOpen), 0, SwapOpen) * BCtoUSD, 2)]
    if (!"SwapOpen" %in% names(by_symb)) {
      by_symb[, SwapOpen := NA_real_]
      by_symb[, calcSwapMusdOpen := NA_real_]
    }
    setcolorder(summary, c("Group","Login","Name","TVbalcur","TVusd","calcPMusd","calcSwapMusd","calcSwapMusdOpen","Commiss","SwapClosed","SwapOpen","Dividend","Profit","NetDeposit","TradesCount"))
    setcolorder(by_symb, c("Group","Login","Symbol","TVbalcur","TVusd","calcPMusd","calcSwapMusd","calcSwapMusdOpen","Markup","TVmarcur","TVlot","Commiss","SwapClosed","SwapOpen","Dividend","Profit","TradesCount"))

    return(list(results, summary, by_symb, From, To, by_date))
  })
  
  ################################################################################# 
  
  # OUTPUTS
  ####### Tab User info
  
  output$resUI_user_summary <-  renderUI({

    results <- getData()[[1]]
    summary <- getData()[[2]]
    by_symb <- getData()[[3]]
    by_date <- getData()[[6]]
    
    my_set1 <- RColorBrewer::brewer.pal(9, "Set1")
    if(ncol(results) > 0 & nrow(results) > 0 ){
      tagList(
        h4(paste0("Group: ", summary$Group[1], " | Login: ", summary$Login[1], " | Name: ", summary$Name[1])),
        hr(style = "border-top: 5px solid #828181;"),
        fluidRow(
          column(5,
            ptable(
              prow("TradedVolume",   bval = summary$TVusd,                                                     d = 0),
              prow("Price Markup",   cval = summary$calcPMusd,            accent = TRUE),
              prow("Swap (closed)",  cval = summary$calcSwapMusd,         bval = summary$SwapClosedUSD,        accent = TRUE),
              prow("Swap (open)",    cval = summary$calcSwapMusdOpen,     bval = summary$SwapOpenUSD,          accent = TRUE),
              prow("Commission",     cval = summary$calcCommissProfitusd, bval = summary$CommissUSD,           accent = TRUE),
              prow("LP Cost",        bval = summary$calcLPcommissusd),
              prow("TOTAL",          cval = summary$TotalcalcProfitUSD,                                        total  = TRUE)
            ),
            tags$details(
              tags$summary("Show other metrics...", style = "cursor:pointer;font-size:0.95em;color:#337ab7;margin-top:10px;user-select:none;"),
              br(),
              mtable("Trading metrics (BC)",
                mrow("TradedVolume",      summary$TVbalcur,                                            d = 0),
                mrow("SwapClosed",  summary$SwapClosed),
                mrow("SwapOpen",    ifelse(is.na(summary$SwapOpen), 0, summary$SwapOpen)),
                mrow("Commission)",  summary$Commiss),
                mrow("Dividend",         summary$Dividend),
                mrow("Profit (client)",  summary$Profit),
                mrow("Net Deposit",      summary$NetDeposit,                                          d = 0),
                mrow("TradesCount",      summary$TradesCount,                                         d = 0)
              )
            )
          ),
          column(7, tagList(
            h5("Formulas:"),
            helpText("Price Markup = TVmargincurr * SymbolPrice * (price_markup/100) * PCtoUSD"),
            helpText("Swap Markup: if Swap<0: (Swap/(1+m))-Swap; if Swap>0: (Swap/(1-m))-Swap; * BCtoUSD"),
            helpText("* Swap (open) — accumulated swap of open positions over their entire lifetime, not for the selected period"),
            helpText("Commission Profit = Client CommissionUSD - LP Cost"),
            helpText("LP Cost = TVusd / 1000000 * LP commis")
          ))
        ),
        hr(style = "border-top: 5px solid #828181;"),
        renderDataTable(server=FALSE, DT::datatable(by_symb, escape = FALSE, rownames = F, width = '100%',#server=FALSE, 
                                                    caption = "by SYMBOL:",
                                                    extensions = c('Scroller',  'Buttons'),
                                                    options = list(
                                                      dom = 'Blrtip',
                                                      deferRender = TRUE,
                                                      #scrollX = TRUE,
                                                      scrollY = ifelse(nrow(by_symb)<4,120,300), #600px high
                                                      scroller = TRUE,
                                                      buttons =list(list(extend = 'collection', buttons = c('excel','csv'), text = as.character(icon("download-alt", lib = "glyphicon")), titleAttr = 'Save as...')),
                                                      initComplete = JS(
                                                        "function(settings, json) {",
                                                        "$(this.api().table().header()).css({'background-color': '#e5e5e5', 'color': '#000'});",
                                                        "$(this.api().table().container()).css({'font-size': '80%'});",
                                                        "}")
                                                    )#options
        )#dt
        ), # renderDataTable
        br(),
        renderPlotly(plot_ly(legendgroup = ~Symbol,textinfo = 'label', hoverinfo = 'text+percent', height = 300, textposition='inside',marker = list(colors = my_set1),
                             insidetextfont = list(color = '#FFFFFF'),
                             #outsidetextfont = list(size = 9)
        ) %>%
          add_pie(data = by_symb, hovertemplate = "%{text:,}<br>%{percent:.2%}<extra>%{label}</extra>", #hovertemplate perekrivaet hoverinfo, <extra>outsidetext</extra> "text:," -add thousand separator / 1 251 587.45$ 25.51%
                  labels = ~Symbol, values = ~TVbalcur,  text = ~paste(TVbalcur), domain = list(row = 0, column = 0))%>%
          add_pie(data = by_symb,  hovertemplate = "%{text}<br>%{percent:.2%}<extra>%{label}</extra>",
                  labels = ~Symbol, values = ~TradesCount, text = ~paste(TradesCount, "orders"), domain = list(row = 0, column = 1))%>%
          add_pie(data = by_symb, hovertemplate = "%{text:,.2f}<br>%{percent:.2%}<extra>%{label}</extra>",
                  labels = ~Symbol, values = ~Commiss*(-1),  text = ~paste(Commiss*(-1)), domain = list(row = 0, column = 2))%>%
          layout(title = "", showlegend = T, separators = '. ', # decimal+thousands
                 uniformtext = list(minsize = 11, mode = "hide"),
                 grid =list(rows=1, columns=3),
                 xaxis = list(showgrid = FALSE, zeroline = FALSE, showticklabels = FALSE),
                 yaxis = list(showgrid = FALSE, zeroline = FALSE, showticklabels = FALSE),
                 legend = list(title = list(text='<b> TradeSymbols </b>')),
                 annotations = list(x = c(.167, .5, .833),
                                    y = c(1.08, 1.08, 1.08),
                                    text = c("TradedVolumeBC","TradesCount", "CommissionBC"),
                                    xref = "paper",
                                    yref = "paper",
                                    showarrow = F))
        ), #piecharts
        ggplotly(height = 350, tooltip = c("text"), 
                 ggplot(by_date, aes(x = Date, y = TVbalcur, fill =  reorder(Symbol, -TVbalcur, sum, na.rm = T),
                                     text= paste0("date: ", Date,"<br>",
                                                  "TradedVolBC: ", TVbalcur,"<br>",
                                                  "Symbol: ", Symbol)                 
                                     ))
                 + geom_col()
                 + scale_y_continuous(labels=function(x) format(x, big.mark = " ", scientific = FALSE))
                 + scale_x_datetime(date_labels = "%Y_%b %d") #date_minor_breaks = "1 day", date_breaks = "1 day",
                 + scale_fill_brewer(palette = "Set1")
                 + theme(axis.text.x = element_text(angle=90))
                 + labs(title = "TradedVolumeBC (balance currency)", x = "", y = "", fill = "TradeSymbols")
                 #+ facet_wrap(~ LOGINname, ncol = 1, scales = "free_y")
                 ),
        
        bs_button("Details >>", button_type = "info", button_size = "extra-small" )
        %>% bs_embed_popover(title = "Show detailed Trade Report")
        %>% bs_attach_collapse("BSB"),
        bs_collapse(id = "BSB", content =
                      tagList(
        br(),                
        renderDataTable(server=FALSE, DT::datatable(results, escape = FALSE, rownames = F, width = '100%',#server=FALSE, 
                                                    caption = "TradeReport detailed:",
                                                    extensions = c('Scroller',  'Buttons'),
                                                    options = list(
                                                      dom = 'Blfrtip',
                                                      deferRender = TRUE,
                                                      scrollX = TRUE,
                                                      scrollY = ifelse(nrow(results)<7,200,500), #600px high
                                                      scroller = TRUE,
                                                      buttons =list(list(extend = 'collection', buttons = c('excel','csv'), text = as.character(icon("download-alt", lib = "glyphicon")), titleAttr = 'Save as...')),
                                                      initComplete = JS(
                                                        "function(settings, json) {",
                                                        "$(this.api().table().header()).css({'background-color': '#e5e5e5', 'color': '#000'});",
                                                        "$(this.api().table().container()).css({'font-size': '80%'});",
                                                        "}")
                                                    )#options
        )#dt
        ), # renderDataTable
        bs_button("<<", button_type = "info", button_size = "extra-small" )
        %>% bs_embed_popover(title = "Hide details")
        %>% bs_attach_collapse("BSB")
                      )#bsb taglist
        ), hr(style = "border-top: 1px solid #828181;"),
      ) # taglist
    }  else {h4(span("No data", style = "color: #0a6ed1"))}
    
  })
    
    
  
  
} #shinyServer

shinyApp(ui, server)
