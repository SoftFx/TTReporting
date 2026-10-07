# Statement serializers -- project one statement `model` into an alternative output format.
# Sourced by task_TT_Statements.R AFTER Render.R. HTML is NOT here (it stays a direct
# render_statement() call in build_and_write_account_statement); this file adds JSON and CSV.
#
# model shape (assembled in build_and_write_account_statement):
#   model$meta    : list(login, name, currency, leverage, acc_type, period_type, day_label,
#                        period_from, period_to, domain, period_folder, summary_layout,
#                        trade_table_label)
#   model$summary : the same `summary` list render_statement() gets
#   model$tables  : list(trades, cash, open_positions, orders)  -- raw data.tables/frames
#
# Each serializer returns a NAMED LIST  "<filename suffix>" = "<content string>"
# (one entry today; the write loop in task_TT_Statements.R handles any number).

library(jsonlite)
library(data.table)

# self-contained NULL/empty coalesce (do NOT reuse task_TT_Statements.R's `%||%` -- that one
# errors on length>1 arguments, and config lists here can be vectors)
.or <- function(a, b) if (is.null(a) || length(a) == 0) b else a

# ================================================================================
# shared formatters
# ================================================================================

# CSV money: plain "10000.00" -- NO space thousands separator (space breaks numeric
# parsing in most spreadsheet locales). Deliberately different from Render.R's fmt_money.
.mny <- function(x) { x <- suppressWarnings(as.numeric(x)); ifelse(is.na(x), "", sprintf("%.2f", x)) }

# price to the symbol's own precision (per-row); NA -> blank
.prc <- function(x, p) {
  x <- suppressWarnings(as.numeric(x))
  if (length(x) == 0) return(character(0))
  p <- suppressWarnings(as.integer(p)); p[is.na(p)] <- 5L
  vapply(seq_along(x), function(i) if (is.na(x[i])) "" else formatC(x[i], format = "f", digits = p[i]), character(1))
}

.csvdt <- function(x) { if (length(x) == 0) return(character(0)); ifelse(is.na(x), "", format(x, "%Y-%m-%d %H:%M:%S", tz = "UTC")) }

.side_str <- function(s) ifelse(is.na(s), "", ifelse(s == 0, "buy", ifelse(s == 1, "sell", "-")))

# one combined order label, same string as Render.R's order_type_label ("sell stop", "buy limit", "sell")
.order_label <- function(side, type) {
  suf <- c("0" = "", "1" = " limit", "2" = " stop", "3" = " stop limit")[as.character(type)]
  paste0(.side_str(side), ifelse(is.na(suf), "", suf))
}

# data.frame -> CSV text (header + rows), no trailing newline. 0-row frame => header line only.
.df_to_csv <- function(df) {
  if (is.null(df) || ncol(df) == 0) return("")
  out <- character(0); tc <- textConnection("out", "w", local = TRUE); on.exit(close(tc))
  utils::write.csv(df, tc, row.names = FALSE)
  paste(out, collapse = "\n")
}
.section <- function(title, df) paste0("# ", title, "\n", .df_to_csv(df))
.kv <- function(...) { p <- list(...); data.frame(field = names(p), value = unlist(p, use.names = FALSE), stringsAsFactors = FALSE) }
.foot <- function(df, ...) {
  r <- as.list(rep("", ncol(df))); names(r) <- names(df); r <- modifyList(r, list(...))
  rbind(df, as.data.frame(r, check.names = FALSE, stringsAsFactors = FALSE))
}

# ================================================================================
# JSON  -- own normalized schema v1.0 (machine contract; NOT the HTML labels)
# ================================================================================

.iso <- function(x) {
  if (is.null(x) || length(x) == 0) return(character(0))
  ifelse(is.na(x), NA_character_, format(x, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"))
}
.side_json <- function(s) ifelse(is.na(s), NA_character_, ifelse(s == 0, "buy", ifelse(s == 1, "sell", "unknown")))
.order_label_json <- function(side, type) {
  suf <- c("0" = "", "1" = " limit", "2" = " stop", "3" = " stop limit")[as.character(type)]
  paste0(.side_json(side), ifelse(is.na(suf), "", suf))
}

# JSON numbers are rounded the same way the HTML report rounds them (Render.R fmt_money /
# fmt_price / fmt_pct), once, on the final value -- sums are still computed on raw numbers.
# Before 2026-10-07 JSON carried raw doubles (0.139999999999418, 69.2756520856592).
# NA stays NA (-> null); a missing summary field (NULL) stays absent.
.r2 <- function(x) if (is.null(x)) x else round(as.numeric(x), 2)          # money, percentages
.rp <- function(x, p) {                                                     # price -> symbol precision
  x <- suppressWarnings(as.numeric(x))
  if (length(x) == 0) return(numeric(0))
  p <- suppressWarnings(as.integer(p)); p[is.na(p)] <- 5L
  round(x, p)
}

statement_to_json <- function(model) {
  m <- model$meta; S <- model$summary
  tr <- as.data.frame(model$tables$trades);         op <- as.data.frame(model$tables$open_positions)
  od <- as.data.frame(model$tables$orders);         ch <- as.data.frame(model$tables$cash)

  # per-symbol aggregate of trades (report's "Trade Symbol Summary"), by total_pl desc.
  # swap is always present here (stable schema); HTML/CSV hide it for investment accounts --
  # a presentation choice, not a data difference.
  tsum <- if (nrow(tr) == 0) {
    data.frame(symbol = character(0), commission = numeric(0), swap = numeric(0),
               profit = numeric(0), total_pl = numeric(0), stringsAsFactors = FALSE)
  } else {
    a <- aggregate(cbind(Commission, Swap, Profit) ~ Item, data = tr, FUN = function(z) sum(z, na.rm = TRUE))
    a$total_pl <- a$Commission + a$Swap + a$Profit
    a <- a[order(-a$total_pl), ]
    data.frame(symbol = a$Item, commission = .r2(a$Commission), swap = .r2(a$Swap),
               profit = .r2(a$Profit), total_pl = .r2(a$total_pl), stringsAsFactors = FALSE)
  }

  doc <- list(
    schema_version = "1.0",
    account = list(
      login = as.character(m$login), name = m$name, currency = m$currency,
      leverage = m$leverage, type = m$acc_type,
      is_investment_account = isTRUE(S$is_investment_account)
    ),
    period = list(
      type = m$period_type, label = m$day_label,
      from = .iso(m$period_from), to = .iso(m$period_to)
    ),
    summary = list(
      opening_balance = .r2(S$opening_balance), opening_equity = .r2(S$opening_equity),
      deposits = .r2(S$deposits), withdrawals = .r2(S$withdrawals), deposit_withdrawal = .r2(S$deposit_withdrawal),
      commission = .r2(S$total_commission), swap = .r2(S$total_swap),
      overnight = .r2(S$overnight), dividends = .r2(S$dividends),
      trading_pl = .r2(S$trading_pl), closed_pl = .r2(S$closed_pl), floating_pl = .r2(S$floating_pl),
      closing_balance = .r2(S$balance), closing_equity = .r2(S$equity),
      margin = .r2(S$margin), free_margin = .r2(S$free_margin), margin_level = .r2(S$margin_level),
      balance_total_pl = .r2(S$balance_total_pl), equity_total_pl = .r2(S$equity_total_pl),
      open_positions = list(
        total = S$open_total, long = S$open_long, short = S$open_short,
        long_won_pct = .r2(S$open_long_won_pct), short_won_pct = .r2(S$open_short_won_pct)
      )
    ),
    trade_symbol_summary = tsum,
    trades = data.frame(
      order_id = as.character(tr$OrderId), position_id = as.character(tr$PosId),
      open_time = .iso(tr$OpenTime), close_time = .iso(tr$CloseTime),
      side = .side_json(tr$Side), volume_lots = tr$VolumeLot, symbol = tr$Item,
      open_price = .rp(tr$OpenPrice, tr$Precision), close_price = .rp(tr$ClosePrice, tr$Precision),
      sl = .rp(tr$Sl, tr$Precision), tp = .rp(tr$Tp, tr$Precision),
      commission = .r2(tr$Commission), swap = .r2(tr$Swap), profit = .r2(tr$Profit), comment = tr$Comment,
      stringsAsFactors = FALSE
    ),
    open_positions = data.frame(
      position_id = as.character(op$Ticket), open_time = .iso(op$OpenTime),
      side = .side_json(op$Side), volume_lots = op$VolumeLot, symbol = op$Item,
      open_price = .rp(op$OpenPrice, op$Precision), current_price = .rp(op$MarketPrice, op$Precision),
      sl = .rp(op$Sl, op$Precision), tp = .rp(op$Tp, op$Precision),
      commission = .r2(op$Commission), swap = .r2(op$Swap), profit = .r2(op$Profit),
      stringsAsFactors = FALSE
    ),
    orders = data.frame(
      order_id = as.character(od$OrderId), created_time = .iso(od$Created),
      type = .order_label_json(od$Side, od$Type),
      volume_lots = od$VolumeLot, symbol = od$Item, price = .rp(od$Price, od$Precision),
      sl = .rp(od$Sl, od$Precision), tp = .rp(od$Tp, od$Precision), comment = od$Comment,
      stringsAsFactors = FALSE
    ),
    # cash split into report sections, in report order (Dividends -> Overnight -> Deposit/Withdrawal)
    dividends = local({ r <- ch[ch$RowType == "dividend", , drop = FALSE]
      data.frame(time = .iso(r$OpenTime), symbol = r$Item, commission = .r2(r$Commission),
                 taxes = .r2(r$Taxes), amount = .r2(r$Amount), stringsAsFactors = FALSE) }),
    overnight = local({ r <- ch[ch$RowType == "overnight", , drop = FALSE]
      data.frame(time = .iso(r$OpenTime), amount = .r2(r$Amount), stringsAsFactors = FALSE) }),
    deposits_withdrawals = local({ r <- ch[ch$RowType == "balance", , drop = FALSE]
      data.frame(time = .iso(r$OpenTime), amount = .r2(r$Amount), comment = r$Comment, stringsAsFactors = FALSE) })
  )

  json <- jsonlite::toJSON(doc, auto_unbox = TRUE, digits = NA, na = "null", pretty = TRUE)
  list(".json" = paste0(as.character(json), "\n"))
}

# ================================================================================
# CSV  -- one sectioned file; section titles / field labels / column headers copied
# VERBATIM from the HTML report (Render.R). Deliberate deviations from HTML:
#   * money plain "10000.00" (no space grouping)
#   * HTML footer lines become a labelled last row (ragged: blanks in other columns)
# Net vs Gross column sets mirror render_statement()'s own is_net branching.
# ================================================================================

statement_to_csv <- function(model) {
  m <- model$meta; S <- model$summary
  is_net <- identical(m$acc_type, "Net")
  tr <- as.data.frame(model$tables$trades);  op <- as.data.frame(model$tables$open_positions)
  od <- as.data.frame(model$tables$orders);  ch <- as.data.frame(model$tables$cash)
  div <- ch[ch$RowType == "dividend", , drop = FALSE]
  ovn <- ch[ch$RowType == "overnight", , drop = FALSE]
  bal <- ch[ch$RowType == "balance",  , drop = FALSE]

  # Investment-account wording (Asset / P&L / Locked cash / Available cash) -- same single source
  # as the HTML: summary$labels from task_TT_Statements.R. Data frames below are built with the
  # default names and renamed by relabel() just before output.
  L <- modifyList(list(symbol = "Symbol", pl = "P/L", used_margin = "Used Margin", free_margin = "Free Margin"),
                  .or(S$labels, list()))
  kv1 <- function(field, value) data.frame(field = field, value = value, stringsAsFactors = FALSE)
  relabel <- function(df) {
    # Swap column dropped for investment accounts (Trade Symbol Summary, Trades, Open Positions)
    if (identical(S$show_swap_column, FALSE)) df <- df[, names(df) != "Swap", drop = FALSE]
    names(df) <- gsub("P/L", L$pl, sub("^Symbol$", L$symbol, names(df)), fixed = TRUE)
    df
  }

  # -- Account Statement: HTML account-info block rows --
  acc <- do.call(rbind, Filter(Negate(is.null), list(
    data.frame(field = paste0(m$period_type, " Statement for"), value = m$day_label, stringsAsFactors = FALSE),
    data.frame(field = "Account",  value = as.character(m$login), stringsAsFactors = FALSE),
    data.frame(field = "Name",     value = m$name,     stringsAsFactors = FALSE),
    data.frame(field = "Currency", value = m$currency, stringsAsFactors = FALSE),
    if (isTRUE(S$show_leverage)) data.frame(field = "Leverage", value = paste0("1:", m$leverage), stringsAsFactors = FALSE),
    data.frame(field = "Account Type", value = .or(S$acc_type_label, m$acc_type), stringsAsFactors = FALSE)
  )))

  # -- Account Summary: HTML single_column_rows labels/values --
  dep_wd <- sprintf("%s / %s", .mny(S$deposits), .mny(S$withdrawals))
  summ <- do.call(rbind, Filter(Negate(is.null), list(
    .kv("Open Equity" = .mny(S$opening_equity)),
    .kv("Open Balance" = .mny(S$opening_balance)),
    kv1(paste("Realised", L$pl), .mny(S$balance_total_pl)),
    .kv("Commission" = .mny(S$total_commission)),
    if (isTRUE(S$show_swap_or_overnight_row))
      data.frame(field = .or(S$swap_or_overnight_label, "Swap"), value = .mny(S$swap_or_overnight), stringsAsFactors = FALSE),
    .kv("Dividends" = .mny(S$dividends)),
    .kv("Deposit/Withdrawal" = dep_wd),
    .kv("Close Balance" = .mny(S$balance)),
    kv1(paste("Unrealised", L$pl, "(Floating)"), .mny(S$floating_pl)),
    kv1(L$used_margin, .mny(S$margin)),
    kv1(L$free_margin, .mny(S$free_margin)),
    .kv("Close Equity" = .mny(S$equity))
  )))

  # -- Trade Symbol Summary: per-symbol aggregate + "Total:" row (Swap hidden for investment) --
  show_swap <- !identical(S$show_swap_column, FALSE)
  if (nrow(tr) == 0) {
    ssum <- if (show_swap) data.frame(Symbol = character(0), Commission = character(0), Swap = character(0), Profit = character(0), `Total P/L` = character(0), check.names = FALSE)
            else            data.frame(Symbol = character(0), Commission = character(0), Profit = character(0), `Total P/L` = character(0), check.names = FALSE)
    ssum <- .foot(ssum, Symbol = "Total:", Commission = .mny(0), Profit = .mny(0), `Total P/L` = .mny(0))
    if (show_swap) ssum$Swap[nrow(ssum)] <- .mny(0)
  } else {
    a <- aggregate(cbind(Commission, Swap, Profit) ~ Item, data = tr, FUN = function(z) sum(z, na.rm = TRUE))
    a$tpl <- a$Commission + a$Swap + a$Profit
    a <- a[order(-a$tpl), ]
    ssum <- if (show_swap) data.frame(Symbol = a$Item, Commission = .mny(a$Commission), Swap = .mny(a$Swap),
                                      Profit = .mny(a$Profit), `Total P/L` = .mny(a$tpl), check.names = FALSE, stringsAsFactors = FALSE)
            else            data.frame(Symbol = a$Item, Commission = .mny(a$Commission),
                                      Profit = .mny(a$Profit), `Total P/L` = .mny(a$tpl), check.names = FALSE, stringsAsFactors = FALSE)
    ssum <- if (show_swap) .foot(ssum, Symbol = "Total:", Commission = .mny(sum(a$Commission)), Swap = .mny(sum(a$Swap)),
                                 Profit = .mny(sum(a$Profit)), `Total P/L` = .mny(sum(a$tpl)))
            else            .foot(ssum, Symbol = "Total:", Commission = .mny(sum(a$Commission)),
                                 Profit = .mny(sum(a$Profit)), `Total P/L` = .mny(sum(a$tpl)))
  }

  # -- Trades (Net vs Gross column set, mirrors render_statement) + "Total P/L:" footer --
  if (is_net) {
    trd <- data.frame(
      `Order ID` = as.character(tr$OrderId), `Position ID` = as.character(tr$PosId),
      `Trade Time` = .csvdt(tr$OpenTime), Type = .side_str(tr$Side),
      Volume = .mny(tr$VolumeLot), Symbol = tr$Item,
      `Open Price` = .prc(tr$OpenPrice, tr$Precision), `Close Price` = .prc(tr$ClosePrice, tr$Precision),
      Commission = .mny(tr$Commission), Swap = .mny(tr$Swap), Profit = .mny(tr$Profit),
      Comment = as.character(tr$Comment), check.names = FALSE, stringsAsFactors = FALSE)
  } else {
    trd <- data.frame(
      `Order ID` = as.character(tr$OrderId), `Position ID` = as.character(tr$PosId),
      `Open Time` = .csvdt(tr$OpenTime), Type = .side_str(tr$Side),
      Volume = .mny(tr$VolumeLot), Symbol = tr$Item,
      `Open Price` = .prc(tr$OpenPrice, tr$Precision),
      `S / L` = .prc(tr$Sl, tr$Precision), `T / P` = .prc(tr$Tp, tr$Precision),
      `Close Time` = .csvdt(tr$CloseTime), `Close Price` = .prc(tr$ClosePrice, tr$Precision),
      Commission = .mny(tr$Commission), Swap = .mny(tr$Swap), Profit = .mny(tr$Profit),
      Comment = tr$Comment, check.names = FALSE, stringsAsFactors = FALSE)
  }
  trd <- .foot(trd, `Order ID` = paste0("Total ", L$pl, ":"), Profit = .mny(S$closed_pl))

  # -- Open Positions + "Floating P/L:" footer --
  if (is_net) {
    opn <- data.frame(
      ID = as.character(op$Ticket), `Open Time` = .csvdt(op$OpenTime), Type = .side_str(op$Side),
      Volume = .mny(op$VolumeLot), Symbol = op$Item,
      Price = .prc(op$OpenPrice, op$Precision), `Current Price` = .prc(op$MarketPrice, op$Precision),
      Swap = .mny(op$Swap), Profit = .mny(op$Profit), check.names = FALSE, stringsAsFactors = FALSE)
  } else {
    opn <- data.frame(
      ID = as.character(op$Ticket), `Open Time` = .csvdt(op$OpenTime), Type = .side_str(op$Side),
      Volume = .mny(op$VolumeLot), Symbol = op$Item,
      Price = .prc(op$OpenPrice, op$Precision),
      `S / L` = .prc(op$Sl, op$Precision), `T / P` = .prc(op$Tp, op$Precision),
      `Current Price` = .prc(op$MarketPrice, op$Precision),
      Commission = .mny(op$Commission), Swap = .mny(op$Swap), Profit = .mny(op$Profit),
      check.names = FALSE, stringsAsFactors = FALSE)
  }
  opn <- .foot(opn, ID = paste0("Floating ", L$pl, ":"), Profit = .mny(S$floating_pl))

  # -- Orders (no footer in HTML) --
  if (is_net) {
    ord <- data.frame(
      ID = as.character(od$OrderId), `Open Time` = .csvdt(od$Created),
      Type = .order_label(od$Side, od$Type), Volume = .mny(od$VolumeLot), Symbol = od$Item,
      Price = .prc(od$Price, od$Precision), Comment = od$Comment,
      check.names = FALSE, stringsAsFactors = FALSE)
  } else {
    ord <- data.frame(
      ID = as.character(od$OrderId), `Open Time` = .csvdt(od$Created),
      Type = .order_label(od$Side, od$Type), Volume = .mny(od$VolumeLot), Symbol = od$Item,
      Price = .prc(od$Price, od$Precision),
      `S / L` = .prc(od$Sl, od$Precision), `T / P` = .prc(od$Tp, od$Precision),
      Comment = .or(od$Comment, character(0)), check.names = FALSE, stringsAsFactors = FALSE)
  }

  # -- Dividends / Overnight / Deposit-Withdrawal + "Total:" rows (match cash_table_block visibility) --
  divd <- .foot(data.frame(Date = .csvdt(div$OpenTime), Symbol = div$Item,
                           Commission = .mny(div$Commission), Taxes = .mny(div$Taxes), Amount = .mny(div$Amount),
                           check.names = FALSE, stringsAsFactors = FALSE),
                Date = "Total:", Amount = .mny(S$dividends))
  depw <- .foot(data.frame(Date = .csvdt(bal$OpenTime), Comment = bal$Comment, Amount = .mny(bal$Amount),
                           check.names = FALSE, stringsAsFactors = FALSE),
                Date = "Total:", Amount = .mny(S$deposit_withdrawal))

  blocks <- c(
    .section("Account Statement",    acc),
    .section("Account Summary",      summ),
    .section(paste("Trade", L$symbol, "Summary"), relabel(ssum)),
    .section(if (is_net) "Trades" else "Closed Trades", relabel(trd)),
    .section("Open Positions",       relabel(opn)),
    .section("Orders",               relabel(ord)),
    .section("Dividends",            relabel(divd)),
    if (isTRUE(S$show_overnight))
      .section("Overnight", .foot(data.frame(Date = .csvdt(ovn$OpenTime), Amount = .mny(ovn$Amount),
                                             check.names = FALSE, stringsAsFactors = FALSE),
                                  Date = "Total:", Amount = .mny(S$overnight))),
    .section("Deposit/Withdrawal",   depw)
  )
  list(".csv" = paste0(paste(blocks, collapse = "\n\n"), "\n"))
}

# content types by file suffix -- used by write_statement_file()
STATEMENT_CONTENT_TYPES <- c(
  ".html" = "text/html; charset=utf-8",
  ".csv"  = "text/csv; charset=utf-8",
  ".json" = "application/json; charset=utf-8"
)
