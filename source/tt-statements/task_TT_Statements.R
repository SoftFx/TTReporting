library(data.table)
library(lubridate)
source('../common/PostgresHost.R')
source('Functions.R')
source('Stats.R')
source('Render.R')
source('Serializers.R')   # statement_to_json / statement_to_csv (+ STATEMENT_CONTENT_TYPES)
source('SeaweedClient.R')
source('../common/RMonitoringClient.R')
options(warn = -1)
Sys.setenv("TZ" = "UTC")

# TrType is exclusive per account type -- Gross accounts only ever produce TrType=4 (PositionClosed,
# clean open+close pair per row); Net accounts only ever produce TrType=3 (OrderFilled, one row per
# fill -- may be opening/adding exposure with no close data yet). fcoalesce() lets one row-builder
# handle both shapes: for Gross rows every field is already populated so the fallback is a no-op.
`%||%` <- function(a, b) if (is.null(a) || is.na(a)) b else a

# Writes one rendered statement file (suffix = "_mail.html" / ".csv" / ".json") to every
# configured storage backend. config$storage$backend may be a single value ("disk" /
# "seaweed") OR a vector of both. Same contract as before: returns TRUE on success, THROWS
# on failure -- callers rely on their own tryCatch(..., error = function(e) FALSE).
write_statement_file <- function(content, domain, period_folder, login, suffix, storage_root, config) {
  backends <- config$storage$backend
  if (is.null(backends) || length(backends) == 0) backends <- "disk"
  ext <- sub("^.*(\\.[a-z]+)$", "\\1", suffix)                 # "_mail.html" -> ".html"
  ctype <- STATEMENT_CONTENT_TYPES[[ext]]
  if (is.null(ctype)) ctype <- "application/octet-stream"

  for (backend in backends) {
    if (backend == "seaweed") {
      put_statement_object(content, domain = domain, period_folder = period_folder, login = login,
                           suffix = suffix, content_type = ctype,
                           filer_url = config$storage$seaweed_filer_url)
    } else if (backend == "disk") {
      out_dir <- file.path(storage_root, domain, period_folder)
      dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
      writeLines(content, file.path(out_dir, paste0(login, suffix)), useBytes = TRUE)
    } else {
      stop("unknown storage.backend value: ", backend)
    }
  }
  TRUE
}

# Build & write the statement HTML for a single account. Returns TRUE/FALSE (written or skipped/error).
build_and_write_account_statement <- function(login, accRow, snapRow, openSnapRow, posSnapDT, ordSnapDT, tradesDT,
                                               day_label, storage_root, domain, period_folder, config,
                                               period_type = "Daily", summary_layout = "cards",
                                               dayFrom = NULL, dayTo = NULL){
  day_trades <- tradesDT[Login == login]
  day_trades5 <- day_trades[TrType == 5] # Balance: deposit/withdrawal/dividend/fee
  trade_type <- if (accRow$AccType == "Gross") 4L else 3L
  trade_table_label <- if (accRow$AccType == "Gross") "Closed Trades" else "Trades"
  is_net_acc <- accRow$AccType == "Net"

  # TrReason 11 (Dividend) and 13 (Overnight swap/interest) each get their own Summary line
  # below, same treatment -- both excluded from the generic Deposit/Withdrawal bucket.
  deposit_withdrawal <- sum(day_trades5[!TrReason %in% c(11, 13), BalanceMovement], na.rm = TRUE)

  raw_trades <- day_trades[TrType == trade_type]
  trade_results <- raw_trades[, .(Time = TrTime,
                                   Result = fcoalesce(Profit, 0) + fcoalesce(Commission, 0) +
                                            fcoalesce(Swap, 0) + fcoalesce(Taxes, 0),
                                   Side,
                                   # Net's TrType=3 mixes opening/adding fills (PosClosed NA) in with
                                   # actual closes (PosClosed set) -- same is_close test as trade_rows
                                   # below. Gross's TrType=4 is exclusively closes, so IsClose is TRUE
                                   # for every row there already; this only changes Net's stats.
                                   IsClose = !is.na(PosClosed))]
  closed_raw_trades <- raw_trades[!is.na(PosClosed)]

  # A row with no PosClosed/PosClosePrice is an opening/adding fill (Net) -- open info comes
  # from TrTime/OrderFillPrice (TrTime IS the open moment here) and close info stays blank.
  # A row that HAS them is a close (Gross always; Net when this fill flattens/reduces exposure) --
  # TrTime there is the CLOSE moment, so open PRICE must come strictly from PosOpenPrice, never
  # falling back to OrderFillPrice. Open TIME is different: PosOpened simply doesn't exist for Net
  # (confirmed empty on real data) -- so Net always uses TrTime for open time too, on every row.
  # Gross does have a real, distinct PosOpened, so it keeps genuine Open Time vs Close Time.
  trade_rows <- raw_trades[, {
    is_close <- !is.na(PosClosed)
    is_net_acc <- accRow$AccType == "Net"
    open_time <- if (is_net_acc) TrTime else PosOpened
    volume_amount <- if (is_net_acc) OrderLastFillAmount else PosLastAmount
    .(OrderId, PosId,
      OpenTime = open_time, CloseTime = PosClosed,
      Side, VolumeLot = volume_amount / ContractSize, Item = Symbol,
      OpenPrice = fifelse(is_close, PosOpenPrice, OrderFillPrice), ClosePrice = PosClosePrice,
      Sl, Tp, Precision = SymbolPrecision,
      Commission, Taxes, Swap, Profit, Comment = UserComment)
  }]
  setorder(trade_rows, OpenTime)

  # Deposit/Withdrawal, Dividends, Overnight -- rendered as their own tables under Trades
  # (used to be interleaved into trade_rows by RowType; split out on request 2026-08-17).
  cash_rows <- day_trades5[, .(RowType = fifelse(TrReason == 11, "dividend",
                                                  fifelse(TrReason == 13, "overnight", "balance")),
                                OpenTime = TrTime, Item = Symbol,
                                Commission = Commission, Taxes = Taxes,
                                Amount = BalanceMovement, Comment = UserComment)]
  setorder(cash_rows, OpenTime)

  # Open price lives in a different column depending on account type: Net accounts populate
  # AveragePrice (Price is always 0 there); Gross accounts populate Price (AveragePrice is
  # always 0 there) -- confirmed on real data across all domains, not just a one-off gap.
  open_rows <- posSnapDT[Login == login][, .(
    Ticket = PositionId, OpenTime = Created, Side, VolumeLot = Amount / ContractSize, Item = Symbol,
    OpenPrice = if (is_net_acc) AveragePrice else Price, Sl = StopLoss, Tp = TakeProfit, Precision = SymbolPrecision,
    MarketPrice = ifelse(Side == 0, CurrentBestBid, CurrentBestAsk),
    Commission, Swap, Profit
  )]

  # Price/StopPrice are mutually exclusive depending on order type (Limit populates Price,
  # Stop populates StopPrice, never both) -- confirmed on real data across all domains. Coalescing
  # avoids depending on the exact Type-number-to-label mapping (order_type_label in Render.R
  # already flags that mapping as best-effort/uncertain).
  order_rows <- ordSnapDT[Login == login][, .(
    OrderId, Created, Side, Type, VolumeLot = Amount / ContractSize, Item = Symbol,
    Price = fcoalesce(Price, StopPrice), Sl = StopLoss, Tp = TakeProfit, Precision = SymbolPrecision,
    State, Comment = UserComment
  )]

  dividend_and_fee <- sum(day_trades5[TrReason == 11, BalanceMovement], na.rm = TRUE)
  overnight <- sum(day_trades5[TrReason == 13, BalanceMovement], na.rm = TRUE)
  # "Investment account" -- Groups.TradingMode=1 AND Groups.StopOutMode=0 AND Accounts.Leverage=1
  # (the actual business definition, found 2026-08-26 -- broader than Groups.PerformOvernight,
  # which only flags the subset currently enrolled in Overnight, a separate/test feature for
  # investment accounts, see is_investment_account's narrower cousin below). isTRUE() around each
  # comparison makes this NA-safe: a GroupFk that doesn't match any Groups row leaves
  # TradingMode/StopOutMode NA, which isTRUE() turns into FALSE rather than propagating NA
  # through &&. Drives Leverage (hidden in the header) and the Swap column (hidden in Trade
  # Symbol Summary, both in Render.R) -- their per-trade Swap is ~always 0 across the whole
  # class regardless of Overnight enrolment, confirmed on real data 2026-08-26.
  is_investment_account <- isTRUE(accRow$TradingMode == 1) && isTRUE(accRow$StopOutMode == 0) && isTRUE(accRow$Leverage == 1)
  # Overnight itself is a separate, still-test feature scoped to investment accounts
  # (Groups.PerformOvernight) -- NOT the same thing as "is this an investment account" (learned
  # 2026-08-26, after briefly conflating the two). Drives the merged Swap/Overnight Summary line
  # and the separate Overnight table's visibility (Render.R) -- gated on is_investment_account
  # (the feature only ever applies there) with a data-loss safety net on top: the flag being
  # FALSE hides Overnight UNLESS this period actually has real Overnight rows (e.g. the flag got
  # toggled off on an account that still carries real Overnight history) -- never silently drop
  # real data from the report just because the flag says "off" today.
  show_overnight <- is_investment_account && (isTRUE(accRow$PerformOvernight %||% FALSE) || nrow(cash_rows[RowType == "overnight"]) > 0)
  starting_balance <- snapRow$Balance - sum(trade_results$Result) - deposit_withdrawal - dividend_and_fee - overnight
  stats <- compute_trade_stats(trade_results[IsClose == TRUE], starting_balance = starting_balance)

  # The balance graph reflects every balance-changing event of the day (trades AND
  # deposits/withdrawals/dividends/fees), in chronological order -- unlike the stats above,
  # which only count trade P&L (TrType 3/4).
  balance_events <- rbind(
    trade_results[, .(Time, Result)],
    day_trades5[, .(Time = TrTime, Result = fcoalesce(BalanceMovement, 0))]
  )
  setorder(balance_events, Time)
  bal_curve <- balance_curve_points(balance_events, starting_balance = starting_balance)

  summary <- list(
    # Accounts created mid-period have no opening snapshot (openSnapRow comes back all-NA) --
    # default to 0, matching TT Manager's own convention (verified in TTGainReport's
    # equity_report_detailed_optimized.sql, COALESCE(start_balance, 0)). This isn't a placeholder:
    # the account's initial funding still lands in Deposit/Withdrawal below, so Total P/L
    # (Closing - Opening - Deposit/Withdrawal) still comes out correct with Opening = 0.
    opening_balance = openSnapRow$Balance %||% 0,
    opening_equity = openSnapRow$Equity %||% 0,
    deposit_withdrawal = deposit_withdrawal,
    deposits = sum(day_trades5[!TrReason %in% c(11, 13) & BalanceMovement > 0, BalanceMovement], na.rm = TRUE),
    withdrawals = sum(day_trades5[!TrReason %in% c(11, 13) & BalanceMovement < 0, BalanceMovement], na.rm = TRUE),
    # Trades-only (not trade_rows, which also carries balance/dividend rows) -- Dividends below
    # is already the NET TrReason=11 amount (BalanceMovement, commission/taxes already deducted),
    # so folding dividend-row commission in here too would double-count it.
    total_commission = sum(raw_trades$Commission, na.rm = TRUE),
    total_swap = sum(raw_trades$Swap, na.rm = TRUE),
    positive_swap = sum(raw_trades$Swap[raw_trades$Swap > 0], na.rm = TRUE),
    negative_swap = sum(raw_trades$Swap[raw_trades$Swap < 0], na.rm = TRUE),
    dividends = dividend_and_fee,
    overnight = overnight,
    # Merged line picks Overnight only when show_overnight says so (flag on, or real data this
    # period despite the flag) -- everyone else (non-investment accounts, and investment accounts
    # not currently enrolled with no Overnight history) gets the plain Swap line as always.
    # total_swap/overnight are kept as-is: still needed for starting_balance, the Close Swap
    # detail card, and the Balance->Total P/L identity in VALIDATION.md.
    is_investment_account = is_investment_account,
    # Precomputed once here (not left as `!isTRUE(summary$is_investment_account)` re-derived
    # independently at each of Render.R's two Leverage call sites, "cards" and "single_column") --
    # code review 2026-08-26 flagged the two-copies version as a drift risk (same class of bug as
    # this project's earlier Gross/Net OpenPrice bug: a future change to the condition only
    # touching one of the two spots). Both now just read this one value.
    show_leverage = !is_investment_account,
    show_overnight = show_overnight,
    # Whether the merged Swap/Overnight line appears in Summary at all -- TRUE unconditionally
    # for non-investment accounts (always "Swap"); for investment accounts, only when
    # show_overnight is TRUE (Render.R drops the row entirely otherwise, per the "investment
    # accounts never show Swap" rule -- no Swap fallback for them, unlike everyone else).
    show_swap_or_overnight_row = !is_investment_account || show_overnight,
    swap_or_overnight_label = if (show_overnight) "Overnight" else "Swap",
    swap_or_overnight = if (show_overnight) overnight else sum(raw_trades$Swap, na.rm = TRUE),
    # Pure trading P/L -- SUM(Profit) only (no Commission/Swap/Taxes), over CLOSED trades only
    # (Net's opening/adding fills always carry Profit=0, so scoping to closed doesn't change the
    # number in practice, but keeps this conceptually "realized trading result", matching
    # Close Trades' closed-only count). trading_profit/trading_loss are the same split by sign,
    # for the Trading P/L card -- previously that card's Profit/Loss came from `stats` (Result,
    # WITH Commission/Swap/Taxes) while Total was already pure Profit -- inconsistent formulas
    # under one card. All three are now the same pure-Profit basis.
    trading_profit = sum(closed_raw_trades$Profit[closed_raw_trades$Profit > 0], na.rm = TRUE),
    trading_loss = sum(closed_raw_trades$Profit[closed_raw_trades$Profit < 0], na.rm = TRUE),
    trading_pl = sum(closed_raw_trades$Profit, na.rm = TRUE),
    closed_pl = sum(trade_results$Result),
    floating_pl = snapRow$Equity - snapRow$Balance,
    # Period P/L net of deposit/withdrawal activity, from the balance side (realized only) and
    # from the equity side (realized + change in floating, since Equity already embeds floating).
    balance_total_pl = snapRow$Balance - (openSnapRow$Balance %||% 0) - deposit_withdrawal,
    equity_total_pl = snapRow$Equity - (openSnapRow$Equity %||% 0) - deposit_withdrawal,
    margin = snapRow$Margin, balance = snapRow$Balance, equity = snapRow$Equity,
    free_margin = snapRow$Equity - snapRow$Margin, margin_level = snapRow$MarginLevel,
    open_total = nrow(open_rows),
    open_long = sum(open_rows$Side == 0, na.rm = TRUE),
    open_short = sum(open_rows$Side == 1, na.rm = TRUE),
    # "won %" for still-open positions = currently profitable by floating P/L (no closed win/loss
    # yet, so this is the closest analogue to the old "Short/Long Positions (won %)" stat).
    open_long_won_pct = {
      n <- sum(open_rows$Side == 0, na.rm = TRUE)
      if (n > 0) 100 * sum(open_rows$Side == 0 & open_rows$Profit > 0, na.rm = TRUE) / n else NA_real_
    },
    open_short_won_pct = {
      n <- sum(open_rows$Side == 1, na.rm = TRUE)
      if (n > 0) 100 * sum(open_rows$Side == 1 & open_rows$Profit > 0, na.rm = TRUE) / n else NA_real_
    },
    balance_curve = bal_curve
  )

  # Structured model every non-HTML serializer projects from (Serializers.R). HTML keeps its
  # own direct render_statement() call below -- byte-identical to before.
  model <- list(
    meta = list(login = login, name = accRow$Name, currency = accRow$Currency, leverage = accRow$Leverage,
                acc_type = accRow$AccType, period_type = period_type, day_label = day_label,
                period_from = dayFrom, period_to = dayTo, domain = domain, period_folder = period_folder,
                summary_layout = summary_layout, trade_table_label = trade_table_label),
    summary = summary, stats = stats,
    tables = list(trades = trade_rows, cash = cash_rows, open_positions = open_rows, orders = order_rows)
  )

  # Which formats to write, from config. NOT via `%||%` -- that operator errors on a vector.
  formats <- config$storage$formats
  if (is.null(formats) || length(formats) == 0) formats <- "html"

  for (fmt in formats) {
    outputs <- switch(as.character(fmt),
      html = list("_mail.html" = render_statement(
                    login = login, name = accRow$Name, currency = accRow$Currency,
                    leverage = accRow$Leverage, day_label = day_label,
                    acc_type = accRow$AccType, trade_table_label = trade_table_label,
                    trade_rows = trade_rows, cash_rows = cash_rows, open_rows = open_rows, order_rows = order_rows,
                    summary = summary, stats = stats, period_type = period_type,
                    summary_layout = summary_layout)),
      json = statement_to_json(model),
      csv  = statement_to_csv(model),
      stop("unknown storage.formats value: ", fmt))
    for (sfx in names(outputs)) {
      write_statement_file(outputs[[sfx]], domain = domain, period_folder = period_folder,
                           login = login, suffix = sfx, storage_root = storage_root, config = config)
    }
  }
  TRUE
}

execute_task_tt_statements <- function(config, dayFrom, dayTo, day_label, storage_root, period_folder, task_exec_log_path,
                                        only_logins = NULL, monitor_paths = NULL, period_type = "Daily",
                                        summary_layout = "cards"){
  if (is.null(monitor_paths)) monitor_paths <- config$monitoring$connection$paths
  # Timestamp of this actual execution -- distinct from `day_label`/`period_folder` (the TARGET
  # period being processed) so re-runs of the same period are distinguishable in the log.
  run_at <- strftime(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  statusError <- NULL
  domain_summary <- data.table()

  if (length(config$database_connections[["tt"]]) > 0) {
    for (db in names(config$database_connections[["tt"]])) {
      tryCatch({
        Credstt <- config$database_connections$tt[[db]]
        # Single connection reused for all 6 queries below (each getter used to open/close its
        # own -- 6 connect/disconnect round-trips per domain instead of 1).
        ConnectToDB(dbname = Credstt$postgre_DB, user = Credstt$postgre_USER, password = Credstt$postgre_PASSWORD, host = Credstt$postgre_HOST)

        # Snapshots are written at 00:00:00 each day and represent EOD state of the day that just
        # ended -- so the snapshot for the target day [dayFrom, dayTo) is the one timestamped at
        # dayTo (the start of the *next* day), not at dayFrom (which would be EOD of the day before).
        snapFrom <- dayTo
        snapTo   <- dayTo + days(1)

        # Opening balance/equity for the period: by the same logic, that's the EOD snapshot of
        # the day immediately BEFORE the period starts -- timestamped exactly at dayFrom.
        openSnapFrom <- dayFrom
        openSnapTo   <- dayFrom + days(1)

        accounts <- getAccountsTT(Credstt)
        accounts <- accounts[!Login %in% config$business_parameters$excluded_users]
        snaps    <- getDayAccountSnapshotsTT(Credstt, snapFrom, snapTo)

        if (nrow(snaps) == 0) {
          # No EOD snapshot rows at all for this date -- distinct from "0 accounts after
          # filtering" (which is a legitimate outcome). Usually means the DB snapshot job
          # hasn't run yet / ran against the wrong date, so there's nothing to render;
          # skip the remaining per-domain queries and flag it instead of writing an empty batch.
          msg <- paste0("no account snapshots in DB for ", format(snapFrom, "%Y-%m-%d"), " -- skipping, nothing to report")
          print(paste(db, ":", msg))
          statusError <- c(statusError, paste0(db, ": ", msg))
          domain_summary <- rbind(domain_summary, data.table(
            run_at = run_at, period_folder = period_folder, DB = db, day = day_label,
            accounts = 0L, written = 0L, errors = 1L))
        } else {
          snaps    <- snaps[!Domain %in% config$business_parameters$excluded_domains]
          openSnaps <- getDayAccountSnapshotsTT(Credstt, openSnapFrom, openSnapTo)
          posSnaps <- getDayPositionSnapshotsTT(Credstt, snapFrom, snapTo)
          ordSnaps <- getDayOrderSnapshotsTT(Credstt, snapFrom, snapTo)
          trades   <- getDayTradeReportsTT(Credstt, dayFrom, dayTo)

          # NB: bigint columns come back from RPostgres as integer64 (bit64 package).
          # base::intersect() silently returns empty when comparing integer64 to a plain
          # numeric vector -- bit64 overrides `%in%` (and `==`) correctly, so use that instead.
          logins <- accounts$Login[accounts$Login %in% snaps$Login]
          if (!is.null(only_logins)) logins <- logins[logins %in% only_logins]
          written <- 0L
          acc_errors <- NULL
          # NB: `for (login in logins)` silently strips the integer64 class per-iteration
          # (bit64 quirk) -- index-based access via `logins[i]` preserves it.
          for (li in seq_along(logins)) {
            login <- logins[li]
            res <- tryCatch({
              build_and_write_account_statement(
                login = login,
                accRow = accounts[Login == login][1],
                snapRow = snaps[Login == login][1],
                openSnapRow = openSnaps[Login == login][1],
                posSnapDT = posSnaps, ordSnapDT = ordSnaps, tradesDT = trades,
                day_label = day_label, storage_root = storage_root, domain = db,
                period_folder = period_folder, config = config, period_type = period_type,
                summary_layout = summary_layout, dayFrom = dayFrom, dayTo = dayTo)
            }, error = function(e) { print(e); acc_errors <<- c(acc_errors, paste(login, substr(e$message, 1, 60))); FALSE })
            if (isTRUE(res)) written <- written + 1L
          }

          print(paste(db, ": statements written =", written, "/ accounts =", length(logins)))
          if (length(acc_errors) > 0) statusError <- c(statusError, paste0(db, ": ", acc_errors))

          domain_summary <- rbind(domain_summary, data.table(
            run_at = run_at, period_folder = period_folder, DB = db, day = day_label,
            accounts = length(logins), written = written, errors = length(acc_errors)))
        }
      }, error = function(e) {
        print(e)
        statusError <<- c(statusError, paste(db, substr(e$message, 1, 70)))
      }, finally = DissconnectFromDB())
    }
  }

  if (nrow(domain_summary) == 0) {
    domain_summary <- data.table(run_at = run_at, period_folder = period_folder, DB = NA_character_,
                                  day = day_label, accounts = NA_integer_, written = NA_integer_,
                                  errors = NA_integer_)
  }
  domain_summary[, err := paste(statusError, collapse = "; ")]

  if (isTRUE(config$monitoring$enabled %||% TRUE)) {
    tryCatch({
      if (length(statusError) > 0) {
        UpdateBoolSensorValue(productKey = config$monitoring$connection$productKey, address = config$monitoring$connection$address,
                              port = config$monitoring$connection$port, path = monitor_paths[2],
                              TRUE, status = 1, comment = paste("!!!!TT_STATEMENTS_ERRORS:\n", paste(statusError, collapse = "; ")))
      }
      UpdateIntSensorValue(productKey = config$monitoring$connection$productKey, address = config$monitoring$connection$address,
                           port = config$monitoring$connection$port, path = monitor_paths[1],
                           value = sum(domain_summary$written, na.rm = TRUE), status = 1,
                           comment = paste("Statements for", day_label, ":", paste(domain_summary$DB, domain_summary$written, sep = "=", collapse = ", ")))
    }, error = function(e){ print("HSMconnErr:"); print(e) })
  } else {
    cat("HSM monitoring disabled (config$monitoring$enabled: false) -- skipping notification.\n")
  }

  if (file.exists(task_exec_log_path)) {
    fwrite(domain_summary, task_exec_log_path, append = TRUE)
  } else {
    fwrite(domain_summary, task_exec_log_path)
  }

  list(TRUE, dayFrom, dayTo, sum(domain_summary$written, na.rm = TRUE))
}
