library(data.table)

# Trade/period statistics in the MT4/MT5 "Results" style, computed over a single day's
# closed trades only (not lifetime) -- daily statement, not full-history statement.
# resultsDT: data.table with columns Time (POSIXct), Result (numeric per-trade P/L),
#            Side (0=Buy/1=Sell, optional -- used for short/long won% only).
# starting_balance: balance immediately before the first trade of the day (previous day's
#            closing balance); used to reconstruct the day's intraday balance curve for drawdown.
compute_trade_stats <- function(resultsDT, starting_balance = 0){
  # Only gross_profit/gross_loss/total_net_profit/total_trades/profit_trades*/loss_trades* are
  # read by Render.R's P/L and Trades cards (since the 2026-08-10 card redesign). Everything else
  # below is commented out, not deleted -- ready to switch back on if the old stats layout returns.
  empty <- list(
    gross_profit = 0, gross_loss = 0, total_net_profit = 0,
    # profit_factor = NA_real_, expected_payoff = NA_real_,
    total_trades = 0L,
    # short_trades = 0L, short_won_pct = NA_real_,
    # long_trades = 0L, long_won_pct = NA_real_,
    profit_trades = 0L, profit_trades_pct = NA_real_,
    loss_trades = 0L, loss_trades_pct = NA_real_
    # largest_profit_trade = 0, largest_loss_trade = 0,
    # average_profit_trade = 0, average_loss_trade = 0,
    # max_consec_wins_count = 0L, max_consec_wins_amount = 0,
    # max_consec_losses_count = 0L, max_consec_losses_amount = 0,
    # maximal_consec_profit_amount = 0, maximal_consec_profit_count = 0L,
    # maximal_consec_loss_amount = 0, maximal_consec_loss_count = 0L,
    # average_consec_wins = 0, average_consec_losses = 0,
    # balance_dd_absolute = 0, balance_dd_maximal = 0, balance_dd_maximal_pct = 0,
    # balance_dd_relative_pct = 0, balance_dd_relative_amount = 0,
    # recovery_factor = NA_real_, sharpe_ratio = NA_real_
  )
  if (is.null(resultsDT) || nrow(resultsDT) == 0) return(empty)

  resultsDT <- resultsDT[order(Time)]
  res <- resultsDT$Result
  res[is.na(res)] <- 0
  n <- length(res)

  gross_profit <- sum(res[res > 0])
  gross_loss   <- sum(res[res < 0])
  total_net_profit <- sum(res)
  # profit_factor <- if (gross_loss != 0) gross_profit / abs(gross_loss) else NA_real_
  # expected_payoff <- total_net_profit / n

  profit_trades <- sum(res > 0)
  loss_trades   <- sum(res < 0)

  # side <- if ("Side" %in% names(resultsDT)) resultsDT$Side else rep(NA_integer_, n)
  # short_trades <- sum(side == 1, na.rm = TRUE) # 1 = Sell
  # long_trades  <- sum(side == 0, na.rm = TRUE) # 0 = Buy
  # short_won_pct <- if (short_trades > 0) 100 * sum(res > 0 & side == 1, na.rm = TRUE) / short_trades else NA_real_
  # long_won_pct  <- if (long_trades  > 0) 100 * sum(res > 0 & side == 0, na.rm = TRUE) / long_trades  else NA_real_

  # largest_profit_trade <- if (profit_trades > 0) max(res[res > 0]) else 0
  # largest_loss_trade   <- if (loss_trades   > 0) min(res[res < 0]) else 0
  # average_profit_trade <- if (profit_trades > 0) mean(res[res > 0]) else 0
  # average_loss_trade   <- if (loss_trades   > 0) mean(res[res < 0]) else 0

  # Streak analysis: classify each trade win(1)/loss(-1)/flat(0); a flat (0 P/L) trade breaks the streak.
  # cls <- ifelse(res > 0, 1L, ifelse(res < 0, -1L, 0L))
  # streaks <- rle(cls)
  # streak_amounts <- numeric(length(streaks$lengths))
  # idx <- 1L
  # for (i in seq_along(streaks$lengths)){
  #   len <- streaks$lengths[i]
  #   streak_amounts[i] <- sum(res[idx:(idx + len - 1)])
  #   idx <- idx + len
  # }
  # win_mask  <- streaks$values == 1L
  # loss_mask <- streaks$values == -1L
  #
  # max_consec_wins_count    <- if (any(win_mask))  max(streaks$lengths[win_mask]) else 0L
  # max_consec_wins_amount   <- if (any(win_mask))  streak_amounts[win_mask][which.max(streaks$lengths[win_mask])] else 0
  # max_consec_losses_count  <- if (any(loss_mask)) max(streaks$lengths[loss_mask]) else 0L
  # max_consec_losses_amount <- if (any(loss_mask)) streak_amounts[loss_mask][which.max(streaks$lengths[loss_mask])] else 0
  #
  # maximal_consec_profit_amount <- if (any(win_mask))  max(streak_amounts[win_mask])  else 0
  # maximal_consec_profit_count  <- if (any(win_mask))  streaks$lengths[win_mask][which.max(streak_amounts[win_mask])] else 0L
  # maximal_consec_loss_amount   <- if (any(loss_mask)) min(streak_amounts[loss_mask]) else 0
  # maximal_consec_loss_count    <- if (any(loss_mask)) streaks$lengths[loss_mask][which.min(streak_amounts[loss_mask])] else 0L
  #
  # average_consec_wins   <- if (any(win_mask))  mean(streaks$lengths[win_mask])  else 0
  # average_consec_losses <- if (any(loss_mask)) mean(streaks$lengths[loss_mask]) else 0

  # Intraday balance curve reconstructed from starting_balance + cumulative trade results (trade order, not wall-clock ticks).
  # balance_curve <- starting_balance + cumsum(res)
  # running_peak <- cummax(c(starting_balance, balance_curve))[-1]
  # dd_series_abs <- pmax(running_peak - balance_curve, 0)
  # dd_series_pct <- ifelse(running_peak > 0, 100 * dd_series_abs / running_peak, 0)
  #
  # balance_dd_absolute    <- max(0, starting_balance - min(c(starting_balance, balance_curve)))
  # balance_dd_maximal     <- max(dd_series_abs)
  # balance_dd_maximal_pct <- max(dd_series_pct)
  # # Largest % drawdown, and its corresponding $ amount (may differ from the largest $ drawdown's day)
  # worst_pct_idx <- which.max(dd_series_pct)
  # balance_dd_relative_pct    <- dd_series_pct[worst_pct_idx]
  # balance_dd_relative_amount <- dd_series_abs[worst_pct_idx]
  #
  # recovery_factor <- if (balance_dd_maximal > 0) total_net_profit / balance_dd_maximal else NA_real_
  # sharpe_ratio <- if (n > 1 && sd(res) > 0) mean(res) / sd(res) else NA_real_

  list(
    gross_profit = gross_profit, gross_loss = gross_loss, total_net_profit = total_net_profit,
    total_trades = n,
    profit_trades = profit_trades, profit_trades_pct = 100 * profit_trades / n,
    loss_trades = loss_trades, loss_trades_pct = 100 * loss_trades / n
    # profit_factor = profit_factor, expected_payoff = expected_payoff,
    # short_trades = short_trades, short_won_pct = short_won_pct,
    # long_trades = long_trades, long_won_pct = long_won_pct,
    # largest_profit_trade = largest_profit_trade, largest_loss_trade = largest_loss_trade,
    # average_profit_trade = average_profit_trade, average_loss_trade = average_loss_trade,
    # max_consec_wins_count = max_consec_wins_count, max_consec_wins_amount = max_consec_wins_amount,
    # max_consec_losses_count = max_consec_losses_count, max_consec_losses_amount = max_consec_losses_amount,
    # maximal_consec_profit_amount = maximal_consec_profit_amount, maximal_consec_profit_count = maximal_consec_profit_count,
    # maximal_consec_loss_amount = maximal_consec_loss_amount, maximal_consec_loss_count = maximal_consec_loss_count,
    # average_consec_wins = average_consec_wins, average_consec_losses = average_consec_losses,
    # balance_dd_absolute = balance_dd_absolute, balance_dd_maximal = balance_dd_maximal, balance_dd_maximal_pct = balance_dd_maximal_pct,
    # balance_dd_relative_pct = balance_dd_relative_pct, balance_dd_relative_amount = balance_dd_relative_amount,
    # recovery_factor = recovery_factor, sharpe_ratio = sharpe_ratio
  )
}

# Balance curve points for the inline SVG sparkline: starting balance + running total after each trade, in time order.
balance_curve_points <- function(resultsDT, starting_balance = 0){
  if (is.null(resultsDT) || nrow(resultsDT) == 0) return(starting_balance)
  resultsDT <- resultsDT[order(Time)]
  c(starting_balance, starting_balance + cumsum(resultsDT$Result))
}
