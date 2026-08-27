library(data.table)

# --- Formatting helpers -----------------------------------------------------

html_escape <- function(x){
  x <- ifelse(is.na(x), "", as.character(x))
  x <- gsub("&", "&amp;", x, fixed = TRUE)
  x <- gsub("<", "&lt;",  x, fixed = TRUE)
  x <- gsub(">", "&gt;",  x, fixed = TRUE)
  x
}

# "1 272.56" / "-9 574.11" style (space thousands separator, 2 decimals)
fmt_money <- function(x){
  x <- ifelse(is.na(x), 0, x)
  format(round(x, 2), big.mark = " ", nsmall = 2, scientific = FALSE, trim = TRUE)
}

fmt_money_or_blank <- function(x){
  ifelse(is.na(x), "", fmt_money(x))
}

# "20 000.00 / -5 000.00" -- deposits and withdrawals shown side by side instead of netted into
# one figure, for the Summary card's "Deposit/Withdrawal" line only (the separate
# Deposit/Withdrawal table under Trades keeps its own net Total: row, untouched). withdrawals is
# already <= 0 (it's the sum of negative BalanceMovement), so its own sign carries through.
fmt_deposit_withdrawal <- function(deposits, withdrawals){
  sprintf("%s / %s", fmt_money(deposits), fmt_money(withdrawals))
}

fmt_pct <- function(x){
  x <- ifelse(is.na(x), 0, x)
  sprintf("%.2f%%", x)
}

fmt_ratio <- function(x) if (is.na(x)) "-" else sprintf("%.2f", x)

# price formatted to the symbol's precision (defaults to 5 when unknown)
fmt_price <- function(x, precision = 5){
  precision <- ifelse(is.na(precision), 5, precision)
  x <- ifelse(is.na(x), 0, x)
  sprintf(paste0("%.", precision, "f"), x)
}

# same, but NA stays blank instead of showing 0 -- for fields that are genuinely absent
# (e.g. close price on a trade that hasn't closed), as opposed to S/L or T/P where 0 means "not set"
fmt_price_or_blank <- function(x, precision = 5){
  ifelse(is.na(x), "", fmt_price(x, precision))
}

# id (OrderId/PosId, possibly integer64) -- NA renders blank, not the literal text "NA"
fmt_id <- function(x){
  ifelse(is.na(x), "", as.character(x))
}

fmt_dt <- function(x){
  if (is.null(x) || length(x) == 0) return("")
  ifelse(is.na(x), "", format(x, "%Y.%m.%d %H:%M:%S", tz = "UTC"))
}

# One stat card (Summary and Details both use this): gray header + label/value rows.
# rows: list of c(label, value) pairs. When bold_last is TRUE, the last row is bolded (a real
# sum/total of the rows above it) -- pass FALSE when the rows are independent figures with no
# meaningful sum (e.g. Margin/Free Margin).
detail_card <- function(title, rows, bold_last = TRUE, width = "20%"){
  n <- length(rows)
  body_rows <- paste(vapply(seq_len(n), function(i){
    label <- rows[[i]][1]; value <- rows[[i]][2]
    if (bold_last && i == n) sprintf('<tr><td><b>%s</b></td><td class="num"><b>%s</b></td></tr>', label, value)
    else sprintf('<tr><td>%s</td><td class="num">%s</td></tr>', label, value)
  }, character(1)), collapse = "")
  sprintf(paste0('<td valign="top" width="%s"><table cellspacing="0" cellpadding="4" border="0" width="100%%" class="card">',
                  '<tr class="stripe"><td colspan="2"><b>%s</b></td></tr>%s</table></td>'),
          width, title, body_rows)
}

# One row-table for Deposit/Withdrawal, Dividends, or Overnight (each its own section under
# Trades, split out of trade_rows on request 2026-08-17). Wrapped in a single
# <td colspan="trade_cols"> around an inner width="100%" table -- same technique
# summary_block_cards/detail_card use -- so its own column widths never affect the outer
# master table's grid (that's what caused the Net/Gross alignment bugs elsewhere in this file).
# show_taxes: Taxes (TradeReports.Taxes) is only ever nonzero for TrReason=11 (Dividends) --
# verified across all 4 domains, 180 days of data, 2026-08-17 -- so Deposit/Withdrawal and
# Overnight are called with show_taxes = FALSE to drop a column that would always read 0.00.
# show_comment: UserComment is always blank for Dividends/Overnight (TrReason 11/13, verified
# across all 4 domains, 365 days, 0/1345 and 0/1105 non-blank -- 2026-08-17); Deposit/Withdrawal
# keeps it since ad-hoc balance adjustments (e.g. TrReason=5 DealerDecision) do carry real notes.
# show_commission: same story -- Commission is always 0 for TrReason=5 (Deposit/Withdrawal, 0
# nonzero across 13336/240/74/4 rows on the 4 domains) and TrReason=13 (Overnight, 0/1105) --
# verified 2026-08-17; only Dividends (TrReason=11) has real nonzero Commission (227/1345,
# 25/400), so it's the only one still called with show_commission = TRUE.
# show_symbol: Symbol is always blank for TrReason=5/13 (0/13336, 0/240, 0/74, 0/4 and 0/1105 --
# verified 2026-08-17) and always populated for Dividends (1345/1345, 400/400, it's the stock
# that paid), so only Dividends is called with show_symbol = TRUE.
cash_table_block <- function(title, rows, trade_cols, show_taxes = TRUE, show_comment = TRUE,
                              show_commission = TRUE, show_symbol = TRUE, date_width = NULL, total = NULL){
  # Amount is always the last column, and always the same fixed width -- so it lines up
  # vertically across Deposit/Withdrawal, Dividends and Overnight even though the tables have
  # different column counts (each is its own independent width="100%" table). date_width is the
  # same idea applied to Date, but only requested between Dividends and Deposit/Withdrawal
  # (2026-08-17) -- NULL (Overnight's default) leaves that column auto-width, as before.
  amount_width <- "90"
  cols <- c("Date", if (show_symbol) "Symbol", if (show_commission) "Commission", if (show_taxes) "Taxes",
            if (show_comment) "Comment", "Amount")
  ncols <- length(cols)
  date_td <- if (is.null(date_width)) '<td>Date</td>' else sprintf('<td width="%s">Date</td>', date_width)
  header <- paste0('<tr align="center" class="colhead">',
                    date_td,
                    paste(sprintf('<td>%s</td>', head(cols, -1)[-1]), collapse = ""),
                    sprintf('<td width="%s">Amount</td>', amount_width), '</tr>\n')
  body_rows <- if (is.null(rows) || nrow(rows) == 0){
    sprintf('<tr class="empty-row" align="center"><td colspan="%d">No transactions</td></tr>', ncols)
  } else {
    paste(vapply(seq_len(nrow(rows)), function(i){
      r <- rows[i]
      row_class <- if (i %% 2L == 1L) "rowA" else "rowB"
      date_cell <- if (is.null(date_width)) sprintf('<td align="left" nowrap>%s</td>', fmt_dt(r$OpenTime)) else sprintf('<td align="left" nowrap width="%s">%s</td>', date_width, fmt_dt(r$OpenTime))
      cells <- c(date_cell,
                 if (show_symbol) sprintf('<td align="left">%s</td>', html_escape(r$Item)),
                 if (show_commission) sprintf('<td>%s</td>', fmt_money_or_blank(r$Commission)),
                 if (show_taxes) sprintf('<td>%s</td>', fmt_money_or_blank(r$Taxes)),
                 if (show_comment) sprintf('<td align="left">%s</td>', html_escape(r$Comment)),
                 sprintf('<td width="%s">%s</td>', amount_width, fmt_money(r$Amount)))
      sprintf('<tr align="right" class="%s">%s</tr>', row_class, paste(cells, collapse = ""))
    }, character(1)), collapse = "\n")
  }
  # Same footer pattern as Trades' "Closed Total P/L:" / Open Positions' "Floating P/L:" --
  # bold label spanning every column but the last, bold sum under Amount.
  total_row <- if (is.null(total)) '' else sprintf(
    '<tr align="right" class="total-row"><td colspan="%d" align="right"><b>Total:</b></td><td width="%s"><b>%s</b></td></tr>\n',
    ncols - 1L, amount_width, fmt_money(total))
  # Title bar lives INSIDE the same card table as its first row (not a separate outer-table row)
  # so the whole card -- title, column header, data, total -- reads as one seamless rounded box,
  # matching the reference PDF (rounded top on the title bar, rounded bottom on the last row).
  paste0(
    # style="padding:0" on the wrapping <td> overrides the outer master table's own
    # cellpadding="3" for just this cell -- otherwise the nested .card table (and its border/
    # radius) sits inset 3px further in than a bare <td> border would, found 2026-08-17 when
    # cards looked misaligned/wider against what were then unwrapped rows; every section uses
    # this same wrapping now so it just needs to stay consistent across all of them.
    '<tr><td colspan="', trade_cols, '" style="padding:0"><table cellspacing="0" cellpadding="4" border="0" width="100%" class="card vgrid">',
    '<tr align="left" class="stripe"><td colspan="', ncols, '"><b>', title, ':</b></td></tr>\n',
    header,
    body_rows, '\n',
    total_row,
    '</table></td></tr>\n',
    '<tr><td colspan="', trade_cols, '" style="font: 1pt arial">&nbsp;</td></tr>\n'
  )
}

# Trades grouped by Symbol -- Commission/Swap/Profit summed per symbol, with a bold Total: row.
# Added 2026-08-17. Groups exactly trade_rows (what's actually shown in Trades/Closed Trades
# above), same source the "Closed Total P/L:" footer sums over -- so this section's own Total
# row reconciles with that footer (Commission+Swap+Profit, no Taxes -- always 0 for real trades).
# show_swap = FALSE for Overnight-provisioned accounts -- per-trade Swap is always 0 for them
# (their swap-equivalent is the separate Overnight balance rows, not this column), so the column
# is dropped rather than shown as an always-zero line, same treatment as Leverage in the header.
# Swap still enters the Total P/L math either way -- only the column's DISPLAY is conditional, so
# the number stays correct even for the one grandfathered account with both (see [[project_tt_statements]]).
symbol_summary_block <- function(trade_rows, trade_cols, show_swap = TRUE){
  grouped <- if (is.null(trade_rows) || nrow(trade_rows) == 0){
    data.table(Item = character(0), Commission = numeric(0), Swap = numeric(0), Profit = numeric(0))
  } else {
    g <- trade_rows[, .(Commission = sum(Commission, na.rm = TRUE),
                         Swap = sum(Swap, na.rm = TRUE),
                         Profit = sum(Profit, na.rm = TRUE)), by = Item]
    # Sorted by Total P/L descending (most profitable symbol first), not alphabetically by
    # Item -- requested 2026-08-17.
    g[, TotalPL := Commission + Swap + Profit]
    setorder(g, -TotalPL)
    g
  }
  ncols <- if (show_swap) 5L else 4L
  swap_th <- if (show_swap) '<td>Swap</td>' else ''
  header <- sprintf('<tr align="center" class="colhead big-label"><td>Symbol</td><td>Commission</td>%s<td>Profit</td><td>Total P/L</td></tr>\n', swap_th)
  body_rows <- if (nrow(grouped) == 0){
    # "No data", not "No transactions" -- this table is aggregated Commission/Swap/Profit per
    # symbol, not a list of individual transactions, so the empty-state wording should match.
    sprintf('<tr class="empty-row" align="center"><td colspan="%d">No data</td></tr>', ncols)
  } else {
    paste(vapply(seq_len(nrow(grouped)), function(i){
      r <- grouped[i]
      row_class <- if (i %% 2L == 1L) "rowA" else "rowB"
      swap_td <- if (show_swap) sprintf('<td>%s</td>', fmt_money(r$Swap)) else ''
      sprintf('<tr align="right" class="%s"><td align="left"><b>%s</b></td><td>%s</td>%s<td>%s</td><td>%s</td></tr>',
              row_class, html_escape(r$Item), fmt_money(r$Commission), swap_td, fmt_money(r$Profit),
              fmt_money(r$Commission + r$Swap + r$Profit))
    }, character(1)), collapse = "\n")
  }
  swap_total_td <- if (show_swap) sprintf('<td><b>%s</b></td>', fmt_money(sum(grouped$Swap))) else ''
  total_row <- sprintf(
    '<tr align="right" class="total-row"><td align="left"><b>Total:</b></td><td><b>%s</b></td>%s<td><b>%s</b></td><td><b>%s</b></td></tr>\n',
    fmt_money(sum(grouped$Commission)), swap_total_td, fmt_money(sum(grouped$Profit)),
    fmt_money(sum(grouped$Commission) + sum(grouped$Swap) + sum(grouped$Profit)))
  # Title bar inside the same card table as its first row (see cash_table_block for why).
  paste0(
    # style="padding:0" on the wrapping <td> overrides the outer master table's own
    # cellpadding="3" for just this cell -- otherwise the nested .card table (and its border/
    # radius) sits inset 3px further in than a bare <td> border would, found 2026-08-17 when
    # cards looked misaligned/wider against what were then unwrapped rows; every section uses
    # this same wrapping now so it just needs to stay consistent across all of them.
    '<tr><td colspan="', trade_cols, '" style="padding:0"><table cellspacing="0" cellpadding="4" border="0" width="100%" class="card vgrid">',
    '<tr align="left" class="stripe"><td colspan="', ncols, '"><b>Trade Symbol Summary:</b></td></tr>\n',
    header, body_rows, '\n', total_row,
    '</table></td></tr>\n',
    # Same spacer as every other section (2026-08-17 -- was a bigger one-off 20px gap, unified
    # for equal spacing between all sections on request).
    '<tr><td colspan="', trade_cols, '" style="font: 1pt arial">&nbsp;</td></tr>\n'
  )
}

side_label <- function(side) ifelse(side == 0, "buy", ifelse(side == 1, "sell", "-"))

# Best-effort pending-order type label (Market=0/Limit=1/Stop=2/StopLimit=3 -- common MT convention)
order_type_label <- function(side, type){
  base <- side_label(side)
  suffix <- c("0" = "", "1" = " limit", "2" = " stop", "3" = " stop limit")[as.character(type)]
  paste0(base, ifelse(is.na(suffix), "", suffix))
}

order_state_label <- function(state){
  # Placed=0/Filled=1/Cancelled=2/Rejected=3/Expired=4 -- best-effort, matches DailyOrderSnapshots.State
  labs <- c("0" = "placed", "1" = "filled", "2" = "cancelled", "3" = "rejected", "4" = "expired")
  out <- labs[as.character(state)]
  ifelse(is.na(out), "placed", out)
}

deal_direction_label <- function(trTime, posOpened){
  ifelse(is.na(posOpened), "", ifelse(trTime == posOpened, "in", "out"))
}

# Inline SVG sparkline of the day's balance curve (replaces the MT4/5 <img ... .gif> graph).
# x_start_label/x_end_label label the time axis (the curve always spans exactly one UTC day).
# NOT called anywhere as of 2026-08-10 -- graph removed from the statement, kept here for
# reference in case it's wanted back later.
svg_sparkline <- function(points, width = 820, height = 140, x_start_label = "00:00", x_end_label = "24:00"){
  points <- as.numeric(points)
  n <- length(points)
  if (n < 2) points <- c(points, points)
  n <- length(points)
  ymin <- min(points); ymax <- max(points)
  yrange <- if (ymax > ymin) ymax - ymin else 1

  left_pad <- 90; right_pad <- 10; top_pad <- 14; bottom_pad <- 24
  plot_w <- width - left_pad - right_pad
  plot_h <- height - top_pad - bottom_pad

  xs <- seq(left_pad, left_pad + plot_w, length.out = n)
  ys <- top_pad + plot_h - (points - ymin) / yrange * plot_h
  path <- paste(sprintf("%.1f,%.1f", xs, ys), collapse = " ")
  color <- "#1a3fa0" # always blue, matching the MT4/5 balance graph (not conditional on direction)

  y_of <- function(v) top_pad + plot_h - (v - ymin) / yrange * plot_h
  gridline <- function(v) sprintf(paste0(
    '<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="#ddd" stroke-dasharray="2,2"/>',
    '<text x="%d" y="%.1f" font-size="10" font-family="Tahoma,Arial" text-anchor="end" fill="#555">%s</text>'),
    left_pad, y_of(v), left_pad + plot_w, y_of(v), left_pad - 6, y_of(v) + 3, fmt_money(v))

  grid_levels <- seq(ymin, ymax, length.out = 6)
  gridlines <- paste(vapply(grid_levels, gridline, character(1)), collapse = "")

  sprintf(paste0(
    '<svg width="%d" height="%d" viewBox="0 0 %d %d" xmlns="http://www.w3.org/2000/svg">',
    '<rect x="0" y="0" width="%d" height="%d" fill="#fafafa" stroke="#ddd"/>',
    '%s',
    '<polyline fill="none" stroke="%s" stroke-width="1.5" points="%s"/>',
    '<text x="%d" y="%d" font-size="10" font-family="Tahoma,Arial" text-anchor="start" fill="#555">%s</text>',
    '<text x="%d" y="%d" font-size="10" font-family="Tahoma,Arial" text-anchor="end" fill="#555">%s</text>',
    '</svg>'),
    width, height, width, height, width, height,
    gridlines,
    color, path,
    left_pad, height - 6, html_escape(x_start_label),
    left_pad + plot_w, height - 6, html_escape(x_end_label))
}

page_head <- function(title){
  paste0(
    '<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">\n',
    '<html><head><title>', html_escape(title), '</title>\n',
    '<style type="text/css">\n',
    'td,th { font: 8pt "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; }\n',
    '.num { text-align:right; }\n',
    # Section/card title bar -- gray, small rounded top corners (matching the reference
    # PDF's subtle radius, not a pill shape -- corrected from 10px to 4px 2026-08-17). #999999
    # is deliberately darker than .colhead's #D0D0D0 below (table column headers) so the two
    # grays stay visually distinct instead of blending together.
    # color:#222222 (not white) -- white-on-#999999 has poor contrast; #222222 matches .colhead's
    # own text color below, which uses the same light-gray-background/dark-text pairing.
    '.stripe { background:#999999; color:#222222; border-radius:4px 4px 0 0; font-size:16px; }\n',
    # .stripe is applied to the <tr>, but td,th{font:8pt...} above is a DIRECT match on every
    # <td> (including the ones inside a .stripe row) -- a direct match always wins over
    # inheritance from the parent <tr>, so the 16px on .stripe alone never actually reached the
    # visible text. Found 2026-08-17 (the size looked unchanged despite .stripe itself computing
    # to 16px) -- .stripe td re-asserts the size directly on the cell that holds the text.
    '.stripe td { font-size:16px; }\n',
    # Column header row directly under a title bar -- lighter tint, dark text, no rounding
    # (flows straight from the stripe above it).
    '.colhead { background:#D0D0D0; color:#222222; font-weight:bold; }\n',
    # Data rows: no fill color (removed 2026-08-17 on request) -- a plain light gray grid line
    # under every cell instead, matching the reference PDF's row separators.
    '.card td { border-bottom:1px solid #E3E3E3; }\n',
    '.card tr:last-child td { border-bottom:none; }\n',
    # "No transactions" placeholder rows -- plain, no highlight (removed 2026-08-17).
    '.empty-row { color:#888888; font-style:italic; }\n',
    # Bigger label text for Account Summary's row labels and Trade Symbol Summary's column
    # headers specifically (2026-08-17 request) -- NOT applied to .colhead generally, since that
    # class is shared with Trades/Open Positions/Orders/cash tables, which should stay as-is.
    '.big-label { font-size:10pt; }\n',
    '.card { border:1px solid #E3E3E3; border-radius:4px; overflow:hidden; }\n',
    # Vertical column-dividing lines (2026-08-17) -- opt-in via .vgrid, used on Trades/Open
    # Positions/Orders now that they're independent .card tables (see render_statement) rather
    # than shared-grid rows, so a vertical rule between every column reads clearly even without
    # cross-table pixel alignment.
    # Total/footer rows (2026-08-17) stay plain -- no vertical dividers -- so they read as one
    # summary line, not another row of the grid.
    '.vgrid tr:not(.total-row) td { border-right:1px solid #E3E3E3; }\n',
    '.vgrid td:last-child { border-right:none; }\n',
    '</style>\n',
    '</head><body topmargin="1" marginheight="1"><div align="center">\n')
}

page_tail <- '</div></body></html>'

# Fields stacked one per line (Statement Date / Generated At / Account Name / ... style),
# matching the reference PDF layout instead of the old single-row-of-columns layout.
# show_leverage = FALSE for investment accounts -- their Leverage is always 1 by definition (part
# of how they're identified, see is_investment_account in task_TT_Statements.R) but isn't
# meaningful for this account type and shouldn't be shown as "1:1".
account_header_block <- function(login, name, currency, leverage, acc_type, day_label, trade_cols, period_type, show_leverage = TRUE){
  field_row <- function(label, value){
    sprintf('<tr align="left"><td colspan="%d"><b>%s:</b> %s</td></tr>\n', trade_cols, label, value)
  }
  paste0(
    '<div style="font: 20pt \'Segoe UI\', Roboto, \'Helvetica Neue\', Arial, sans-serif; color:#999999"><b>Account Statement</b></div><br>\n',
    '<table cellspacing="1" cellpadding="3" border="0" style="width:60%; max-width:60%; margin:0 auto;">\n',
    field_row(paste0(period_type, " Statement for"), day_label),
    field_row("Account", login),
    field_row("Name", html_escape(name)),
    field_row("Currency", currency),
    if (show_leverage) field_row("Leverage", paste0("1:", leverage)) else '',
    field_row("Account Type", acc_type))
}

# --- Universal daily statement (both Gross and Net accounts) ---------------
# trade_table_label: "Closed Trades" for Gross (TrType=4, one row per fully closed position)
#                     "Trades" for Net (TrType=3, one row per fill -- opening or reducing exposure)

render_statement <- function(login, name, currency, leverage, day_label, acc_type,
                              trade_table_label, trade_rows, cash_rows, open_rows, order_rows, summary, stats,
                              period_type = "Daily", summary_layout = "cards"){

  # Net has no reliable PosOpened (confirmed empty on real data), so it gets a single "Time"
  # column (TrTime) instead of separate Open/Close Time -- one column narrower than Gross.
  # Each statement file covers exactly one account, so the two shapes never mix in one table.
  # Net has no reliable PosOpened (confirmed empty on real data), so it gets a single "Time"
  # column (TrTime) instead of separate Open/Close Time -- one column narrower than Gross.
  # Each statement file covers exactly one account, so the two shapes never mix in one table.
  is_net <- identical(acc_type, "Net")
  # S/L and T/P are always 0.00 for Net (Trades/Open Positions/Orders alike -- verified
  # 2026-08-17 across all 4 domains, 0 nonzero out of thousands of rows) but real and nonzero
  # for Gross, so they're dropped from Net's column set only. trade_cols is Trades' own natural
  # column count per account type -- Net = 12 (was 14, -2 for S/L/T/P), Gross = 15 (unchanged) --
  # kept only as the outer master-grid wrap colspan value now (see below): Trades/Open
  # Positions/Orders are each their own independent card table as of 2026-08-17 (no more
  # cross-section column alignment between them -- that machinery caused repeated Net/Gross
  # alignment bugs in this file's history, and dropping it doesn't cost anything: the report's
  # width was never actually driven by that alignment, verified against the pre-change version).
  trade_cols <- if (is_net) 12L else 15L

  closed_html <- if (is.null(trade_rows) || nrow(trade_rows) == 0){
    '<tr class="empty-row" align="right"><td colspan="12" align="center">No transactions</td></tr>'
  } else {
    paste(vapply(seq_len(nrow(trade_rows)), function(i){
      r <- trade_rows[i]
      if (is_net) {
        sprintf(paste0('<tr align="right"><td>%s</td><td>%s</td><td nowrap>%s</td><td>%s</td><td>%s</td><td>%s</td>',
                        '<td>%s</td><td>%s</td>',
                        '<td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>'),
                fmt_id(r$OrderId), fmt_id(r$PosId), fmt_dt(r$OpenTime), side_label(r$Side),
                fmt_money(r$VolumeLot), html_escape(r$Item),
                fmt_price(r$OpenPrice, r$Precision),
                fmt_price_or_blank(r$ClosePrice, r$Precision),
                fmt_money(r$Commission), fmt_money(r$Swap), fmt_money(r$Profit),
                html_escape(r$Comment))
      } else {
        sprintf(paste0('<tr align="right"><td>%s</td><td>%s</td><td nowrap>%s</td><td>%s</td><td>%s</td><td>%s</td>',
                        '<td>%s</td><td>%s</td><td>%s</td><td nowrap>%s</td><td>%s</td>',
                        '<td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>'),
                fmt_id(r$OrderId), fmt_id(r$PosId), fmt_dt(r$OpenTime), side_label(r$Side),
                fmt_money(r$VolumeLot), html_escape(r$Item),
                fmt_price(r$OpenPrice, r$Precision), fmt_price(r$Sl, r$Precision), fmt_price(r$Tp, r$Precision),
                fmt_dt(r$CloseTime), fmt_price_or_blank(r$ClosePrice, r$Precision),
                fmt_money(r$Commission), fmt_money(r$Swap), fmt_money(r$Profit),
                html_escape(r$Comment))
      }
    }, character(1)), collapse = "\n")
  }

  trade_header <- if (is_net) {
    paste0('<tr align="center" class="colhead"><td>Order ID</td><td>Position ID</td><td nowrap>Trade Time</td><td>Type</td><td>Volume</td><td>Symbol</td>',
           '<td>Open Price</td><td>Close Price</td>',
           '<td>Commission</td><td>Swap</td><td>Profit</td><td>Comment</td></tr>\n')
  } else {
    paste0('<tr align="center" class="colhead"><td>Order ID</td><td>Position ID</td><td nowrap>Open Time</td><td>Type</td><td>Volume</td><td>Symbol</td>',
           '<td>Open Price</td><td>S / L</td><td>T / P</td><td nowrap>Close Time</td><td>Close Price</td>',
           '<td>Commission</td><td>Swap</td><td>Profit</td><td>Comment</td></tr>\n')
  }
  trade_total_row <- sprintf(
    '<tr align="right" class="total-row"><td colspan="%d" align="right"><b>Total P/L:</b></td><td colspan="2" align="right"><b>%s</b></td></tr>\n',
    trade_cols - 2L, fmt_money(summary$closed_pl))

  # Open Positions -- own card, own natural column count (no more cross-alignment with Trades,
  # so the old colspan=2 ID/Profit merges and the Commission/S-L/T-P blank fillers are gone;
  # Commission (DailyPositionSnapshots.Commission -- a different field from Trades' own
  # Commission, which stays real for Net) is always 0.00 for Net -- verified 2026-08-17,
  # 0/10388+156+240 nonzero across 3 domains -- so it's dropped entirely for Net, kept for Gross
  # (97%/87% nonzero there). S/L/T/P: same story as Trades, Net always 0, Gross real.
  open_ncols <- if (is_net) 9L else 12L
  open_positions_header <- if (is_net) {
    paste0('<tr align="center" class="colhead"><td>ID</td><td nowrap>Open Time</td><td>Type</td><td>Volume</td><td>Symbol</td>',
           '<td>Price</td><td nowrap>Current Price</td><td>Swap</td><td>Profit</td></tr>\n')
  } else {
    paste0('<tr align="center" class="colhead"><td>ID</td><td nowrap>Open Time</td><td>Type</td><td>Volume</td><td>Symbol</td>',
           '<td>Price</td><td>S / L</td><td>T / P</td><td nowrap>Current Price</td>',
           '<td>Commission</td><td>Swap</td><td>Profit</td></tr>\n')
  }

  open_html <- if (is.null(open_rows) || nrow(open_rows) == 0){
    sprintf('<tr class="empty-row" align="right"><td colspan="%d" align="center">No transactions</td></tr>', open_ncols)
  } else {
    paste(vapply(seq_len(nrow(open_rows)), function(i){
      r <- open_rows[i]
      if (is_net) {
        sprintf(paste0('<tr align="right"><td>%s</td><td nowrap>%s</td><td>%s</td><td>%s</td><td>%s</td>',
                        '<td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>'),
                r$Ticket, fmt_dt(r$OpenTime), side_label(r$Side),
                fmt_money(r$VolumeLot), html_escape(r$Item),
                fmt_price(r$OpenPrice, r$Precision),
                fmt_price(r$MarketPrice, r$Precision),
                fmt_money(r$Swap), fmt_money(r$Profit))
      } else {
        sprintf(paste0('<tr align="right"><td>%s</td><td nowrap>%s</td><td>%s</td><td>%s</td><td>%s</td>',
                        '<td>%s</td><td>%s</td><td>%s</td><td>%s</td>',
                        '<td>%s</td><td>%s</td><td>%s</td></tr>'),
                r$Ticket, fmt_dt(r$OpenTime), side_label(r$Side),
                fmt_money(r$VolumeLot), html_escape(r$Item),
                fmt_price(r$OpenPrice, r$Precision), fmt_price(r$Sl, r$Precision), fmt_price(r$Tp, r$Precision),
                fmt_price(r$MarketPrice, r$Precision),
                fmt_money(r$Commission), fmt_money(r$Swap), fmt_money(r$Profit))
      }
    }, character(1)), collapse = "\n")
  }
  open_total_row <- sprintf(
    '<tr align="right" class="total-row"><td colspan="%d" align="right"><b>Floating P/L:</b></td><td colspan="2" align="right"><b>%s</b></td></tr>\n',
    open_ncols - 2L, fmt_money(summary$floating_pl))

  # Orders -- own card, own natural column count. State column removed entirely (verified
  # 2026-08-17: always "filled" across all 4 domains, 30 days, 2308/2308 rows). S/L/T/P: Net
  # drops them (always 0), Gross keeps them (real). No footer row after Orders.
  order_ncols <- if (is_net) 7L else 9L
  order_header <- if (is_net) {
    paste0('<tr align="center" class="colhead"><td>ID</td><td nowrap>Open Time</td><td>Type</td><td>Volume</td><td>Symbol</td>',
           '<td>Price</td><td align="left">Comment</td></tr>\n')
  } else {
    paste0('<tr align="center" class="colhead"><td>ID</td><td nowrap>Open Time</td><td>Type</td><td>Volume</td><td>Symbol</td>',
           '<td>Price</td><td>S / L</td><td>T / P</td><td align="left">Comment</td></tr>\n')
  }
  order_html <- if (is.null(order_rows) || nrow(order_rows) == 0){
    sprintf('<tr class="empty-row" align="right"><td colspan="%d" align="center">No transactions</td></tr>', order_ncols)
  } else {
    paste(vapply(seq_len(nrow(order_rows)), function(i){
      r <- order_rows[i]
      if (is_net) {
        sprintf(paste0('<tr align="right"><td>%s</td><td nowrap>%s</td><td>%s</td><td>%s</td><td>%s</td>',
                        '<td>%s</td><td align="left">%s</td></tr>'),
                r$OrderId, fmt_dt(r$Created), order_type_label(r$Side, r$Type),
                fmt_money(r$VolumeLot), html_escape(r$Item),
                fmt_price(r$Price, r$Precision), html_escape(r$Comment))
      } else {
        sprintf(paste0('<tr align="right"><td>%s</td><td nowrap>%s</td><td>%s</td><td>%s</td><td>%s</td>',
                        '<td>%s</td><td>%s</td><td>%s</td><td align="left">%s</td></tr>'),
                r$OrderId, fmt_dt(r$Created), order_type_label(r$Side, r$Type),
                fmt_money(r$VolumeLot), html_escape(r$Item),
                fmt_price(r$Price, r$Precision), fmt_price(r$Sl, r$Precision), fmt_price(r$Tp, r$Precision),
                html_escape(r$Comment))
      }
    }, character(1)), collapse = "\n")
  }

  # Computed only when actually used (is_single_column discards this entirely, and single_column
  # is the production default) -- was built unconditionally then thrown away every render before
  # 2026-08-18; wasted work on every one of ~25k accounts/day for no visual difference.
  is_single_column <- identical(summary_layout, "single_column")

  # Balance/Equity Total P/L both net out Deposit/Withdrawal (summary$balance_total_pl /
  # equity_total_pl = Closing - Opening - Deposit/Withdrawal) -- Balance's version is realized
  # P&L only; Equity's also captures the period's change in floating, since Equity already
  # embeds it. Floating (below) is the current point-in-time snapshot, not a period figure.
  summary_block_cards <- if (is_single_column) '' else paste0(
    '<tr align="left" class="stripe"><td colspan="', trade_cols, '"><b>Account Summary:</b></td></tr>\n',
    '<tr><td colspan="', trade_cols, '"><table cellspacing="8" cellpadding="0" border="0" width="100%"><tr valign="top">',
    detail_card("Open State", list(
      c("Equity", fmt_money_or_blank(summary$opening_equity)),
      c("Balance", fmt_money_or_blank(summary$opening_balance))),
      bold_last = FALSE, width = "25%"),
    detail_card("Cash Movement", Filter(Negate(is.null), list(
      c("Deposit/Withdrawal", fmt_deposit_withdrawal(summary$deposits, summary$withdrawals)),
      c("Commission", fmt_money(summary$total_commission)),
      # Investment accounts never show a "Swap" line here (their per-trade Swap isn't
      # meaningful) -- only "Overnight", and only when show_overnight says so; when an
      # investment account has neither the flag nor real data this period, the row is
      # dropped entirely rather than falling back to Swap. Non-investment accounts are
      # unaffected -- show_swap_or_overnight_row is always TRUE for them.
      if (summary$show_swap_or_overnight_row) c(summary$swap_or_overnight_label, fmt_money(summary$swap_or_overnight)) else NULL,
      c("Dividends", fmt_money(summary$dividends)),
      c("Trading P/L", fmt_money(summary$trading_pl)))),
      bold_last = FALSE, width = "25%"),
    detail_card("Close State", list(
      c("Equity", fmt_money(summary$equity)),
      c("Balance", fmt_money(summary$balance)),
      c("Margin", fmt_money(summary$margin)),
      c("Free Margin", fmt_money(summary$free_margin))),
      bold_last = FALSE, width = "25%"),
    detail_card("P/L", list(
      c("Equity Total P/L", fmt_money(summary$equity_total_pl)),
      c("Closed Total P/L", fmt_money(summary$balance_total_pl)),
      c("Floating", fmt_money(summary$floating_pl))),
      bold_last = FALSE, width = "25%"),
    '</tr></table></td></tr>\n',
    '<tr><td colspan="', trade_cols, '" style="font: 1pt arial">&nbsp;</td></tr>\n'
  )

  # Single-column variant (reference: user-edited summary.xlsx, one flat list instead of 4
  # side-by-side cards) -- kept alongside summary_block_cards, selected via summary_layout.
  # No inner detail_card() header (it would duplicate the "Summary:" stripe below) and the table
  # is narrow/unstretched so label and value stay close together instead of spanning full width.
  # 3rd element ("gap") marks a row that gets extra breathing room below it -- between the two
  # opening figures and between Close Balance/Unrealised P/L, by request.
  single_column_rows <- Filter(Negate(is.null), list(
    c("Open Equity", fmt_money_or_blank(summary$opening_equity), "gap"),
    c("Open Balance", fmt_money_or_blank(summary$opening_balance)),
    c("Realised P/L", fmt_money(summary$balance_total_pl)),
    c("Commission", fmt_money(summary$total_commission)),
    # See the "Cash Movement" card's equivalent line for why this can be dropped entirely
    # (investment account, Overnight not currently applicable) instead of falling back to Swap.
    if (summary$show_swap_or_overnight_row) c(summary$swap_or_overnight_label, fmt_money(summary$swap_or_overnight)) else NULL,
    c("Dividends", fmt_money(summary$dividends)),
    c("Deposit/Withdrawal", fmt_deposit_withdrawal(summary$deposits, summary$withdrawals)),
    c("Close Balance", fmt_money(summary$balance), "gap"),
    c("Unrealised P/L (Floating)", fmt_money(summary$floating_pl)),
    c("Used Margin", fmt_money(summary$margin)),
    c("Free Margin", fmt_money(summary$free_margin)),
    c("Close Equity", fmt_money(summary$equity))
  ))
  single_column_html <- paste(vapply(single_column_rows, function(r){
    row_html <- sprintf('<tr><td class="big-label"><b>%s</b></td><td class="num">%s</td></tr>', r[1], r[2])
    if (length(r) > 2 && r[3] == "gap") {
      row_html <- paste0(row_html, '<tr><td colspan="2" style="line-height:10px; font-size:1pt">&nbsp;</td></tr>')
    }
    row_html
  }, character(1)), collapse = "")

  # single_column mode: 4-column top row -- col 1 blank (10%), col 2 Account info (left-aligned,
  # 40%), col 3 Summary (40%), col 4 intentionally blank (10%). cards mode is untouched:
  # account_header_block() opens the page's one big table and summary_block_cards is just the
  # first section inside it, as before.
  if (is_single_column) {
    account_field_rows <- paste0(
      sprintf('<tr align="left"><td style="font-size:14pt"><b>%s:</b> %s</td></tr>\n', paste0(period_type, " Statement for"), day_label),
      sprintf('<tr align="left"><td style="font-size:14pt"><b>Account:</b> %s</td></tr>\n', login),
      sprintf('<tr align="left"><td style="font-size:14pt"><b>Name:</b> %s</td></tr>\n', html_escape(name)),
      sprintf('<tr align="left"><td style="font-size:14pt"><b>Currency:</b> %s</td></tr>\n', currency),
      # summary$show_leverage is the single source of truth (task_TT_Statements.R) -- account_header_block's
      # show_leverage param (the "cards" layout's equivalent, below) reads the same value.
      if (isTRUE(summary$show_leverage)) sprintf('<tr align="left"><td style="font-size:14pt"><b>Leverage:</b> 1:%s</td></tr>\n', leverage) else '',
      sprintf('<tr align="left"><td style="font-size:14pt"><b>Account Type:</b> %s</td></tr>\n', acc_type)
    )
    # Wrapped in a SINGLE <td colspan="trade_cols"> (same safe pattern summary_block_cards/
    # detail_card already use) so this row is opaque to the outer table's per-column width
    # computation -- doesn't affect the rest of the report's width at all. Same technique as
    # Details' 5 cards: explicit width="%" on EVERY cell of the inner width="100%" table, so the
    # browser has enough info to size all 3 columns proportionally instead of just shrink-wrapping
    # the ones without a width.
    header_block <- paste0(
      '<div style="font: 20pt \'Segoe UI\', Roboto, \'Helvetica Neue\', Arial, sans-serif; color:#999999"><b>Account Statement</b></div><br>\n',
      '<table cellspacing="1" cellpadding="3" border="0" style="width:60%; max-width:60%; margin:0 auto;"><tr><td colspan="', trade_cols, '">',
      '<table cellspacing="1" cellpadding="3" border="0" width="100%"><tr valign="top">',
      '<td width="10%" valign="top">&nbsp;</td>',
      '<td width="40%" valign="middle"><table cellspacing="1" cellpadding="3" border="0" style="font-size:14pt">', account_field_rows, '</table></td>',
      '<td width="40%" valign="top" style="padding-left:5px">',
      '<table cellspacing="0" cellpadding="4" border="0" width="100%" class="card"><tr class="stripe"><td colspan="2"><b>Account Summary</b></td></tr>',
      single_column_html,
      '</table></td>',
      '<td width="10%" valign="top">&nbsp;</td>',
      '</tr></table>',
      '</td></tr>\n',
      # Extra <br>-sized spacer row so the gap before "Trade Symbol Summary" matches the gap
      # the page title's own <br> creates above this block (requested 2026-08-18) -- the
      # ordinary inter-section spacer (font: 1pt arial td, used everywhere else) is much
      # thinner than a real <br>, so it can't be reused here.
      '<tr><td colspan="', trade_cols, '"><br></td></tr>\n'
    )
  } else {
    header_block <- account_header_block(login, name, currency, leverage, acc_type, day_label, trade_cols, period_type,
                                          show_leverage = isTRUE(summary$show_leverage))
  }

  # Details section removed from output 2026-08-17 on request -- code kept (not deleted) in case
  # it's wanted back later. Flip show_details to TRUE to restore it. Construction itself is now
  # gated by the same flag (was unconditional before 2026-08-18 -- wasted 5 detail_card() calls
  # on every render even though the result was always discarded, since show_details is FALSE).
  show_details <- FALSE
  details_block <- if (!show_details) '' else paste0(
    '<tr><td colspan="', trade_cols, '" style="font: 1pt arial">&nbsp;</td></tr>\n',
    '<tr align="left" class="stripe"><td colspan="', trade_cols, '"><b>Details:</b></td></tr>\n',
    '<tr><td colspan="', trade_cols, '"><table cellspacing="8" cellpadding="0" border="0" width="100%"><tr valign="top">',
    detail_card("Trading P/L", list(
      c("Profit", fmt_money(summary$trading_profit)),
      c("Loss", fmt_money(summary$trading_loss)),
      c("Total", fmt_money(summary$trading_pl)))),
    detail_card("Close Trades", list(
      c("Profit Trades", paste0(stats$profit_trades, " (", fmt_pct(stats$profit_trades_pct), ")")),
      c("Loss Trades", paste0(stats$loss_trades, " (", fmt_pct(stats$loss_trades_pct), ")")),
      c("Total", stats$total_trades))),
    detail_card("Open Positions", list(
      c("Long", paste0(summary$open_long, " (", fmt_pct(summary$open_long_won_pct), "won)")),
      c("Short", paste0(summary$open_short, " (", fmt_pct(summary$open_short_won_pct), "won)")),
      c("Total", summary$open_total))),
    detail_card("Deposit/Withdrawal", list(
      c("Deposit", fmt_money(summary$deposits)),
      c("Withdrawal", fmt_money(summary$withdrawals)),
      c("Total", fmt_money(summary$deposit_withdrawal)))),
    detail_card("Close Swap", list(
      c("+", fmt_money(summary$positive_swap)),
      c("-", fmt_money(summary$negative_swap)),
      c("Total", fmt_money(summary$total_swap)))),
    '</tr></table></td></tr>\n'
  )

  # Trades/Open Positions/Orders each wrapped as their own independent card table (2026-08-17,
  # same pattern cash_table_block/symbol_summary_block already use) -- class="card vgrid" adds
  # vertical column-dividing lines on top of the usual horizontal row lines, since these three no
  # longer share a column grid with each other to visually line up against.
  card_grid_wrap <- function(title, header, rows_html, footer_html, ncols) paste0(
    '<tr><td colspan="', trade_cols, '" style="padding:0"><table cellspacing="0" cellpadding="4" border="0" width="100%" class="card vgrid">',
    '<tr class="stripe"><td colspan="', ncols, '"><b>', title, ':</b></td></tr>\n',
    header, rows_html, '\n', footer_html,
    '</table></td></tr>\n',
    '<tr><td colspan="', trade_cols, '" style="font: 1pt arial">&nbsp;</td></tr>\n'
  )

  body <- paste0(
    if (is_single_column) '' else summary_block_cards,

    symbol_summary_block(trade_rows, trade_cols, show_swap = !isTRUE(summary$is_investment_account)),

    # Order requested 2026-08-18: Trades/Open Positions/Orders come right after Trade Symbol
    # Summary; Dividends, Overnight, Deposit/Withdrawal follow, in that order (Deposit/Withdrawal
    # moved to last, after previously being first of the three -- second reorder same session).
    # summary$closed_pl = SUM(Profit + Commission + Swap + Taxes) over what's actually in Trades
    # (raw_trades -- both closed trades and, for Net, opening/adding fills) -- deliberately NOT
    # summary$balance_total_pl (which nets in Deposit/Withdrawal AND Dividends/Overnight, their
    # own cards with their own Total: rows below).
    card_grid_wrap(trade_table_label, trade_header, closed_html, trade_total_row, trade_cols),
    card_grid_wrap("Open Positions", open_positions_header, open_html, open_total_row, open_ncols),
    card_grid_wrap("Orders", order_header, order_html, '', order_ncols),

    cash_table_block("Dividends", cash_rows[RowType == "dividend"], trade_cols, show_taxes = TRUE, show_comment = FALSE, show_commission = TRUE, show_symbol = TRUE, date_width = "200", total = summary$dividends),
    # Visibility keyed off summary$show_overnight -- TRUE when the account's group has Overnight
    # enabled (Groups.PerformOvernight, a still-test feature scoped to investment accounts) OR
    # this period has real Overnight rows regardless of the flag (safety net so toggling the
    # feature off never silently drops existing data from the report). An enrolled account with a
    # quiet period still shows an empty "Overnight: No transactions" card instead of vanishing.
    if (isTRUE(summary$show_overnight)) cash_table_block("Overnight", cash_rows[RowType == "overnight"], trade_cols, show_taxes = FALSE, show_comment = FALSE, show_commission = FALSE, show_symbol = FALSE, total = summary$overnight) else '',
    cash_table_block("Deposit/Withdrawal", cash_rows[RowType == "balance"], trade_cols, show_taxes = FALSE, show_comment = TRUE, show_commission = FALSE, show_symbol = FALSE, date_width = "200", total = summary$deposit_withdrawal),

    if (show_details) details_block else '',
    '</table>\n'
  )

  paste0(page_head(paste0("Statement: ", login, " - ", name, " (", acc_type, ")")),
         header_block,
         body, page_tail)
}
