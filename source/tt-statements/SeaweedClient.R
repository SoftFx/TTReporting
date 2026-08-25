library(httr)

# PUT statement HTML to the SeaweedFS filer's plain HTTP API, under the same
# <domain>/<period_folder>/<login>_mail.html layout the disk backend uses.
# Contract matches the disk write path (writeLines): returns TRUE on success, THROWS on
# failure -- callers rely on their own tryCatch(..., error = function(e) FALSE) to turn a
# thrown error into FALSE, same as a disk I/O error would.
write_statement_to_seaweed <- function(html, domain, period_folder, login, filer_url) {
  remote_path <- sprintf("%s/buckets/statements/%s/%s/%s_mail.html", filer_url, domain, period_folder, login)
  resp <- PUT(remote_path, body = html)
  stop_for_status(resp)
  TRUE
}
