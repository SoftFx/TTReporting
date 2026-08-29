library(httr)

# PUT one rendered statement file to the SeaweedFS filer's plain HTTP API, under the
# <bucket>/statements/<domain>/<period_folder>/<login><suffix> layout the disk backend
# mirrors (suffix = "_mail.html" / ".csv" / ".json").
# Contract matches the disk write path (writeLines): returns TRUE on success, THROWS on
# failure -- callers rely on their own tryCatch(..., error = function(e) FALSE) to turn a
# thrown error into FALSE, same as a disk I/O error would.
put_statement_object <- function(content, domain, period_folder, login, suffix,
                                 content_type = "application/octet-stream", filer_url) {
  remote_path <- sprintf("%s/buckets/statements/%s/%s/%s%s",
                         filer_url, domain, period_folder, login, suffix)
  resp <- PUT(remote_path, body = content, content_type(content_type))
  stop_for_status(resp)
  TRUE
}
