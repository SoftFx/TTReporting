library(httr)

# Bucket a domain's statements are written to, from the explicit domain -> bucket map
# config$storage$seaweed_buckets. There is deliberately NO default bucket: a domain missing
# from the map is a configuration error (THROWS), never a silent write to some other bucket.
seaweed_bucket_for_domain <- function(domain, config) {
  bucket <- config$storage$seaweed_buckets[[domain]]
  if (is.null(bucket) || length(bucket) != 1 || is.na(bucket) || !nzchar(bucket)) {
    stop("no bucket in storage.seaweed_buckets for domain ", domain)
  }
  as.character(bucket)
}

# PUT one rendered statement file to the SeaweedFS filer's plain HTTP API, under the
# /buckets/<bucket>/<domain>/<period_folder>/<login><suffix> layout (suffix = "_mail.html" /
# ".csv" / ".json"); everything after <bucket> mirrors the disk backend's layout.
# Contract matches the disk write path (writeLines): returns TRUE on success, THROWS on
# failure -- callers rely on their own tryCatch(..., error = function(e) FALSE) to turn a
# thrown error into FALSE, same as a disk I/O error would.
put_statement_object <- function(content, bucket, domain, period_folder, login, suffix,
                                 content_type = "application/octet-stream", filer_url) {
  remote_path <- sprintf("%s/buckets/%s/%s/%s/%s%s",
                         filer_url, bucket, domain, period_folder, login, suffix)
  resp <- PUT(remote_path, body = content, content_type(content_type))
  stop_for_status(resp)
  TRUE
}
