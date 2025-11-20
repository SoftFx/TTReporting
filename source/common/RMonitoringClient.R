library(httr)
library(jsonlite)
library(data.table)
library(lubridate)
options(scipen = 999, digits.secs = 6)

# Вспомогательная функция для всех запросов к HSM
.hsm_post <- function(productKey, abs_url, body_list) {
  body_json <- jsonlite::toJSON(body_list, auto_unbox = TRUE, pretty = TRUE)
  
  res <- httr::POST(
    abs_url,
    body = body_json,
    httr::add_headers(key = productKey),
    httr::config(
      ssl_verifypeer = 0L,
      ssl_verifyhost = 0L,
      verbose = FALSE
    ),
    content_type_json()
  )
  
  return(res)
}

UpdateBoolSensorValue <- function(productKey, address, port, path, value, status = 0, comment = "") {
  abs_url <- paste0("https://", gsub("https://", "", address), ":", port, "/api/Sensors/bool")
  
  body <- list(
    path    = path,
    time    = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3"),
    comment = comment,
    status  = status,
    value   = value
  )
  
  res <- .hsm_post(productKey, abs_url, body)
  
  if (res$status_code != 200) {
    print(res)
    print(paste("Post req returned a code", res$status_code))
    print(httr::content(res, "text", encoding = "UTF-8"))
    return(FALSE)
  }
  return(TRUE)
}

UpdateStringSensorValue <- function(productKey, address, port, path, value, status = 0, comment = "") {
  abs_url <- paste0("https://", gsub("https://", "", address), ":", port, "/api/Sensors/string")
  
  body <- list(
    path    = path,
    time    = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3"),
    comment = comment,
    status  = status,
    value   = value
  )
  
  res <- .hsm_post(productKey, abs_url, body)
  
  if (res$status_code != 200) {
    print(res)
    print(paste("Post req returned a code", res$status_code))
    print(httr::content(res, "text", encoding = "UTF-8"))
    return(FALSE)
  }
  return(TRUE)
}

UpdateIntSensorValue <- function(productKey, address, port, path, value, status = 0, comment = "") {
  abs_url <- paste0("https://", gsub("https://", "", address), ":", port, "/api/Sensors/int")
  
  body <- list(
    path    = path,
    time    = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3"),
    comment = comment,
    status  = status,
    value   = value
  )
  
  res <- .hsm_post(productKey, abs_url, body)
  
  if (res$status_code != 200) {
    print(res)
    print(paste("Post req returned a code", res$status_code))
    print(httr::content(res, "text", encoding = "UTF-8"))
    return(FALSE)
  }
  return(TRUE)
}

UpdateDoubleSensorValue <- function(productKey, address, port, path, value, status = 0, comment = "") {
  abs_url <- paste0("https://", gsub("https://", "", address), ":", port, "/api/Sensors/double")
  
  body <- list(
    path    = path,
    time    = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3"),
    status  = status,
    comment = comment,
    value   = value
  )
  
  res <- .hsm_post(productKey, abs_url, body)
  
  if (res$status_code != 200) {
    print(res)
    print(paste("Post req returned a code", res$status_code))
    print(httr::content(res, "text", encoding = "UTF-8"))
    return(FALSE)
  }
  return(TRUE)
}

UpdateFileSensolValue <- function(productKey, address, port, path, filePath, status = 0, comment = "", newFileName = "") {
  parsedFilePath <- grep("\\.[A-za-z0-9]{2,5}$", filePath, value = TRUE)
  if (length(parsedFilePath) < 1 && (filePath != "" || length(filePath) != 0)) {
    stop("No files or invalid file format")
  }
  
  parsedFilePath <- unlist(strsplit(parsedFilePath, "\\."))
  fileExtension <- parsedFilePath[length(parsedFilePath)]
  fileName <- basename(parsedFilePath[1])
  
  if (!(newFileName == "" || is.na(newFileName) || is.null(newFileName))) {
    fileName <- unlist(strsplit(basename(newFileName), "\\."))[1]
  }
  
  fileInfo <- file.info(filePath)
  alldata <- readBin(filePath, raw(), size = 1, n = fileInfo$size)
  
  return(
    UpdateFileSensolValueRawMethod(
      productKey, address, port, path,
      alldata,
      fileExtension = fileExtension,
      status = status,
      comment = comment,
      fileName = fileName
    )
  )
}

UpdateFileSensolValueRawMethod <- function(productKey, address, port, path, fileBytes, fileExtension = "html", status = 0, comment = "", fileName = "") {
  abs_url <- paste0("https://", gsub("https://", "", address), ":", port, "/api/Sensors/file")
  
  body <- list(
    path      = path,
    time      = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3"),
    comment   = comment,
    status    = status,
    extension = fileExtension,
    name      = fileName,
    value     = as.numeric(fileBytes)
  )
  
  res <- .hsm_post(productKey, abs_url, body)
  
  if (res$status_code != 200) {
    print(res)
    print(paste("Post req returned a code", res$status_code))
    print(httr::content(res, "text", encoding = "UTF-8"))
    return(FALSE)
  }
  return(TRUE)
}

GetHistorySensorsValues <- function(productKey, address, port, path, from, to = now("UTC"), count = 0) {
  abs_url <- paste0("https://", gsub("https://", "", address), ":", port, "/api/Sensors/history")
  
  body <- list(
    path  = path,
    from  = format(from, "%Y-%m-%dT%H:%M:%OS3"),
    to    = format(to, "%Y-%m-%dT%H:%M:%OS3"),
    count = count
  )
  
  res <- .hsm_post(productKey, abs_url, body)
  
  print(res$status_code)
  if (res$status_code != 200) {
    print(res)
    print(paste("Post req returned a code", res$status_code))
    print(httr::content(res, "text", encoding = "UTF-8"))
    return(data.table())
  }
  
  result <- fromJSON(httr::content(res, "text", encoding = "UTF-8"))
  setDT(result)
  return(result)
}

GetHistoryFileSensors <- function(productKey, address, port, path, fileName, extension, from, to = now("UTC"), count = 0, saveOnDisk = FALSE) {
  abs_url <- paste0("https://", gsub("https://", "", address), ":", port, "/api/Sensors/historyFile")
  
  body <- list(
    path        = path,
    from        = format(from, "%Y-%m-%dT%H:%M:%OS3"),
    to          = format(to, "%Y-%m-%dT%H:%M:%OS3"),
    count       = count,
    fileName    = fileName,
    extension   = extension,
    isZipArchive = FALSE
  )
  
  res <- .hsm_post(productKey, abs_url, body)
  
  if (res$status_code != 200) {
    print(res)
    print(paste("Post req returned a code", res$status_code))
    print(httr::content(res, "text", encoding = "UTF-8"))
    return(data.table())
  }
  
  t <- httr::content(res, "text", encoding = "UTF-8")
  splitedContent <- unlist(strsplit(t, split = "\n"))
  
  header <- splitedContent[1]
  body_lines <- splitedContent[2:length(splitedContent)]
  
  headerParsed <- unlist(strsplit(header, split = ","))
  resDT <- as.data.table(tstrsplit(body_lines, split = ","))
  
  while (length(resDT) < length(headerParsed)) {
    name1 <- paste0("V", length(resDT) + 1)
    resDT[, c(name1) := as.character(NA)]
  }
  setnames(resDT, unlist(strsplit(header, split = ",")))
  
  if (saveOnDisk) {
    resDT[, writeBin(base64enc::base64decode(Value), file.path(paste(Name, Extension, sep = "."))), by = seq_len(nrow(resDT))]
  }
  
  return(invisible(resDT))
}
