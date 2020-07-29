#' @title  Upload file(s) to InfluxDB database
#' 
#' @description \code{influxr_upload_files} uploads a list of files to influxDB.
#' @param files character vector, containing full file names of files to be uploaded, use 
#' \code{\link{influxr_list_files}} provided with this package to get a filtered list of files 
#' according to user specified coditions.
#' @param timestamp Where to get time stamp information for the the provided files. Can be one of the following:
#' \itemize{
#' \item A numeric vector specifying the column(s) of the files in which the timestamp is contained as a character string.
#' The string will be converted to \code{POSIXt} according to \code{timestamp_format}, using \code{tz} as the timezone.
#' If several columns are specified, their contents in each row are pasted together, separated by a single whitespace.
#' \item A character vector which is converted to \code{POSIXt} format according to \code{format}, using \code{tz} as the timezone.
#' \item A function to generate timestamp, for instance, when the filename encodes the start
#' or the end of measurement period. In this case the filename is considered a timestamp 
#' and converted to \code{POSIXt} format according to \code{timestamp_format}, 
#' using \code{tz} as the timezone and arguments 
#' \code{start_of_interval} and \code{resolution} (see there). 
#' This can be used to process files which do not contain a timestamp variable.
#' \item A \code{POSIXt} vector.
#' }
#' @param timestamp_format character. Format used for converting the timestamp to \code{POSIXt} 
#' @param tz character. Timezone of the provided measurements, the timestamp will be converted to the server timestamp before uploading.
#' The default is the system environment variable "TZ" and if empty the system timezone \code{Sys.timezone()}'.
#' You can specify other timezones, e.g. for time in Germany, do \code{tz = "Etc/GMT-1"}. 
#' Please note the sign of the offset, which might be counter intuitive: if you are east of Greenwich you have to give 
#' a negative offset because you have to decrease your local time to make it become UTC time. 
#' For timezones west of Greenwich the offset is positive.
#' @param exclude_timestamp_cols logical. Should the column(s) specified in \code{timestamp} be excluded before upload?
#' @param na_string character vector. Strings to be replaced with (NA) and hence excluded from uploading to the databases.
#' @param skip_lines integer. Rows to skip in each file before reading data.
#' @param nrows numeric. Number of rows to read. -1 means all lines. See \code{\link{fread}}.
#' @param header Passed to fread.
#' @param headerline numeric or \code{NULL}. If numeric, it is considered the row number of the header line from which column names will be taken. If \code{NULL}, column names will depend on \code{col.names}.
#' @param col.classes A character vector of classes (named or unnamed), as \code{\link{read.csv}}. Or a named list of vectors of column names or numbers, see \code{\link{fread}}. If \code{NULL}, column classes are guessed.
#' @param sep character. The separator between columns. See \code{\link{fread}}.
#' @param start_of_interval logical. Only used if \code{timestamp = "filename"} (see there). 
#' If \code{TRUE}, the filename is considered to be the first timestamp. 
#' If \code{FALSE}, the filename is considered the last timestamp.
#' @param text_preprocess_FUN NULL or function. A function to be excuted on the file content before reading as csv
#' @param upload_preprocess_FUN NULL or function. A function to be excuted on the content before uploading to the database (eg. calibration)
#' @param stop_on_error Whether to stop code excution when a reading error is faced. if True instead of halting
#' the excution, the file is ignored and a warning is shown at the end.
#' @inheritParams data.table::fread
#' @param ... Options passed to \code{\link{influxr_fread_recover}}.
#' @export
influxr_upload_files <- function(client,
                         files,
                         measurement,
                         database,
                         precision = c('ms', 'u', 'ns', 's', 'm', 'h'),
                         tags,
                         timestamp,
                         timestamp_format = '%Y-%m-%d %H:%M:%OS',
                         timestamp_offset = 0,
                         tz = ifelse(Sys.getenv("TZ") != "", Sys.getenv("TZ"), Sys.timezone()),
                         headerline = NULL,
                         na_strings = c('', 'NA', 'N/A', 'null', 'None'),
                         skip_lines = 0L,
                         nrows = -1L,
                         col.classes = NULL,
                         coerce.classes = FALSE,
                         select = NULL,
                         sep = 'auto',
                         inclusive = FALSE,
                         exclude_timestamp_cols = TRUE,
                         recover_files = TRUE,
                         read_method = "fast",
                         text_preprocess_FUN = NULL,
                         upload_preprocess_FUN = NULL,
                         stop_on_error = TRUE,
                         store_status_locally = FALSE,
                         verbose = FALSE,
                         ...) {
  
  precision <- match.arg(precision) 
  if(is.null(files) || length(files) == 0) {
    message('Nothing to upload, supplied files argument if of length 0 or is NULL')
    return(NULL)
  }
  
  for (i in seq_along(files)) {
    file <- files[i]
    if (verbose) {
      cat('Processing file',
          i,
          'out of',
          length(files),
          'file name:',
          file,
          '\n')
    }
    
    failed_files <- NULL
    result <- tryCatch(
      expr = .upload_file (
        file = file,
        client = client,
        measurement = measurement,
        database = database,
        precision = precision,
        tags = tags,
        timestamp = timestamp,
        timestamp_format = timestamp_format,
        timestamp_offset = timestamp_offset,
        tz = tz,
        headerline = headerline,
        na.strings = na_strings,
        skip_lines = skip_lines,
        nrows = nrows,
        col.classes = col.classes,
        coerce.classes = coerce.classes,
        select = select,
        sep = sep,
        inclusive = inclusive,
        exclude_timestamp_cols = exclude_timestamp_cols,
        recover_files = recover_files,
        read_method = read_method,
        text_preprocess_FUN = text_preprocess_FUN,
        upload_preprocess_FUN = upload_preprocess_FUN,
        verbose = verbose,
        ...
      ),
      error = function(e)
        e
    )
    
    if (is(result, 'error')) {
      msg <-
        paste('Error: couldn\'t upload file',
              file,
              '\n',
              result$message,
              '\n')
      if (stop_on_error) {
        stop(msg)
      } else {
        message(msg)
        cat('Call:',result$call[[1]], '... \n')
        failed_files <- c(failed_files, file)
      }
    } else {
      if(store_status_locally) {
        influxr_update_upload_status(no = i, file_name = file)
      }
    }
    
    gc()
    
  }
  
  #TODO: clean status file (remove duplicates).
  
  if (!is.null(failed_files)) {
    warn_msg <-
      paste(
        'The following',
        length(failed_files),
        'file(s) couldn\'t be uploaded due to errors:\n',
        paste(failed_files, collapse = '; ')
      )
    warning(warn_msg)
  }
  
  uploaded_files_num <- length(files) - length (failed_files)
  if (uploaded_files_num > 0 && verbose) {
    cat('Successfully uploaded', uploaded_files_num, 'file(s) \n')
  }
  
}

.upload_file <- function(file,
                         client,
                         measurement,
                         database,
                         precision, 
                         tags,
                         timestamp,
                         timestamp_format = '%Y-%m-%d %H:%M:%OS',
                         timestamp_offset = 0,
                         tz = "Etc/GMT",
                         headerline = NULL,
                         na.strings = c('', 'NA', 'N/A', 'null'),
                         skip_lines = 0L,
                         nrows = -1L,
                         col.classes = NULL,
                         coerce.classes = TRUE,
                         select = NULL,
                         sep = 'auto',
                         inclusive = FALSE,
                         exclude_timestamp_cols = TRUE,
                         recover_files = TRUE,
                         read_method = 'fast',
                         text_preprocess_FUN = NULL,
                         upload_preprocess_FUN = NULL,
                         verbose = FALSE,
                         ...) {

  file_content <- influxr_fread_recover (
    file = file,
    headerline = headerline,
    na.strings = na.strings,
    skip_lines = skip_lines,
    nrows = nrows,
    select = select,
    sep = sep,
    recover_files = recover_files,
    read_method = read_method,
    colClasses = col.classes, 
    text_preprocess_FUN = text_preprocess_FUN,
    ...
  )

  if (is.function(timestamp)) {
    #timestamp <- function(filename_pattern, resolution, interval_start, match_timestamp = FALSE, ...) {
  }
  
  
  
  # Coerce to supplied column classes.
  if (coerce.classes && !is.null(col.classes)) {
      if (ncol(file_content) != length(col.classes)) {
          stop(
               paste(
                     'Provided col.classes vector doesn\'t match in length the number of columns in read file \n',
                     'col.classes has',
                     length(col.classes),
                     'elements, while the read file has',
                     ncol(file_content),
                     'columns.'
               )
          )
      }

      for (i in 1:ncol(file_content)) {
          if (!is(file_content[[i]], col.classes[i])) {
              data.table::set(file_content,
                              j = i,
                              value = as(file_content[[i]], col.classes[i]))

              # Check if  nas are introduced by coercion
              NAs <-  is.na(file_content[[i]])
              NAs_N <- sum(NAs)

              if (NAs_N > 0) {
                  warning(NAs_N, " NAs after coersion from lines:\n", 
                          paste(which(NAs), collapse = ", "))
              }


          }
      }
  }
  
  # If a preprocessing function is supplied.
  if (!is.null(upload_preprocess_FUN)) {
    call_args <- as.list(match.call())
    call_args[[1]] <- NULL
    call_args$x <- file_content
    call_args$filename = file
    preprocess_args <- subset(call_args, names(call_args) 
                               %in% names(formals(upload_preprocess_FUN)))
   
    if (verbose) {
      cat('Calling preprocessing function with the arguments:', 
          paste( names(preprocess_args), collapse = ','), '\n')
    }
    file_content <- do.call(upload_preprocess_FUN, args = preprocess_args)
  }
  
  # skip files with zero rows or zero columns
  dim.num <- (NCOL(file_content) - 1) * NROW(file_content)
  
  if (0 %in% dim(file_content)) {
    stop(paste('File:',
                files[i],
                'returned empty content. Nothing to upload.'))
  }
  t0 <- Sys.time()
  response <- influxr_write(
    client = client,
    x = as.data.frame(file_content),
    measurement = measurement,
    database = database,
    precision = precision,
    tags = tags,
    timestamp = timestamp,
    timestamp_format = timestamp_format,
    timestamp_offset = timestamp_offset,
    tz = tz,
    exclude_timestamp_cols = exclude_timestamp_cols,
    verbose = verbose,
    ...
  )
  
  
  if (response && verbose) {
    t1 <- Sys.time()
    cat('Successfully uploaded:', dim.num, 'points.\n')
    cat('Estiamted speed:      ', ceiling(dim.num/(as.numeric(t1)-as.numeric(t0))[[1]]), 'points/sec.\n') 
  }
}

