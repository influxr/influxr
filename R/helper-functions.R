#' Automatically determine the separator in delimited-files
#' \code{auto_sep}
#' @param lines character vector. Rows as lines to determine the separator for.
#' 
#' @keywords internal
#' @noRd 
auto_sep <- function (lines) {
  seps <- c(',', '\t', ' ', '|', ';', ':')
  sep_freq <-
    sapply(seps, function (x)
      count.fields(textConnection(lines), x))
  
  # Sep is the first encounter of the seps vector, which has a median larger than 1
  sep_index <-
    which(apply(sep_freq, 2, function(x)
      median (x) > 1))[1]
  if(is.na(sep_index)) {
    sep <- 'auto'
  } else {
    sep <- seps[sep_index]
  }
  return(sep)
  
}

#' Calculate the mode
#' @param v numerical vector
#' @keywords internal
#' @noRd 
getmode <- function(v) {
  uniqv <- unique(v)
  uniqv[which.max(tabulate(match(v, uniqv)))]
}


#' Try parsing datetime
#'
#' it accepts NULL, converted to POSIXct NA
#' @keywords internal
#' @noRd
influxr_parse_datetime <- function(x) {
    # parse if not NULL and not already POSIX
    if(!is.null(x) && !is(x, 'POSIXct')) {
        ## TODO: remove xts dependency if possible
        x = xts::.parseISO8601(x)$first.time
        cat('Provided timestamp is not POSIXct, the input was parsed to', format(x), '\n')

        # Stop if parsing has failed
        if(is.null(x) || x < 0) {
            stop("Failed to parse input date string.",
                 " Check the datetime input format and make sure it's an ISO8601 string or POSIXct object.",
                 "\n\tinput: ", x)
        }
    # Otherwise, if NULL return NA POSIXct
    } else if(is.null(x)) {
        x <- as.POSIXct(NA)
    }

    return(x)
}

#' Convert POSIXct to time64 string
#' @keywords internal
#' @noRd
influxr_time64String <- function(x) {
  return(format(as.numeric(x)*1e9, scientific = F))
}

#' Escape characters according to influxDB line protocol
#' @keywords internal
#' @noRd 
influxr_escape_characters <- function(x, type = c('tags', 'measurements')) {
  chars <- c(' ', ',', '=')
  chars_measurement <- c(' ', ',')
  
  type <- match.arg(type)
  
  replace_chars <- switch (type,
    tags = chars,
    measurements = chars_measurement
  )
  
  
  for (char in replace_chars) {
    x <- gsub(pattern = char, replacement = paste0('\\', char), x = x, fixed = TRUE)
  }
  
  return(x)
}


#' Get precision multiplier
#' @param precision character milliseconds, nanoseconds, microseconds, seconds, minutes, hours
#' @return numeric multiplier.
#' @keywords internal
#' @noRd
influxr_precision_multiplier <-
  function(precision = c('ms', 'ns', 'u', 's', 'm', 'h')) {
    precision = match.arg(precision)
    m <- switch(
      precision,
      "ns" = 1e+9,
      "n" = 1e+9,
      "u" = 1e+6,
      "ms" = 1e+3,
      "s" = 1,
      "m" = 1 / 60,
      "h" = 1 / 3600
    )
    return(m)
}

#' Store upload status locally
#' store as a table separated by ';' with the following columns.
#' (1) file_number; (2) base_file_name; (3) mtime; (4) size; (5) system_time
#' 
#' @keywords internal
#' @noRd 
influxr_update_upload_status <- function(no, file_name) {
  status_file <-
    file.path(dirname(file_name), '.influxr_propcessed_files')
  
  formatted_mtime <-
    format(as.numeric(file.info(file_name)$mtime), digits = 12)
  new_row <- matrix(c(no, basename(file_name), formatted_mtime, file.size(file_name),
                    format(Sys.time())), ncol = 5, nrow = 1)
  
  try_write <- tryCatch(
    write.table(
      new_row,
      status_file,
      append = TRUE,
      sep = ';',
      col.names = FALSE,
      row.names = F
    ),
    error = function(e) {
      e
    }
  )
  
  if (is(try_write, 'error')) {
    warning(
      paste(
        "Upload status will not be saved, couln't write to file",
        status_file,
        'check if you have permission to write to the specified location. Error details:'
      ),
      try_write$message
    )
  }
}
