#' Write data to InfluxDB database
#'
#' \code{influxr_write} writes a `data.frame` to influxDB databases.
#' @param x A `data.frame` that contains the data to be written to the influxDB database.
#' @param tags A named character vector of tag keys and values `c(tag_key=tag_value)` pairs to
#' be applied to the uploaded data.
#' Must not contain single quotes or double quotes.
#' See \strong{Details} on how to escape special characters and on how unescaped special
#' characters are checked for.
#'  example `tags=(station='Hainich', level = 3)`
#' @param precision precision of the timestamp. Default to milliseconds 'ms'.
#' @param timestamp The timestamp of the data. Can be one of the following:
#' \itemize{
#' \item A numeric vector specifying the column(s) of the provided data in which the timestamp is contained either in \code{POSIXt} format,
#'  or as a character string which is converted to \code{POSIXt} format according to \code{timestamp_format}, using \code{tz} as 
#'  the timezone. If several character columns are specified, their contents in each row are pasted together, separated by a 
#'  single whitespace.
#' \item A character vector which is converted to \code{POSIXt} format according to \code{timestamp_format}, using \code{tz} as the timezone.
#' \item A \code{POSIXt} vector. It will be used as is without conversion or changing the timezone.
#' \item A \code{function} in this case x will be passed to the provided function to extract the timestamp vector.
#' }
#' @param timestamp_format character string. Format for converting \code{timestamp} from character to \code{POSIXt} format. Check the 
#' \code{strptime} function from base R for more information about formatting.
#' @param timestamp_offset Number of seconds to apply as an offset to the timestamp and file 
#'  modification times before uploading to database.
#' @param tz character. Timezone of the data. Will be used in converting \code{timestamp} from character to 
#' \code{POSIXt} format.
#'  The default is environment variable "TZ" otherwise system timezone \code{Sys.timezone()}.
#'  
#'  You can specify other timezones, e.g. for time in Germany, do \code{tz = "Etc/GMT-1"}.
#'  Please note the sign of the offset, which might be counter intuitive: if you are east of Greenwich you have
#'  to give a negative offset because you have to decrease your local time to make it become UTC time. 
#'  For timezones west of Greenwich the offset is positive.
#' @param missing A vector of missing values to be replaced with NULL when uploaded to InfluxDB default to `c(NA, Inf)`. 
#' @param from Timestamp in \code{POSIXt} format specifying the lower limit ("greater or equal to", i.e. inclusive) of 
#' timestamps to upload to database. 
#' Note: timestamps in data to upload have to be in ascending order for the detection to work.
#' @param to Timestamp in \code{POSIXt} format specifying the upper limit ("smaller than", i.e. exclusive) of 
#'  timestamps to upload to database. 
#'  Note: timestamps in data to upload have to be in ascending order for the detection to work.
#' @param inclusive logical. Whether the upper time limit be interpreted inclusively or not.
#' @param exclude_timestamp_cols logical. Whether the column(s) specified in \code{timestamp} be excluded from 
#' \code{dataframe} before upload.
#' @details
#' Influx Line Protocol prohibits quoting of measurements, tag keys, tag values and field keys.
#'  Special characters can nonetheless be escaped with a backslash.
#'  
#'  Please note that in \code{R} you need to escape the backslash character itself with another backslash character 
#'  when constructing strings. Function \code{\link{writeLines}}
#'  may be useful when trying to figure out which characters of a string have not been properly escaped.
#'
#' @return
#' \code{TRUE} if upload is successful.
#' @seealso \code{\link{strptime}} for converting character strings to \code{POSIXt} format.
#'
#' \code{\link{OlsonNames}} for valid time zone names.
#' @export
influxr_write <- function(client,
                         x,
                         measurement,
                         database,
                         precision = c('ms', 'u', 'ns', 's', 'm', 'h'),
                         tags = NULL,
                         timestamp,
                         timestamp_format = NULL,
                         timestamp_offset = 0,
                         tz = ifelse(Sys.getenv("TZ") != "", Sys.getenv("TZ"), Sys.timezone()),
                         missing = c(NA, Inf),
                         from = NULL,
                         to = NULL,
                         inclusive = FALSE,
                         exclude_timestamp_cols = TRUE,
                         ...) {
  extra_args <- list(...)
  verbose <- FALSE
  if (!is.null(extra_args$verbose)) {
    verbose <- extra_args$verbose
  }
  
  # Check if input arguments are valid
  stopifnot(is.data.frame(x))
  stopifnot(is.character(measurement))
  stopifnot(is.character(database))
  stopifnot(is.null(tags) || is.character(tags))
  
  # Currently we support only dataframes.
  if(is(x, 'data.table')) {
    x <- as.data.frame(x)
  }
  
  # Format time stamp to a POSIXct vector and convert time to server time.
  ts_origin <- extra_args$origin

  formatted_x <- format_timestamp(x, timestamp, timestamp_format,
                                  source_tz = tz, server_tz = client@server_timezone,
                                  origin = ts_origin)
  rm(x)
  
  # Subset input, normalize the column names and remove missing values.
  formatted_x <- subset_and_normalize (
    formatted_x,
    from = from,
    to = to,
    offset = timestamp_offset,
    missing = missing,
    inclusive = inclusive
  )
  
  if (is.null(formatted_x)) {
    return(FALSE)
  }
  
  
  # Split the input if it's larger than chunk size
  nrow_x <- NROW(formatted_x$dataframe)
  row_chunk_size <- floor(client@post_chunk_size / NCOL(formatted_x$dataframe))
  
  chunks <- split(1:nrow_x, ceiling(seq_len(nrow_x) / row_chunk_size))
  
  if (nrow_x > row_chunk_size && verbose) { cat('Uploading content in chunks.\n') }
  
  response <- NULL
  for (chunk in chunks) {
    # Covert cleaned data to line protocol.
    cat(ifelse(is.null(response),
               'Uploading chunks...',
               '.'))
    x_line_protocol <- influxr_line_protocol(
      x = formatted_x$dataframe[chunk,,drop = FALSE],
      timestamp = formatted_x$time[chunk],
      measurement = measurement,
      precision = precision,
      tags = tags
    )
     
    if(!is.null(extra_args$dry_run ) && extra_args$dry_run) {
      cat('\n Dry run selected, uploading nothing. \n')
      return (x_line_protocol)
    }
    
    # Upload converted data to influxdb server.
    response <-
      c(
        response,
        influxr_post(
          x = x_line_protocol,
          client = client,
          database =  database,
          precision = precision,
          ...
        )
      )
    
  }
  cat('\n')
  
  return(all(response))
}


# Format timestamp ----
#'
#' @keywords internal
#' @noRd 
format_timestamp <- function(x, timestamp, timestamp_format, exclude_timestamp_cols = TRUE, 
                             source_tz = "Etc/GMT", server_tz = 'Etc/GMT', origin = NULL) {
  
    timestamp_vector <- extract_timestamp_vector (x, timestamp)
    
    if (is.character(timestamp_vector) || is.factor(timestamp_vector)) {
      # Convert "timestamp.character" to POSIX, using "format" and "tz".
      if (is.null(timestamp_format)) {
        stop('Argument "timestamp_format" needs to be provided.')
      }
      timestamp.posix <-
        as.POSIXct(x = timestamp_vector, format = timestamp_format, tz = source_tz)

    } else if (is(timestamp_vector, 'POSIXt')) {
      timestamp.posix <- timestamp_vector

    } else if (is.numeric(timestamp_vector)) {
        if (is.null(origin)) {
            stop("Timestamp is numeric, 'origin' must be supplied as an additional argument.")
        }
       timestamp.posix <- as.POSIXct(timestamp_vector, 
                                     origin = origin, tz = source_tz) 
    } else {
      stop('Could not use provided timestamp.')
    }
    
    
    # Finialize
    if (all(is.na(timestamp.posix))) {
      stop(
        'Failed to format timestamp column, resulted NA. 
        Check your choice of timestamp_format and test it with strptime() R function'
      )
    }
    
    
    if (NROW(timestamp.posix) != nrow(x)) {
      stop('Provided timestamp and dataframe differ in length.')
    }
    
    if (anyDuplicated(timestamp.posix)) {
      warning(paste(
        'Timestamp contains duplicates.',
        sum(duplicated(timestamp.posix)),
        'values will be overwritten.'
      ))
    }
    
    # If desired, exclude column(s) "timestamp" from "dataframe".
    if (exclude_timestamp_cols && is.numeric(timestamp) && !is(timestamp, 'POSIXt')) {
      x <- subset(x, select = -timestamp)
    }
    
    return(list(dataframe = x, time = timestamp.posix))
  }

#' @keywords internal
#' @noRd 
extract_timestamp_vector <- function(x, timestamp) {

    if(is.function(timestamp)) {
        message("Applying timestamp formatting function")
        return(timestamp(x)) 
    }

    # If it's a character vector or POSIXt vector it's provided separately
    if (is.character(timestamp) || is(timestamp, 'POSIXt')) {
        return(timestamp)
    }

    # Is timestamp vector part of the data frame?
    if (is.numeric(timestamp) && length(timestamp) > 1) {
        # If the timestamp information is spread out over multiple columns,
        # paste the contents of the specified column(s) into a single character string
        # (each column content separated by " ") for each row of "dataframe".
        timestamp_vector <-
            do.call(what = paste, args = c(x[, timestamp], sep = " "))
    } else {
        timestamp_vector <- x[, timestamp]
    }

    return(timestamp_vector)

}


# Subset and offset ---


#' @keywords internal
#' @noRd 
subset_and_normalize <-
  function (x,
            from = NULL,
            to = NULL,
            offset = 0,
            missing = c(NA, Inf),
            inclusive = F) {
    
    # Add argument "offset" to timestamp.
    if(offset > 0) {
      x$time <- x$time + offset
    }
    
    # If specified, use argument "from" as the lower time limit for data selection.
    if (!is.null(from)) {
      if (min(x$time) < from) {
        x$dataframe <- x$dataframe[x$time >= from, ]
        x$time <- x$time[x$time >= from]
      }
    }
    
    # If specified, use argument "to" as the upper time limit for data selection.
    if (!is.null(x = to) && length(x$time) > 0) {
      if (inclusive) {
        if (max(x$time) > to) {
          x$dataframe <- x$dataframe[x$time <= to, , drop = FALSE]
          x$time <- x$time[x$time <= to, drop = FALSE]
        }
      } else {
        if (max(x$time) >= to) {
          x$dataframe <- x$dataframe[x$time < to, , drop = FALSE]
          x$time <- x$time[x$time < to, drop = FALSE]
        }
      }
    }
    
    
    # Replace missing with NA
    for (col_idx in ncol(x$dataframe)) {
      x$dataframe[x$dataframe[, col_idx] %in% missing, col_idx] <- NA
    }
    
    # Remove NAs
    bad_rows <- rowSums(is.na.data.frame(x$dataframe)) == ncol(x$dataframe)
    bad_rows <- bad_rows | is.na(x$time)
    x$dataframe <- x$dataframe[!bad_rows,,drop=FALSE]
    x$time <- x$time[!bad_rows]
    
    # Proceed only if there is data left after applying lower and upper time limit.
    if (0 %in% dim(x$dataframe)) {
      warning('Nothing to write, dataframe is empty. Check "from" and "to" arguments')
      return(NULL)
    }
    
    
    # make.names() makes syntactically valid names out of character vectors. A syntactically valid name consists of letters, numbers and the dot or underline characters and starts with
    # a letter or the dot not followed by a number.
    original_colnames <- colnames(x$dataframe)
    colnames(x$dataframe) <-
      make.names(original_colnames, unique = TRUE)
    
    
    # drop trailing dots in colnames
    colnames(x$dataframe) <-
      gsub(
        x = colnames(x$dataframe),
        pattern = "(\\.)+$",
        replacement = ""
      )
    
    if (any(original_colnames != colnames(x$dataframe))) {
      warning(paste(
        'Variable names changed while upload.',
        'New names are:',
        paste(colnames(x$dataframe), collapse = '; ')
      ))
    }
    
    return(x)
    
  }

