#' Convert a data.frame to InfluxDB line protocol
#'
#' @param x data.frame. Input to be converted to line protocol.
#' @param timestamp POSIXct vector of the same length as the number of rows of x, 
#' timestamp of data points, Shouldn't contain NAs.
#' @param measurement Character, name of measurement to be used in influxDB database.
#' @param precision Time precision. the time stamp will be rounded to match this 
#' resolution, default to milliseconds.
#' @return character. Data formatted according to influxDB line protocol.
#' @param ... further parameters.
#' @keywords internal
#' @noRd 
influxr_line_protocol <- function(x,
                          timestamp,
                          measurement,
                          tags,
                          precision = c('ms', 'u', 'ns', 's', 'm', 'h'),
                          ...) {
  precision = match.arg(precision)
  
  stopifnot(is.data.frame(x) && is(timestamp, 'POSIXt'))
  
  # extract tag keys and tag values
  if (!is.null(tags))  {
    tag_keys <- influxr_escape_characters(names(tags), type = 'tags')
    tag_values <- influxr_escape_characters(tags, type = 'tags')

    # Replace empty values 
    empty_tag_values <- tag_values == ''
    
    if(any(empty_tag_values)) {
      warning('Tag value is empty, it will be replaced with \'None\'')
      tag_values[empty_tag_values] <- 'None'
    }
    
    
    # merge tag keys and values
    tag_key_value <-
      paste(tag_keys, tag_values, sep = "=", collapse = ",")
  }
  
  # escape characters
  measurement <- influxr_escape_characters(measurement, type = 'measurement')
  
  # create time vector
  time <-
    format(as.numeric(timestamp) * influxr_precision_multiplier(precision),
           scientific = FALSE)
  
#==========================================================================================================+
# Line protocol with datatypes.|  space                                             space                  |
#------------------------------+    v     integer  ,float ,bool        ,string        v                    |
#   |    weather,location=us-midwest temperature=22i,H=15.4,enabled=TRUE,sonic="metek" 1465839830100400200 |
#   |-------------------------------^-------------------------------------------------^---------------------
#   |           |                   |                                                 |                    |
#   +-----------+--------+----------+---------+---------------------------------------+--------------------+
#   |measurement|,tag_set|          |           field_set                             |  timestamp         |
#   +-----------+--------+----------+---------+-+-------------------------------------+--------------------+
  
  quotes <- getOption("useFancyQuotes")
  on.exit(options("useFancyQuotes" = quotes))
  options("useFancyQuotes" = FALSE)
  values = NULL
  
  # Loop over columns
  for (i in 1:ncol(x)) {
    field_name = colnames(x)[i]
    field_values <- x[, i]
    NA_s <- is.na(field_values) 
    
    if (is.integer(field_values)) {
      field_data <- paste0(field_name, '=', field_values, 'i')
    } else if (is.numeric(field_values)) {
      field_data <- paste0(field_name, '=', field_values)
    }
    else if (is.logical(field_values)) {
      field_data <- paste0(field_name, '=', as.character(field_values))
    } else{
      field_data <- paste0(field_name, '=', dQuote(field_values))
    }
    
    if (any(NA_s)) {
      field_data[NA_s] <- "##NA_REPLACE"
    }
    
    if (is.null(values)) {
      values = field_data
    } else {
      values = paste(values, field_data, sep = ',')
    }
    
  }
  
  values <-
    gsub('(,##NA_REPLACE)|(##NA_REPLACE,)|(##NA_REPLACE)',
         '',
         values)
  
  # Trim leading and trailing whitespaces
  values <- trimws(values)
  
  if (is.null(tags)) {
    influxdb_line_protocol <- paste(measurement,
                                    values,
                                    time,
                                    collapse = "\n")
  } else {
    influxdb_line_protocol <- paste0(measurement,
                                     paste0(",", tag_key_value),
                                     " ",
                                     values,
                                     " ",
                                     time,
                                     collapse = "\n")
  }
  
  
  return(influxdb_line_protocol)
  
}




