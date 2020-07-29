#' Fetch a query from InfluxDB
#'
#' \code{influxr_fetch_query}
#' @description \code{influxr_fetch_query()} fetches the results of a query
#' object from the database.
#' @param query \code{infux_query} object created with \code{\link{Influx_query}}
#' @keywords internal
#' @noRd 
influxr_fetch_query <- function(client, query, verbose = FALSE) {
  if (verbose) {
    cat(paste("Querying:      ", query@query, "\n"))
    cat(paste("From database: ", query@database, "\n"))
  }
  
  
  # create a handle 
  h <- query_handle(client, query)
  
  result <- NULL
  
  # Set number of lines to read per chunk.
  nlines <- client@chunk_size
  
  # Create a connection with curl.
  con <- curl::curl(url = client@query_endpoint,
                    open = "rb",
                    handle = h)
  
  
  # Close the connection if the function exits for whatever reason.
  on.exit(expr = close(con))
  
  
  # Set chunk counter.
  chunk_nr <- 1
  
  should_write_to_file <- is.function(query@write_handler)
  
  
  if(should_write_to_file && verbose) {
    cat('Result will be written to disk.')
  }
  
  # As long as the connection produces output, read from it.
  if(verbose) {cat('\n')}

  database_header = NULL
  
  while (length(x = read_chunk <-
                readLines(con = con, n = nlines + (chunk_nr == 1)))) {
    if (verbose) {
      show_progress(chunk_nr)
    }
    
    if (query@return_type == 'data.table') {
      # Convert chunk to datatable
      read_chunk <- chunk_to_datatable (read_chunk, database_header, query@precision, 
                                        query@ignore_name, query@ignore_tags)
      database_header <- attr(read_chunk, 'database_header', TRUE)
      
    }  
    
    ## Get file writing args
    write_handler_args <- query@write_handler_args

    # Process writing to a file
    if (should_write_to_file) {
      write_handler_args$x <- read_chunk
      write_handler_args$chunk_number <- chunk_nr
      do.call(query@write_handler, write_handler_args)
    } else {
      result <- concat_chunks(result, read_chunk)
    }

    chunk_nr = chunk_nr + 1
  }
  
  if (verbose) {
    cat('\n')
    show_query_summary(h, NROW(result))
  }
  
  if (!should_write_to_file) {
    return(result)
  }
  
}


#' write datatable chunk to file
#'
#' @param x datatable chunk.
#' @param file_name  full file name.
#' @param chunk_number chunk number in order to manage header.
#' @param append logical. Whether to append to existing file.
#' @examples
#' # Write `influxr_select` output to a csv file
#'
#' \dontrun{
#' output_file <- tempfile()
#' influxr_select(tss_client, database = 'metrics', measurement = 'cpu',
#'                tags = c(level = 1, station = 'home'),
#'                from = '2017-07-10 12:00:00', to = '2017-07-20 12:00:00',
#'                write_handler = write_csv_handler,
#'                file_name = output_file, append = TRUE)
#' }
#' @export
write_csv_handler <- function(x, file_name, chunk_number, append = TRUE) {
    should_append <- append || chunk_number !=1
    data.table::fwrite(x = x,
                       file = file_name,
                       sep = ';',
                       append = should_append) 
    
}



#' Concatenate chunks (rbind)
#'
#' @keywords internal
#' @noRd 
concat_chunks <- function(content, read_chunk) {
  if (is.null(content)) {
    return(read_chunk)
  } else {
    if (typeof(read_chunk) == "character") { # In case plain text return type is requested.
      return(c(content, read_chunk))
    } else {
      return(rbind(content, read_chunk))
    }
  }
}

#' Convert chunk to datatable
#' formats time, maintains header and remove extra columns
#' @param read_chunk raw chunk as text fetched from influxDB server
#' @param header header row as character vector if empty '' that indicates it it the first line
#' @param database_header character vector. header names for the resulting columns from the database.
#' @keywords internal
#' @noRd 
chunk_to_datatable <-
  function(read_chunk, database_header = NULL, precision, ignore_name = TRUE, ignore_tags = TRUE) {
    
    
    
    chunk_dt <-
      data.table::fread(
        input = paste(read_chunk, collapse = '\n'),
        integer64 = "numeric",
        sep = ',',
        header = is.null(database_header)
      )
    
    
    # Get the colnames from the first chunk.
    if (is.null(database_header)) {
      database_header <- colnames(chunk_dt)
    } else {
      colnames(chunk_dt) <- database_header
    }
    
    db_col_names <- colnames(chunk_dt)
    
    # Get the index of the "time" column
    time_index <- min(which( grepl(pattern = "time", x = db_col_names, fixed = T)), Inf)
    
    
    # Convert time column if it exists
    if (!is.infinite(time_index)) {
        chunk_dt[, (time_index) := rfc3339_to_POSIXct(chunk_dt[, time_index, with = FALSE][[1]], precision = precision)]
    }
    
    to_remove <- rep(FALSE, length(db_col_names))
    
    if(ignore_name) {
      to_remove <- to_remove | db_col_names == 'name'
    }
    
    if(ignore_tags) {
      to_remove = to_remove | db_col_names == 'tags'
    }
      
    if(any(to_remove))  {
      remove_cols = which(to_remove)
      chunk_dt <- chunk_dt[,-remove_cols, with = FALSE]
    }

    # Store the header fetched from the database as an attribute 
    attr(chunk_dt, 'database_header') <- db_col_names
    
    # Remove missing values
    # chunk_dt <- chunk_dt[,lapply(.SD, replace_missing_values)]
    chunk_dt[,(colnames(chunk_dt)) := lapply(.SD, replace_missing_values)]
    
    return(chunk_dt)
  }

replace_missing_values <- function(x, missing = c(-9999.00, -9999.99, -9999.9999), missing_replacement = NA) {
  to_replace <- x %in% missing
  x[to_replace] <- missing_replacement
  return(x)
}

#' Show progress
#'
#' @keywords internal
#' @noRd 
show_progress <- function(chunk_nr) {
  status <- paste('Processing chunk:', chunk_nr)
  cat("\r")
  cat(status)
}

#' Helper function
#' Convert RFC3339 time stamp to POSIXct
#' @param x numeric timestamp
#' @param precision precision of the provided timestamp default to 'ms'.
#' @noRd 
rfc3339_to_POSIXct <- function(x, precision = 'ms', server_tz = 'UTC') {
  timestamp <-
    as.POSIXct(
      x = x / influxr_precision_multiplier(precision),
      origin = "1970-01-01",
      tz = server_tz
    )
  
  local_tz = ifelse(Sys.getenv('TZ') != '',  Sys.getenv('TZ'), Sys.timezone())
  attr(timestamp, 'tzone') <- local_tz
  
  return(timestamp)
  
}


#' Create curl handle for a specific client and query
#' @keywords internal
#' @noRd 
query_handle <- function(client, query) {
  # Define a new curl handle.
  h <- curl::new_handle()
  
  # Authentication
  should_authenticate <-
    length(client@username) >= 1 && length(client@password) >= 1
  
  if (should_authenticate) {
    curl::handle_setopt(handle = h,
                        userpwd = paste0(client@username, ":", client@password))
  }
  
  # SSL verification
  curl::handle_setopt(h, ssl_verifypeer = as.integer(client@ssl_verifypeer))
  
  
  # Set handle parameters.
  curl::handle_setform(
    handle = h,
    q = query@query,
    db = query@database,
    chunked = 'true',
    epoch = query@precision,
    chunk.size = toString(as.integer(client@chunk_size)),
    precision = query@precision
  )
  
  
  # Add header to request csv format.
  curl::handle_setheaders(handle = h, "Accept" = "application/csv")
  
  return(h)
}


show_query_summary <- function(h, n) {
  cat('Returned', n, 'value(s).\n')
  cat('Times:\n')
  print(curl::handle_data(h)$times)
  
}
