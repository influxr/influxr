#' Obtain time stamp from filename
#'
#' @param resolution numeric. Only used if \code{timestamp = "filename"}.
#' Resolution of timestamps in seconds. This is typically the reciprocal of data aquisition frequency.
#' @keywords internal
#' @noRd 
timestamp_from_filename <- function(filename_pattern, resolution, interval_start, 
                                    match_timestamp = FALSE, ...) {
    extra_args = (...)

    timestamp_format <- extra_args$timestamp_format 
    n_lines <- extra_args$n_lines
    file_name <- extra_args$file_name


    # Extract name string based on pattern
    t0 <- as.POSIXct(name_string, format = timestamp_format, tz = tz)
  
    if (start_of_interval){
      direction <- 1
    } else {
      direction <- -1
    }

  
  t <- seq(from = t0, by = direction * resolution, length.out = n_lines)
  if(is.null(direction)) {
    lapply(X, FUN, ...)
  }

}

