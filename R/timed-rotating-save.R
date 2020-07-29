#' Save into timed rotating files
#' 
#' @param x data.table. Data to be written, first variable should be named time and of type POSIXct
#' @param path string. Path where files should be saved.
#' @param base_file_name string. File name to be used as a base.
#' @param rotation_interval Duration of data to be written on each file eg. '1 day' or '1 hour 30 minutes' 
#' @param time_suffix_format Time format for file name suffix eg. '%Y%m%d%H%M%S' if left to auto, it will automatically choose formatting.
#' @param trunc_start Whether to truncate the start time to the nearest rotation_interval.
#' @param chunk_number numeric. Number of the chunk.
#' @param ... passed to fwrite
#' @param append logical. Whether to append if same file exists on disk
#' @export
#' @details Important: date time columns are converted to UTC upon saving.
#' 
save_timed_rotating_files <- function(x, path,  base_file_name, rotation_interval = '1 day', time_suffix_format = 'auto',
                                      trunc_start = TRUE, chunk_number = NULL, drop_time = FALSE, append = TRUE, filter = NULL, ...){
  
  stopifnot(is.data.table(x))
  
  stopifnot(dir.exists(path))
  
  if(nrow(x) < 1) {
    warning('Writing timed rotating file: empty data table provided, skipping.')
    return()
  }

  setorderv(x, "time")
  start_time <- x[1, time]
  end_time <- x[.N,time]
  
  if(trunc_start) {
    start_time = floor_date(start_time, unit = rotation_interval)
  }
  
  
  files_boundaries <- find_time_boundaries (start_time, end_time, rotation_interval)
  
  if (time_suffix_format == 'auto') {
    format_string <- guess_format(rotation_interval)
  } else {
    format_string = time_suffix_format
  }
  
  
  
  for (subset in seq_len(nrow(files_boundaries))) {
    subset_start <- files_boundaries[subset, 'start_points']
    subset_end <- files_boundaries[subset, 'end_points']
    
    file_content <- x[time >= subset_start & time < subset_end]
    rotated_file_name <- format_file_name(base_file_name, subset_start, format_string)
    
    written_files <- c()
    
    full_file_name <- file.path(path, rotated_file_name)
    should_append <-  append && file.exists(full_file_name)
    same_session_file <- rotated_file_name %in% written_files
    
    # Warn if file exists, and 
    # it's the same session OR it's the first chunk
    # Same session means faulty data OR time stamp
    # First chunk means the file has already been there before

    if (is.null(chunk_number)) chunk_number = 0
    if(should_append && (same_session_file || chunk_number == 1)) {
      warning(paste('Timed rotating saving: file', full_file_name, 'exists. Appending new data to the end of the file.'))
    }

    # If a filtering function is provided
    if (!is.null(filter)) {
        filter <- match.fun(filter)
        file_content <- filter(file_content)

    }



    
    if(nrow(file_content) > 0) {
      if(!should_append) {
        cat('\nWriting file:', full_file_name, '\n')
      }
      if (drop_time) {
        file_content <- file_content[,-'time']
      }

      fwrite(file_content, file = full_file_name, append = should_append, ...)
      written_files <- c(written_files, rotated_file_name)
    }
    
  }
  
}


find_time_boundaries <- function(start_time, end_time, rotation_interval) {
  
  stopifnot(is.POSIXt(start_time) && is.POSIXt(end_time))
  stopifnot(end_time > start_time)
  
  
  time_seq <- seq(start_time, end_time, by = duration(rotation_interval))

  if (time_seq[length(time_seq)] != end_time) {
      time_seq <- c(time_seq, time_seq[length(time_seq)] + duration(rotation_interval))
  }


  start_points <- time_seq[1:(length(time_seq) -1 )]
  end_points <- time_seq[2:length(time_seq)]
  boundaries <- data.frame(start_points = start_points, end_points = end_points)
  
  return(boundaries)
  
}



guess_format <- function (x)   {
  
  if (duration(x) %% duration('1 year') == duration('0s')) {
    format_string = '%Y'
  } else if (duration(x) %% duration('1 month') == duration('0s')) {
    format_string = '%Y%m'
  } else if (duration(x) %% duration('1 day') == duration('0s')) {
    format_string = '%Y%m%d'
  } else if (duration(x) %% duration('1 hour') == duration('0s')) {
    format_string = '%Y%m%d%H'
  } else if (duration(x) %% duration('1 minute') == duration('0s')) {
    format_string = '%Y%m%d%H%M'
  } else if (duration(x) %% duration('1 second') == duration('0s')) {
    format_string = '%Y%m%d%H%M%S'
  } else {
    format_string = '%Y%m%d%H%M%0S'
  }
  
  return(format_string)

}





format_file_name <- function (x, start_time,  format_string) {
  
  formatted_suffix <- format.POSIXct(start_time, format = format_string)
  if (is.na(formatted_suffix)) {
    stop(paste("Couldn't format file time stamp:", start_time, "with format:", 
               format_string, "check time formatting string."))
  }
  
  return(paste0(x, formatted_suffix))
  
}

