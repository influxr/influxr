#' List files in a directory
#'
#' \code{influxr_list_files} (recursively) list all files in a directory that
#' names match criteria.
#' 
#' @param path character. Path to the directory.
#' @param pattern character. String pattern (regex) used for selecting files for
#' upload. If \code{pattern} is a character vector of length > 1, its elements
#' are connected with a logical AND, i.e., all elements must match to select a
#' file for upload.
#' @param pattern_exclude character or \code{NULL}. If character, it is taken as
#' the filename string pattern used for deselecting files for upload.
#' If \code{pattern_exclude} has a length > 1, its elements are connected with
#' a logical OR, i.e., one matching element is sufficient to deselect a file
#' for upload. If \code{NULL}, no deselection based on filename takes place.
#' @param grep_path logical. If \code{TRUE}, the whole file path will be
#' considered when selecting files based on \code{pattern} and \code{pattern_exclude}.
#' If \code{FALSE}, only the filename will be considered when selecting files
#' based on \code{pattern} and \code{pattern_exclude}.
#' @param recursive logical. Should the directory be searched recursively?
#' @param min_size numeric. Minimum size of files to be selected for upload in bytes.
#' @param max_size numeric. Maximum size of files to be selected for upload in bytes.
#' @param skip numeric. Number of files to skip. Indexing is done according to
#' \code{list.files(..., full.names = TRUE)}.
#' @param from \code{POSIXt} data time object. If not NULL file list will be
#' subsetted
#' based on file modification time (mtime), and files will which have a i
#' timestamp same as or newer than \code{from} will be kept.
#' @param to \code{POSIXt} data time object. If not NULL file list will be subsetted
#' based on file modification time (mtime), and files will which have a
#' timestamp older than \code{from} will be kept.
#' @param mtime logical or character. If \code{TRUE}, data selection is based
#' on file modification time (mtime).
#' If character, it is used as the format string for converting the filename
#' to \code{POSIXt} on which data selection is based.
#' In both cases, \code{from} and \code{to} specify the data selection time limits.
#' If \code{FALSE}, data selection is not based on file modification time or
#' filename timestamp.
#' @param mtime_format Format of modification time.
#' @param nfiles integer. Maximum number of files to process.
#' @param auto_skip_processed logical. When TRUE \code{influxr_list_files} will
#' compare the listed files against upload 
#' status file created by \code{influxr_upload_files} funtion and will skip
#' files that have been processed and their modification time haven't changed.
#' To use
#' this feature \code{store_status_locally} argument has to be enabled when
#' using \code{influxr_upload_files} function.
#' @param verbose. Whether to print output messages.
#' @export
influxr_list_files <- function (path = '.',
                        pattern = '*',
                        pattern_exclude = NULL,
                        grep_path = FALSE,
                        recursive = FALSE,
                        min_size = 0,
                        max_size = Inf,
                        skip = 0,
                        from = NULL,
                        to = NULL,
                        file_mtime = c('mtime', 'filename'),
                        mtime_format = NULL,
                        nfiles = Inf,
                        auto_skip_processed = FALSE,
                        verbose = FALSE,
                        ...) {
  file_mtime <- match.arg(file_mtime)
  
  cat ("Listing available files in dir: \n\t", path, '\n')
  
  files <- normalizePath(list.files(
    path = path,
    recursive = recursive,
    full.names = TRUE
  ))
  
  if (grep_path) {
    for (k in 1:length(pattern)) {
      files <- files[grep(pattern = pattern[k], x = files)]
    }
  } else {
    for (k in 1:length(pattern)) {
      files <- files[grep(pattern = pattern[k], x = basename(files))]
    }
  }
  
  if (verbose) {
    nfiles <- length(files)
    cat(nfiles, "files found after selecting for pattern.\n")
  }
  
  
  selection <- rep(TRUE, length(files))
  
  
  
  if ((min_size + max_size) != 0) {
    
    ## only process files which file size lies between min and max
    selection[!{
      file.size(files) >= min_size &
        file.size(files) <= max_size
    }] <- FALSE
    
    if (verbose) {
      cat(sum(selection), "files remained after selecting for size.\n")
      
    }
  }
  
  # only process files with mtime inside specified time range
  # consider file modification time if mtime = TRUE
  mtimes <- file.info(files)$mtime
  if (file_mtime == 'mtime') {
    if (!is.null(from)) {
      selection[!(mtimes >= from)] <- FALSE
    }
    if (!is.null(to)) {
      selection[!(mtimes < to)] <- FALSE
    }
    if (verbose) {
      cat(
        sum(selection),
        "files remained after selecting for mtime.\n"
      )
    }
  }
  
  # consider time from file name mtime = format of time in file name
  if (file_mtime == 'filename') {
    if (!is.null(from)) {
      selection[!strptime(basename(files), format = mtime_format, tz = tz)  >= from] <-
        FALSE
    }
    if (!is.null(to)) {
      selection[!strptime(basename(files), format = mtime_format, tz = tz)  < to] <-
        FALSE
    }
    if (verbose) {
      cat(
        sum(selection),
        "files remained after selecting for mtime from file name.\n"
      )
    }
  }
  
  
  # deselect files according to pattern_exclude
  if (!is.null(pattern_exclude)) {
    for (k in 1:length(pattern_exclude)) {
      selection[grep(pattern = pattern_exclude[k], x = files)] <- FALSE
    }
    
    if (verbose) {
      cat(sum(selection), "files remained after selecting for pattern_exclude.\n")
    }
  }
  
  if (file_mtime == 'filename') {
    files_mtime <-
      strptime(x = basename(files[selection]),
               format = format,
               tz = tz) 
    
    if (!is.null(from)) {
      selection <- selection & (files_mtime > from)
    }
    if (!is.null(to)) {
      selection <- selection & (files_mtime < to)
    }
    if (verbose) {
      cat(
        sum(files[selection]),
        "files remain after selecting for time from filename.\n"
      )
      
    }
  }
  
  if (skip >= sum(selection)) {
    cat('Nothing left after skipping', skip, 'files.\n')
    files <- NULL
  } else if(skip > 0) {
    if (verbose) {
      cat('Skipping first', skip, 'files.\n')
    }
    selection[selection][1:skip] <- FALSE
  }
  
  if (auto_skip_processed) {
    should_upload <- new_or_changed(files[selection], verbose)
    selection[selection] <- selection[selection] & should_upload
  }
  
  files <- files[selection]
  
  if (verbose) {
    cat(length(files), 'files returned.\n')
  }
  
  return(files)
}


# For a character vector of files, check these files against a status_file to see whether the mtime or size has been changed since 
# these files were uploaded. Will return TRUE when the file has changed or new.
new_or_changed <- function(files, verbose = FALSE) {

  status_files <- file.path(unique(dirname(files)), '.influxr_propcessed_files')
  processed_files_table <- read_status_file(status_files)
  
  all_new <- rep(TRUE, length(files))

  if (is.null(processed_files_table)) {
    return(all_new)
  }
  
  # .influxr_propcessed_files format
  # (1) file_number; (2) base_file_name; (3) mtime; (4) size; (5) system_time
  # [,1] [,2]                    [,3]            [,4]       [,5]                 
  # [1,] "1"  "output_20181001101311" "1538391879.19" "42650214" "2018-10-25 13:14:58"
  
  
  files_base_names <- basename(files)
  
  # which files are on the processed status file
  files_with_status <- files[files_base_names %in% processed_files_table$name]
  
  if (verbose && length(files_with_status) > 0) {
    cat(length(files_with_status),
        'previously uploaded files were found on the list. \n')
  }
  
  # If so get the new mtime of these files
  new_mtime <-
    as.numeric(file.info(files_with_status)$mtime)
  
  new_size <- file.size(files_with_status)

  new_files_status <- data.frame(name = basename(files_with_status), 
                                 new_size = new_size, new_mtime = new_mtime)
  
  status <- merge(processed_files_table, new_files_status, on = "name")
  
  # Did the new modification time change form the old stored one.
  same_mtime <- abs(status$new_mtime - as.numeric(status$mtime)) < 1
  
  # Did the size change
  same_size <- status$new_size == as.numeric(status$size)
  
  # Have been uploaded before and haven't changed
  status$not_changed <- same_mtime & same_size
  
  if (verbose && sum(status$not_changed) > 0) {
    cat(sum(status$not_changed),
        'files have not changed and will be skipped. \n')
  }
  
  # Which files from the list have already been processed and haven't changed
  not_changed_files <- files_base_names %in% status$name[status$not_changed]
  
  
  if (sum(not_changed_files) > 0) {
    # Return the new files (wasn't processed)
    return(!not_changed_files)
  } else {
    return(all_new)
  }
  
}


read_status_file <- function (status_files) {
  doExist <- file.exists(status_files)
  status_files <- status_files[doExist]
  if (length (status_files) > 0) {
    
    cat('Previously proccessed files found, reading status file (s).\n')
    
    file_content_list <- lapply(status_files, function (x) {
      result <- tryCatch (
        read.table(x, sep = ';', stringsAsFactors = F),
        error = {
          function (e)
            e
        }
      )
      
      if (is(result, 'error')) {
        warning(paste('Could not read status file', x, result$message))
      } else {
        # complete and normalize the path of processed files
        # base_dir <- dirname(x)
        # result[,2] <- normalizePath(file.path(base_dir, result[,2]), mustWork = FALSE)
        return(result)
      }
    })
    
    processed_files_table <- do.call(rbind, file_content_list)
    colnames(processed_files_table) <- c("id", "name", "mtime", "size", "date")
    
    # Keep only latest version for each file
    processed_files_table <- processed_files_table[!duplicated(processed_files_table$name, fromLast = TRUE),]

    if (nrow(processed_files_table) > 0) {
      return (processed_files_table)
    }
  }
  return(NULL)
}

