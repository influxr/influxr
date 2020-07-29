#' @title  File fast read and recovery
#'
#' @description \code{influxr_fread_recover}.
#' @inheritParams data.table::fread
#' @param skip_lines Number of lines to skip.
#' @param read_method Either 'fast', 'R' or a function.
#' If 'fast' \code{fread} function from package data.table will be
#' used to read files, it has fast performance but doesn't handle corrupted files well,
#' the other option 'R' would use internal R \code{readLines} function to read the files
#' and try to strip irregularities before passing the content again to fread for parsing.
#' If a function is provided, file content will be read using the provided function.
#' The function should accept file name as a \code{file} argument. And any additional arguments will be
#' passed to the function. It should return file content as text which will parsed using fread.
#' This can be useful when some treatments are necessary for the files before reading e.g. unzip, or format conversion.
#' @param logical. Strip_extra_whitespaces. When TRUE duplicated spaces as
#' well as trailing and leading white spaces will be removed before processing columns.
#' Useful when you have the separator as white space to avoid
#' confusion with the number of columns.
#' @param ... Additional arguments passed to \code{fread}
#' @details Fill = FALSE will remove any rows that don't have the same number of columns.
#' @export

influxr_fread_recover <-
  function(file,
           sep = 'auto',
           nrows = -1L,
           header = 'auto',
           skip_lines = 0L,
           select = NULL,
           read_method = "fast",
           text_preprocess_FUN = NULL,
           fill = TRUE,
           strip_extra_whitespace = FALSE,
           verbose = FALSE,
           ...) {
    
    extra_args <- list(...)
    fread_extra_args <- subset(extra_args, names(extra_args) 
                               %in% names(formals(data.table::fread)))
    
    # Check if custom read function is provided
    if (!is.function(read_method)) {
	    read_method <- match.arg(read_method, choices = c("fast", "R"))
    }
    
    
    if(!file.exists(file)) {
      stop(paste('File', file, 'doesn\'t exist.'))
    }
    
    stopifnot(is.function(text_preprocess_FUN) || is.null(text_preprocess_FUN)) 
    
    # Try reading normally with fread.
    
    fread_args <-
      c(
        fread_extra_args,
        list(
          file = file,
          sep = sep,
          nrows = nrows,
          header = header,
          select = select,
          skip = skip_lines,
          blank.lines.skip = TRUE,
          fill = fill
        )
      )
    
    if(is.null(select)) {
    # delete item from list (this is due to a bug in fread when select is explicitly set to NULL)
      fread_args$select <- NULL
    } else  {
      fread_args$colClasses <- NULL
      warning('colClasses ingored in fread.')
    }
    
    if (!is.null(text_preprocess_FUN) && read_method == 'fast') { 
      warning('Using R file reading method because text_preprocess_FUN function was supplied.')
      read_method = 'R'
      
    }
    
    file_content <- NULL
    read_error   <- FALSE

    if (is.function(read_method)) {
      message("Calling custom read method on file")
      read_method_args <- subset(extra_args, names(extra_args) 
                                 %in% names(formals(read_method)))
      read_method_args$file <- file
      text_file_content <- do.call(read_method, read_method_args)
    } else {
      if (read_method == 'fast') {
        file_content <- tryCatch(
          expr <- {
            do.call(data.table::fread, fread_args)
          },
          error <- function(e) {
            e
          }
        )
        if (!is(file_content, 'error')) {
          return(file_content)
        }
        read_error <- TRUE
      }
      
      # read text_file_content
      if (read_error || read_method == 'R') {
        if (verbose && read_error) {
          cat(file_content$message, '\n')
          cat(paste('Reading error in file:', file, 'trying to recover content.\n'))
        }
        text_file_content <- readLines(file)
        
      }
    }
    
      
    # Remove NA's
    text_file_content <- text_file_content[!is.na(text_file_content)]

    if (!is.null(text_preprocess_FUN)) {
	    cat('Applying provided text preprocessor function to file content. \n')
	    text_file_content <- text_preprocess_FUN (text_file_content)
    }

    if (skip_lines > 0L) {
	    text_file_content <- text_file_content[-(1:skip_lines)]
    }

    if(sep == 'auto')  {
	    detected_sep <- auto_sep (text_file_content[1:min(60, length(text_file_content))])

    } else {
	    detected_sep <- sep
    }

    # Strip duplicate, leading and trailing whitespaces
    if(strip_extra_whitespace) {
	    text_file_content <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", text_file_content, perl=TRUE)
	    text_file_content <- trimws(text_file_content, which = "both")
    }

    fields_count  <- lengths(gregexpr(detected_sep, text_file_content)) + 1
    ncols <- getmode(fields_count)
    good_lines <- fields_count == ncols
    text_file_content  <- text_file_content[good_lines]



    fread_args$file <- NULL
    fread_args$input <- paste(text_file_content, collapse = '\n') 
    fread_args$sep = detected_sep

    file_content <- do.call(data.table::fread, fread_args)


    if (!fill && sum(!good_lines) > 0) {
	    warning(paste(
			  'Lines numbered:',
			  paste(seq_len(length(text_file_content))[!good_lines], collapse = '; '),
			  'from file',
			  file,
			  'were skipped.'
			  ))
    }

    
    return(file_content)
}

