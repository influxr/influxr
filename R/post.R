#' Http post request to influxDB
#' @param x influxDB line protocol formatted string.
#' @param client influxDB client object created using influxr_connection().
#' @param database character. Database name to upload to.
#' @param precision Timestamp precision.
#' @keywords internal
#' @noRd 
influxr_post <-
  function(x,
           client,
           database, 
           precision = c('ms', 'u', 'ns', 's', 'm', 'h'),
           ...) {
    extra_args <- list(...)
    verbose = FALSE
    if (!is.null(extra_args$verbose)) {
      verbose <- extra_args$verbose
    }
    
    precision <- match.arg(precision)
    
    # Authentication
    auth_config <- list()
    .user_name <- client@username
    .password <- client@password
    
    should_authenticate <-
      length(.user_name) >= 1 && length(.password) >= 1
    
    if (should_authenticate) {
      auth_config <-
        httr::authenticate(user = .user_name, password = .password)
    }
    
    
    httr::set_config(httr::config(ssl_verifypeer = as.integer(client@ssl_verifypeer)))
    if(client@timeout > 0) {
      httr::timeout(client@timeout)
    }
    retries <- 0
    response <- list(success = FALSE)
    
    while (retries <= client@retries && !response$success) {
      
      if(retries > 0) {
        cat('Influx_post: retry number', retries, '\n')
        Sys.sleep(min(exp(retries), 120))
      }
      
      response <-
        tryCatch(
          expr = {
            response <- httr::POST(
              url = client@write_endpoint,
              query = list(db = database, precision = precision),
              body =  x,
              hostname = client@host,
              port = client@port,
              path = 'write',
              scheme = ifelse(client@ssl, 'https', 'http'),
              config = auth_config)
            if (response$status_code %in% c(200, 204)) {
              response$success = TRUE
            } else {
              if(response$status_code %in% c(400:499)) {
                stop(paste (
                  'FAILED to upload to influxDB with status code:',
                  response$status_code, 'Check your request. \n',
                  paste( names(response$headers), sep = ': ', response$headers, collapse = '\n' ))) 
              }
              response$success = FALSE
            }
            response
          }, error = function(e) {
            e$success = FALSE
            e
          })
      
      
      if(is(response, 'error')) {
        stop(paste(response$message, '\n Error occured while trying to post to influxDB server.'))
      }
      retries <- retries + 1
    }
    

    
    
    if (verbose && response$success) {
      if(!is.null(extra_args$show_times) && extra_args$show_times) {
        print(response$times)
      }
    }
    invisible(response$success)
  }
