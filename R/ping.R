#' Ping InfluxDB server
#' @export
#' @docType methods
#' @rdname ping-methods
#' @aliases ping
setGeneric("ping", function(client, ...)
  standardGeneric("ping"))


setMethod('ping', 'Influx_client', function(client, verbose = T) {
  if (!is(client, 'Influx_client')) {
    stop('Provided object is not of type influxr_connection.')
  }
  
  
  res <-
    tryCatch(
      expr = {
        req <- curl::curl_fetch_memory(url = client@ping_endpoint)
        req$success <- TRUE
        req
      },
      error = function(e) {
        e$success <- FALSE
        return(e)
      }
    )
  
  if (!res$success) {
    print(paste('FAILD TO REACH SERVER;',  res$message))
    return(FALSE)
  } else {
    if (res$status_code == 204) {
      if(verbose) {
       cat('Success!\n')
       cat(paste(curl::parse_headers(res$headers), collapse = "\n"))
       cat('\n')
      }
      return(TRUE)
    } else {
      cat(paste(curl::parse_headers(res$headers), collapse = "\n"))
      cat('\n')
      return(FALSE)
    }
    
  }
  
  
})
