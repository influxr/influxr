#' @title Define a connection to InfluxDB instance
#' 
#' 
#' @description \code{influxr_connection} Creates an `Influx_client` object which contains connection parameters and
#'  facilitates connecting to an InfluxDB server.
#' 
#' @details
#' The returned object will hold all the information necessary to connect to InfluxDB and shall be passed to functions
#' responsible for connecting to the database. Use \code{'summary()'} on the client object to view basic information.
#' 
#' @param host character string. Hostname of InfluxDB server, default is 'localhost'
#' @param port numeric. Port number to connect to InfluxDB instance, default is 8086
#' @param username character string. User name to use for authentication, default is getting it from the environment
#' variable INFLUX_USERNAME
#' @param password character, password to use for authentication, default is getting it from the environment variable INFLUX_PASSWORD
#' @param ssl logical. Whether https should be used instead of http to connect to InfluxDB, defaults to TRUE.
#' @param ssl_verifypeer logical. Whether to verify SSL peer.
#' @param timeout numeric. Number of seconds requests will wait for the client to
#'  establish a connection, default is 0 (unlimited).
#' @param retries numeric. Number of retries the client will try to connect before 
#' aborting, default is 3, NULL indicates try until success.
#' @param chunk_size numeric. Number of data points to fetch from influxDB server per chunk, 
#' default to 20000.
#' @param post_chunk_size numeric. Number of data points uploaded at each chunk when posting data to 
#' influxDB server.
#' @param server_timezone character string. The timezone code of the data in the server. Uploaded data will be converted to it. InfluxDB keeps data in 
#' \code{'UTC'} - currently only 'UTC' is supported.
#' @export

influxr_connection <- function (host = 'localhost', port = 8086, 
                                username = Sys.getenv('INFLUX_USERNAME'),
                                password = Sys.getenv('INFLUX_PASSWORD'), 
                                ssl=TRUE, ssl_verifypeer = TRUE,
                                timeout = 0, retries = 3, chunk_size = 2e4L, 
                                post_chunk_size = 5e4L, server_timezone = 'UTC') 
{
  
  Influx_client(host = host, port = port, username = username,password = password,
                 ssl = ssl, timeout = timeout, retries = retries, chunk_size=chunk_size,
                 post_chunk_size=post_chunk_size, ssl_verifypeer = ssl_verifypeer,
                 server_timezone = server_timezone)
  
}

#' Influx_client class
#' 
#' @keywords internal
#' @noRd 
Influx_client <- setClass(Class = 'Influx_client', 
                           slots = c(host = 'character', port = 'numeric', username = 'character',
                                     password = 'character',  ssl='logical', timeout = 'numeric',
                                     retries = 'numeric', chunk_size = 'numeric',
                                     post_chunk_size = 'numeric',
                                     ssl_verifypeer = 'logical',
                                     query_endpoint = 'character', 
                                     ping_endpoint = 'character', 
                                     write_endpoint = 'character',
                                     server_timezone = 'character')) 


#' Show Influx_client information.
#' 
#' @keywords internal
#' @noRd 
setMethod('show', 'Influx_client', function(object) {
  
  cat("InfluxDB connection.\n")
  cat('Host         ', object@host,'\n')
  cat('Port         ', object@port, '\n')
  cat('Username     ', object@username, '\n')
  cat('Timeout      ', object@timeout, '\n')
  cat('Retries      ', object@retries, '\n')
  cat('Chunk size   ', object@chunk_size, '\n')
  cat('SSL          ', ifelse(object@ssl, 'enabled', 'disabled'), '\n')
})


#' Initialize Influx_client classs.
#' 
#' @keywords internal
#' @noRd 
setMethod('initialize', 'Influx_client', function(.Object, ...) {
  .Object = callNextMethod()
  
  if(nchar(.Object@host) < 1) {
    stop(paste("Host can't be empty."))
  }
  
  
  if (.Object@ssl) {
    protocol = "https://"
  } else {
    protocol = "http://"
  }

  if (.Object@server_timezone != "UTC") {
    stop("Only 'UTC' server timezone is supported.")
  }
  
  base_url <- paste0(protocol, .Object@host, ":", .Object@port)
  .Object@query_endpoint = paste0(base_url, "/query")
  .Object@ping_endpoint = paste0(base_url, "/ping")
  .Object@write_endpoint = paste0(base_url, "/write")
  
  
  .Object})



#' Show Influx_client information.
#' @keywords internal
#' @noRd 
setMethod('summary', 'Influx_client', function(object) {
  cat("InfluxDB client object.\n")
  cat('Host         ', object@host,'\n')
  cat('Port         ', object@port, '\n')
  cat('Username     ', object@username, '\n')
  cat('Timeout      ', object@timeout, '\n')
  cat('Retries      ', object@retries, '\n')
  cat('Chunk size   ', object@chunk_size, '\n')
  cat('SSL          ', ifelse(object@ssl, 'enabled', 'disabled'), '\n')
  cat ('Querying instance basic info: \n')
  cat ('Databases in influxDB instance: \n')
  
  summary_query <- influxr_query(object, 'show databases', return_type = 'plain.text')
  print(summary_query)
})

