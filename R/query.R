#'  Perform a query on InfluxDB instance.
#' 
#' \code{influxr_query}
#' @param client InfluxDB connection object, created with \code{\link{influxr_connection}}
#' @param query character. Query statement in influxDB query language `InfluxQL`
#' @param databse character, representing the database to name to run the query against.
#' @param precision character. Timestamp precision of returned results, defaults to milliseconds with a
#' range of options from nanosecond to hour.
#' @param return_type character. Type of returned result. Default to 'data.table'.
#' @param col_names logical. Whether to return the column names from influxDB variable names.
#' @param missing Vector. Values to be considerd as missing values.
#' @param missing_replacement What to replace missing values with.
#' @param ignore_name logical. Whether to remove the name variable from the returned result from influx.
#' @param ignore_tags logical. Whether to keep influx measurement tags as variables in the returend result.
#' @param write_hanler function. A function to handle writing to disk. If provided, query results to be written
#' to disk will be passed
#' to the function under an argument named \code{x}. Chunk number will be passed as an argument named
#' \code{chunk_number}.
#' the build in function timed_rotating_file can be used as write handler. default is NULL.
#' @param ... Further arguments to be passed to the write_handler function.
#' @export
#' @details Note on time zones: the time stamp of the returned query will be converted from the server
#' timezone ('UTC') to the local system
#' timezone obtained by \code{Sys.timezone()} unless the environment variable 'TZ' is not empty
#' then its value will be applied to the 
#' returned timestamp.
influxr_query <- function (client, query = '',
                   database = '',
                   precision = c('ms','ns', 'u', 's', 'm', 'h'),
                   return_type = c('data.table', 'data.frame', 'plain.text', 'xts'),
                   col_names = TRUE,
                   missing = c(-9999.00, -9999.99, -9999.9999),
                   missing_replacement = NA,
                   ignore_name = TRUE,
                   ignore_tags = TRUE,
                   write_handler = NULL,
                   verbose = FALSE,
                   ...) {
  
  write_handler_args <- list(...)
  
  if(!is.null(write_handler)) {
    write_handler <- match.fun(write_handler)
  }

  if(database == '' || is.na(database) || is.null(database)) {
    stop('Database name was not provided.\n')
  }
  
  query_class <-  new(
    'Influx_query',
    query = query,
    database = database,
    precision = match.arg(precision),
    return_type = match.arg(return_type),
    col_names = col_names,
    missing = missing,
    missing_replacement = missing_replacement,
    ignore_name = ignore_name,
    ignore_tags = ignore_tags,
    write_handler = write_handler,
    write_handler_args = write_handler_args
  )
  
  influxr_fetch_query(client, query_class, verbose = verbose)
}


#' InfluxQuery S4 Class
#'
#' @slot query  Query statement as character string.
#' @slot database Database name.
#' @slot precision Time stamp precision.
#' @slot return_type Query return type.
#' @slot col_names Whether to fetch column names.
#' @slot qmethod query method
#' @slot ignore_name Whether to ignore names
#' @slot ignore_tags Whehter to ignore tags
#' 
#' @keywords internal
#' @noRd 
setClass(
  Class = 'Influx_query',
  slots = c(
    query = 'character',
    database = 'character',
    precision = 'character',
    return_type = 'character',
    col_names = 'logical',
    missing = 'ANY',
    missing_replacement = 'ANY',
    qmethod = 'character',
    ignore_name = 'logical',
    ignore_tags = 'logical',
    write_handler = 'ANY',
    write_handler_args = 'list'
  )
)

setMethod('show', 'Influx_query', function(object) {
  cat("InfluxDB query object.\n")
  cat(paste('Query         \t', object@query, '\n'))
  cat(paste('From database \t', object@database, '\n'))
  cat(paste('Precision     \t', object@precision, '\n'))
})

