#' Selects data points from an InfluxDB database.
#' 
#' @description `influxr_select` returns the result of a query. 
#' @details other options that can be passed to the function are \code{ignore_names} and \code{ignore_tags}.
#' @param client Influx_client object created with \code{\link{influxr_connection}}
#' @param database character string. Database name.
#' @param measurement character string. InfluxDb measurement name.
#' @param fields character vector, \code{NULL} or "*". Fields to be 
#'      selected eg. \code{c('T','Humidity', 'w')}. \code{NULL} or '*' will select all fields.
#' @param from `POSIX` datetime or character in ISO8601 format which will be parsed to date time
#' eg. '2017-12-31 14:00:00' or simply '2017' for the start of 2017. if \code{NULL} all values will be selected.
#' @param to POSIX datetime or character in ISO8601 format which will be parsed to date time
#' eg. '2017-12-31 14:00:00' or simply '2017' for the start of 2017. if \code{NULL} all values will be selected.
#'  Note: influxr_select returns data points in the range [from, to[, ie: where time >= from and time < to.
#' @param tags named character vector. Only data points tagged with provided
#' tags will be fetched. eg. \code{c(station = 'Hainich', level = 3, method = 'EC')}.
#' @param group_by character string. How data points should be grouped when an aggregation function is specified, 
#' possible values are \code{'time(1h)'} to aggregate by time, or any other time interval or other field.
#'  If \code{NULL} no grouping will happen.
#' @param aggreagation character string. Aggregation function to be used. Currently supported
#' functions are \code{'MEAN','COUNT','DISTINCT', 'INTEGRAL','MEDIAN','MODE', 'SPREAD','STDDEV','SUM', 'FIRST','LAST', 'MIN', 'MAX'}
#' if \code{NULL} no aggregation is performed.
#' @param fill character string. How should empty datapoints be filled, default to 'null' other 
#' possible options are \code{"linear", "none", "null" or "previous"}
#' @param limit numeric. How many data points should the result be limited to. If \code{NULL} or \code{Inf}
#' everything will be returned. Default to one million data points.
#' @param desc logical, not emplemented yet.
#' @param extra_conditions Pass extra conditions to the query.
#' @param ... passed to `influxr_query`. e.g. `write_to_file = '~/test.csv'`
#' @examples
#'
#' # Read data from InfluxDB instance and store the result in timed rotating 
#  # files with an interval of 1 week for each file, the files will have 
#  # names suffixed with date (e.g. cpu_20170709)
#'
#' \dontrun{
#' 
#' influxr_select(
#'   influxdb_connection,
#'   database,
#'   measurement = 'cpu',
#'   tags = c(level = 1, station = 'work'),
#'   verbose = TRUE,
#'   from = '2017-07-10 12:00:00',
#'   to = '2017-08-10 12:00:00',
#'   write_handler = save_timed_rotating_files,
#'   path = my_path,
#'   base_file_name = 'cpu_',
#'   rotation_interval = '1 week',
#'   trunc_start = TRUE
#' )
#'}
#' 
#' @export
influxr_select <- function (client,
                    database, 
                     measurement = '',
                     fields = '*',
                     from = NULL,
                     to = NULL, 
                     tags = NULL,
                     group_by = NULL,
                     aggregation = NULL,
                     fill = NULL,
                     missing = c(-9999.00, -9999.99, -9999.9999),
                     missing_replacement = NA,
                     limit = 1e6,
                     desc = FALSE,
                     extra_conditions = NULL,
                     ...
                     ) {
   
    
    extra_args <- list(...)
  
    if(is.null(fill)) { fill <- 'null' }
    if(is.null(fields)) {fields <- NA_character_}
    if(is.null(group_by)) {group_by <- NA_character_}
    if(is.null(aggregation)) {aggregation <- NA_character_}
    if(is.null(tags)) {tags <- NA_character_}
    if(is.null(limit)) {limit <- NA_integer_}
  
  
    # Remove NA's from fields
    fields <- fields[!is.na(fields)]
    
    if(length(fields) < 1) {
      stop("No fields were sellected")
    }
    
  
  
  
  # Show warning when a field name will be ignore and no explicit ignore argument is passed
  name_warning <- any(fields == 'name') && is.null(extra_args$ignore_name)
  tags_warning <- any(fields == 'tags') && is.null(extra_args$ignore_tags)
  
  if(name_warning || tags_warning)
    {
      warning('The fields "name" and "tag" are usually ignored when returned from the database, you can override this behaviour by 
              passing ignore_name = FALSE or ignore_tags = FALSE to this function.', call. = FALSE)
    }
  
  
    aggregation_function <- match.arg(toupper(aggregation), 
                                      choices = c('MEAN','COUNT','DISTINCT',
                                                  'INTEGRAL','MEDIAN','MODE',
                                                  'SPREAD','STDDEV','SUM',
                                                  'FIRST','LAST', 'MIN', 'MAX', NA_character_), 
                                      several.ok = FALSE)

    fill_option <- match.arg(fill, choices = c("linear", "none", "null", "previous" ), 
                             several.ok = FALSE)
    
    from <- influxr_parse_datetime (from)
    to <- influxr_parse_datetime (to)
    
    query_options <-
      queryOptions(
        measurement = measurement ,
        retention_policy = NA_character_,
        fields = fields ,
        from = from ,
        to = to ,
        tags = tags ,
        group_by = group_by ,
        aggregation = aggregation_function ,
        fill = fill_option,
        limit = limit ,
        desc = desc
        
      )
    
    query <- build_query(query_options)
    
    if(!is.null(extra_args$verbose) && extra_args$verbose) {
      cat('Query built: \n', query, '\n')
    }
    query_result <-  influxr_query(client, query = query, 
                                  database = database, 
                                  missing = missing, 
                                  missing_replacement = missing_replacement,...)
    
    return(query_result)
    
}




#' Build influxQL query statement.
#' @keywords internal
#' @noRd 
build_query <- function(query_options) {
  
  #TODO: distinguish between aggregation and selection functions,
  # assert the group_by clause when using aggregation functions.
  
  op <- options("useFancyQuotes")
  options(useFancyQuotes = FALSE)
  fields <- query_options@fields
  
  all_fields <- FALSE # When fields  != '*'
  
  if (length(fields) == 1 &&  (is.na(fields) || fields == '*')) {
    all_fields <- TRUE
    fields <- '*'
  } else {
      fields <- dQuote(fields)
  }
  
    group_by_clause = NULL
    if (!is.na(query_options@aggregation)) {
      if (all_fields) {
        closing_statement <- ')'
      } else{
        closing_statement <- paste0 (') as ', fields)
      }
      
      fields <-
        paste0(query_options@aggregation, '(', fields, closing_statement)
      if (!is.na(query_options@group_by)) {
        group_by_clause <- paste('GROUP BY', query_options@group_by)
      }
    }
    
  fields_clause <- paste(fields, collapse = ',')
  
  query <- paste("SELECT", fields_clause, "FROM", dQuote(query_options@measurement))
  
  where_clause = NULL
  
  if(!is.na(query_options@from)) {
    where_clause <- c(where_clause, paste(' time >=', influxr_time64String (query_options@from)))
  }
  
  if(!is.na(query_options@to)) {
    where_clause <- c(where_clause, paste(' time <', influxr_time64String(query_options@to)))
  }
  
  if(!all(is.na(query_options@tags))) {
    tags <-  query_options@tags[!is.na(query_options@tags)]
    where_clause <- c(where_clause, paste(dQuote(names(tags)), sQuote(tags), sep = '='))
  }
  
  
  if(!is.null(where_clause)) {
    query <- paste (query, paste('WHERE', paste(where_clause, collapse = ' AND ')))
  }
  
  if(!is.null(group_by_clause)) {
    fill_clause <- paste0('fill(', query_options@fill,')')
    query <- paste(query, group_by_clause, fill_clause)
  }

  if(!is.na(query_options@limit) && query_options@limit != Inf) {
    query <- paste(query, 'LIMIT', toString(as.integer(query_options@limit)))
  }
  
  options(op)
  return(query)

  
}




#' Class to contain query options.
#' 
#' @keywords internal
#' @noRd 
queryOptions <- setClass('queryOptions', slots = c(measurement = 'character', fields = 'character',
                                     retention_policy = 'character', from = 'POSIXct',
                                     to ='POSIXct', tags = 'character', group_by = 'character',
                                     aggregation = 'character',
                                     fill = 'character',
                                     limit = 'numeric',
                                     desc = 'logical'))
