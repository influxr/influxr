#' \code{influxr} interfacing with InfluxDB database.
#' 
#' Features:
#' \itemize{
#' \item{Read time series data from InfluxDB databases.}
#' \item{Write time series data from InfluxDB databases.}
#' \item{Manage connections to InfluxDB databases.}
#' \item{Attach meta data to your time series for uploading to InfluxDB databases.}
#' \item{Upload R objects, single or multiple files (possibly all files in a directory or even recursively in whole directory trees) to InfluxDB databases.}
#' \item{Perform any query provided with the InfluxDB query language on InfluxDB databases directly from within R.}
#' \item{High performance data transfer from and to InfluxDB databases for high volume and high resolution time series data, featuring very fast ways for reading files from disk (function fread from the data.table package) and intelligent management of automatic sequential transfer (automatic and transparent chunking of data during transfer).}
#' \item{Transfer huge data sets through chunking, avoiding InfluxDB limits on single read / write queries.}
#' \item{Write InfluxDB data directly to files on disk including timed rotating files.}
#' \item{Convert between different time stamp formats between R and InfluxDB.}
#' \item{Handle time series, time stamps and time zones correctly, avoiding truncation, including high resolution time series down to nano seconds.}
#' \item{Manage handling of different missing values representations in R and InfluxDB.}
#' \item{Check and possibly fix the format of your files prior to uploading to InfluxDB, with options to recover broken files.}}
#'
#' @docType package
#' @name influxr
#' @importFrom curl curl handle_data handle_setform handle_setheaders handle_setopt new_handle
#' @importFrom data.table data.table fread fwrite rbindlist set
NULL
