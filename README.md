# influxr  
An R interface to [InfluxDB](https://www.influxdata.com/time-series-platform/influxdb/) **1.8.x** time series databases.


<!-- vim-markdown-toc GFM -->

* [Current features](#current-features)
* [Installation](#installation)
    * [Installing system dependencies](#installing-system-dependencies)
        * [On Linux](#on-linux)
        * [On Windows](#on-windows)
    * [Installing R dependencies](#installing-r-dependencies)
    * [Installing the `influxr` package](#installing-the-influxr-package)
* [Getting help](#getting-help)
    * [Examples](#examples)
        * [Create a connection object using `influxr_connection` to be used for reading and writing from and to an influxDB database instance](#create-a-connection-object-using-influxr_connection-to-be-used-for-reading-and-writing-from-and-to-an-influxdb-database-instance)
        * [Reading from influxDB using `influxr_select`](#reading-from-influxdb-using-influxr_select)
        * [Writing to influxDB using `influxr_write`](#writing-to-influxdb-using-influxr_write)
        * [Getting data back using `influxr_select`](#getting-data-back-using-influxr_select)
        * [Deleting data from the database using `influxr_query`](#deleting-data-from-the-database-using-influxr_query)

<!-- vim-markdown-toc -->


## Current features
* Read time series data from InfluxDB databases.
* Write time series data from InfluxDB databases.
* Manage connections to InfluxDB databases.
* Attach meta data to your time series for uploading to InfluxDB databases.
* Upload R objects, single or multiple files (possibly all files in a directory or even recursively in whole directory trees) to InfluxDB databases.
* Perform any query provided with the InfluxDB query language on InfluxDB databases directly from within R.
* High performance data transfer from and to InfluxDB databases for high volume and high resolution time series data, featuring very fast ways for reading files from disk (function fread from the data.table package) and intelligent management of automatic sequential transfer (automatic and transparent chunking of data during transfer).
* Transfer huge data sets through chunking, avoiding InfluxDB limits on single read / write queries.
* Write InfluxDB data directly to files on disk including timed rotating files.
* Convert between different time stamp formats between R and InfluxDB
* Handle time series, time stamps and time zones correctly, avoiding truncation, including high resolution time series down to nano seconds.
* Manage handling of different missing values representations in R and InfluxDB.
* Check and possibly fix the format of your files prior to uploading to InfluxDB, with options to recover broken files.


## Installation

### Installing system dependencies

#### On Linux  
On Ubuntu, you may need to install the following system packages
```
libssl-dev, libcurl4-openssl-dev
```
#### On Windows  
Nothing to do here.

### Installing R dependencies  
Install required packages in **R** with

```r
install.packages(c('data.table', 'xts', 'curl', 'httr', 'devtools'))
```

### Installing the `influxr` package  
Install the latest version of the `influxr` package in **R** with

```r
devtools::install_github("influxr/influxr")
```

## Getting help  

Type one of the following in ***R***  

```r
help(influxr)
?influxr
?influxr_connection
?influxr_select
?influxr_write
?influxr_query
?influxr_list_files

```

### Examples

#### Create a connection object using `influxr_connection` to be used for reading and writing from and to an influxDB database instance

```r
# Load the influxr package
library(influxr)

# Define a client to hold connection parameters to your server running the influxDB database instance. Here you can also provide username and password if authentication is needed.
tss_client <- influxr_connection(host = 'localhost', ssl = FALSE)

# Test the connection
ping(tss_client)

```

#### Reading from influxDB using `influxr_select`

```r
# Let's get the following series 
# "fluxes,gasanalyzer=LI-6262,level=2,method=EC,software=Eddypro,sonic=Gill-R3,station=Hainich"

ec_fluxes <- influxr_select(
    tss_client,                         
      database = 'db_climate',          
      measurement = 'concentrations',
      fields = c('CO2', 'H2O', 'CH4'),
      from = '2019',
      to = '2019-12-31',
      tags = c(station = 'Hainich', level = 2),
      group_by = 'time(30m)',
      aggregation = 'mean',
      fill = 'none',
      verbose = TRUE
)

# Display the results
print(ec_fluxes)
```

#### Writing to influxDB using `influxr_write`  

```r
# Generate random data with different data types
# Number of data points to generate
n = 1e5

# Create dummy data.frame for uploading
  dummy_upload <-
    data.frame(
      time = Sys.time() + 1:n,
      Temp = round(rnorm(n) * 50, digits = 2),
      H = rnorm (n),
      DOY = floor(runif(n, 0, 365)),
      Code = replicate(n, paste0(sample(LETTERS, 4), collapse = ''))
    )

    
# Define meta data, knows as tags, for uploading to influxDB
  tags <-
    c(
      station = 'Hainich',
      instrument = 'uSonic-3',
      level = 1,
      empty_tag =''
    )
  

# Upload the data with the meta data
  res <- influxr_write(
    x = dummy_upload,
    client = tss_client,
    measurement = 'test',
    database = 'db_test',
    precision = 'ms',
    missing = c(NA, -9999),
    tags = tags,
    timestamp = 1, verbose = TRUE)
  
 
```
 
#### Getting data back using `influxr_select` 

```r
data_back <- influxr_select(tss_client, database = 'db_test', measurement = 'test', 
                            fields = c('Temp', 'H', 'DOY', 'Code'), aggregation = NULL,
                            verbose = T, limit = NULL)
```

#### Deleting data from the database using `influxr_query`  

```r
q <- influxr_query(client = tss_client, query = 'drop measurement test', database = 'db_test')
```

