library(testthat)
library(influxr)
packageVersion("influxR")

test_that('Create influxDB client', {
  client <- influxr_connection()
  
  expect_that(client, is_a('Influx_client'))
  expect_that (client@host, equals('localhost'))
  expect_that (client@ssl, equals(TRUE))
})


tss_client <- influxr_connection(host = 'localhost', ssl = FALSE)

test_that('Ping tss influx server', {
  expect_that(ping(tss_client), equals(TRUE))
})



test_that('Upload 1e4 rows and verify.', {

  n = 1e5
  dummy_upload <-
    data.frame(
      time = Sys.time() + 1:n,
      Temp = round(rnorm(n) * 50, digits = 2),
      H = rnorm (n),
      DOY = floor(runif(n, 0, 365)),
      Code = replicate(n, paste0(sample(LETTERS, 4), collapse = ''))
    )

    
  tags <-
    c(
      station = 'Imtan',
      instrument = 'BME280',
      level = 15,
      empty_tag =''
    )
  
  
  # add random NAs
  dummy_upload[sample(seq_len(NROW(dummy_upload)), size = floor(n * .1)), 'Temp'] <- NA
  dummy_upload[sample(seq_len(NROW(dummy_upload)), size = floor(n * .1)), 'H'] <- NA
  dummy_upload[sample(seq_len(NROW(dummy_upload)), size = floor(n * .1)), 'DOY'] <- NA
  dummy_upload[sample(seq_len(NROW(dummy_upload)), size = floor(n * .1)), 'Code'] <- NA
  dummy_upload[50, -1] <- NA # One row with all NA's

  
  # Drop measurement if it exists
  q <- influxr_query(tss_client, 'drop measurement test', database = 'db_test')
  
  check_measurement <- influxr_select(tss_client, 'db_test', 
                measurement = 'test', group_by = NULL, aggregation = NULL, limit = 1)
  
  expect_null(check_measurement)

  # Ready to upload
  res <- influxr_write(
    x = dummy_upload,
    client = tss_client,
    measurement = 'test',
    database = 'db_test',
    precision = 'ms',
    tags = tags,
    timestamp = 1, verbose = TRUE)
  
 expect_true(res)
 
 # Get data
  data_back <- influxr_select(tss_client, database = 'db_test',
                      measurement = 'test', 
                      fields = c('Temp', 'H', 'DOY', 'Code'), aggregation = NULL, verbose = T, limit = NULL)
  
  expect_length(data_back, 5)
  
  # Equal number of non-nas rows
  rows_not_all_NA <- apply(dummy_upload[,-1], 1, function(x) any(!is.na(x)))
  expect_equal(NROW(data_back), sum(rows_not_all_NA))
  
  # convert time back to local time zome
  attr(data_back$time, 'tzone') <- Sys.timezone()
  expect_equivalent(dummy_upload[rows_not_all_NA,'time'], data_back$time)
  
  # Expect the same numbers for fields
  expect_equal(dummy_upload[rows_not_all_NA,'Temp'], data_back$Temp)
  
  expect_equal(dummy_upload[rows_not_all_NA,'H'], data_back$H)
  
  expect_equal(dummy_upload[rows_not_all_NA,'DOY'], data_back$DOY)
  
  # Replace empty strings with NAs
  data_back$Code[data_back$Code == ''] <- NA
  
  # check equality
  expect_equal(as.character(dummy_upload[rows_not_all_NA,'Code']), data_back$Code)
  
  # Clean 
  influxr_query(tss_client, query = 'drop measurement test', database = 'db_test', return_type = 'plain.text')

})
