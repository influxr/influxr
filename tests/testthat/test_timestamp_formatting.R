library(testthat)
library(lubridate)



test_that('Test time stamp formatting for POSIXct', code = {
  x <- data.frame(datetime = Sys.time() + 1:100, T = rnorm(100))
  formatted_ts <-
    format_timestamp(x, 1, timestamp_format = '')
  expect_equal(x$datetime, formatted_ts$time)
})

test_that('Test time stamp formatting for POSIXct separate', code = {
  x <- data.frame(ID = 1:100, T = rnorm(100))
  ts <- Sys.time() + 1:100
  formatted_ts <-
    format_timestamp(x, timestamp = ts, timestamp_format = '')
  expect_equal(ts, formatted_ts$time)
})


test_that('Test time stamp formatting for character separate', code = {
  x <- data.frame(ID = 1:100, T = rnorm(100))
  ts_format <- '%Y-%m-%d %H:%M:%OS'
  original_ts <- Sys.time() + 1:100
  ts <- format(original_ts, format = ts_format)
  formatted_ts <-
    format_timestamp(x, timestamp = ts, timestamp_format = ts_format, 
                     source_tz = Sys.timezone(), server_tz = Sys.timezone())
  expect_equal(original_ts, formatted_ts$time)
})

test_that('Test time stamp formatting for character inside', code = {
  ts_format0 <- '%Y-%m-%d %H:%M:%OS'
  ts_format <- '%Y-%m-%d %H:%M:%OS4'
  original_ts <- Sys.time() + 1:100
  ts <- format(original_ts, format = ts_format)
  x <- data.frame(timedate = ts, ID = 1:100, T = rnorm(100))
  formatted_ts <- format_timestamp(x, timestamp = 1, timestamp_format = ts_format0, 
                  source_tz = Sys.timezone(), server_tz = Sys.timezone())
  expect_equal(original_ts, formatted_ts$time)
})


test_that('Test time stamp formatting for character multiple columns', code = {
  ts_format0 <- '%Y-%m-%d %H:%M:%OS'
  ts_format1 <- '%Y-%m-%d'
  ts_format2 <-  '%H:%M:%OS4'
  original_ts <- Sys.time() + 1:100
  ts1 <- format(original_ts, format = ts_format1)
  ts2 <- format(original_ts, format = ts_format2)
  x <- data.frame(date = ts1, time = ts2, ID = 1:100, T = rnorm(100))
  formatted_ts <- format_timestamp(x, timestamp = c(1,2), timestamp_format = ts_format0, 
              source_tz = Sys.timezone(), server_tz = Sys.timezone())
  expect_equal(original_ts, formatted_ts$time)
})
