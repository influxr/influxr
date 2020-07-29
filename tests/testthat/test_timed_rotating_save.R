# test writing/ reading to disk
library(testthat)
library(data.table)
library(lubridate)

test_that('Creating timed rotating files.', {
  Sys.setenv(TZ = 'UTC')
  N = 1e6
  x <-
    data.table(
      time = Sys.time() + (1:N) * 10,
      A = rnorm(N),
      B = rnorm(N, 10),
      C = rnorm(N, 100)
    )
  .path <- tempdir()
  
  base_name_pattern = 'test.test_'
  save_timed_rotating_files(
    x,
    path = .path,
    base_file_name = base_name_pattern,
    rotation_interval = '12 hours',
    trunc_start = TRUE
  )
  
  ### Read a list of files
  read_back <-
    read_timed_rotating_files(
      path = .path,
      base_name_pattern = base_name_pattern,
      timestamp_format = '%Y%m%d%H'
    )
  
  options("digits.secs" = 6)
  expect_that(nrow(x), equals(nrow(read_back)))
  expect_that(all.equal(x$time, read_back$time), is_true())
  expect_that(x$time, is_a('POSIXct'))
  
  ### Remove files
  file.remove(list.files(
    path = .path,
    pattern = base_name_pattern,
    full.names = TRUE
  ))
})
