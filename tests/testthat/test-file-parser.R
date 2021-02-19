context("file_parser")

library(data.table)
library(dplyr)

test_data <- function(N = 10000L, seed = 142L){
  set.seed(seed)
  data.table(
    id=1:N,
    original=sapply(1:N, function(x) paste(sample(letters, sample(5:10)), collapse = ",")),
    json = jsonlite::toJSON(iris[1,1:2]))
}

output_test_data <- function(N = 10000L, temp_file){
  dt_list <- test_data(N)
  dt_list[, raw := lapply(original, charToRaw)]
  dt_list[, raw_string := sapply(raw, function(x) paste(x, collapse = " "))]
  fwrite(x = dt_list[,.(id, original, raw_string, json)],
         file = temp_file,
         quote = T)
}

output_test_error_data <- function(temp_file, N = 10000L, seed = 142L){
  set.seed(seed)
  dt <- data.table(
    id=1:N,
    original = rep("helloworld", N),
    raw_string = rep("helloworld", N),
    json = rep("[Name = Bob]", N))
  fwrite(dt,
         temp_file,
         quote = T)
}

data_type <- list(list(Name = "id",
                       Type = "integer"),
                  list(Name = "original",
                       Type = "string"),
                  list(Name = "raw_string",
                       Type = "varbinary"),
                  list(Name = "json",
                       Type = "json"))

method_1 <- "method"
class(method_1) <- "athena_data.table"

method_2 <- "method"
class(method_2) <- "athena_vroom"

test_that("Check if json has been correctly parser under chunk method",{
  size <- 1e5
  test_file <- tempfile()
  output_test_data(size, test_file)
  
  noctua::noctua_options(json="auto")
  noctua::noctua_options(binary="character")
  
  dt1 <- noctua:::athena_read.athena_data.table(
    method_1,
    test_file,
    data_type)
  
  dt2 <- noctua:::athena_read.athena_vroom(
    method_2,
    test_file,
    data_type)
  
  expect_equal(jsonlite::toJSON(dt1$json[[1]], auto_unbox = T), jsonlite::toJSON(iris[1,1:2]))
  expect_equal(jsonlite::toJSON(dt2$json[[1]], auto_unbox = T), jsonlite::toJSON(iris[1,1:2]))
  unlink(test_file)
})

test_that("Binary and json conversion",{
  size <- 1e1
  test_file <- tempfile()
  output_test_data(size, test_file)
  
  noctua::noctua_options(json="auto")
  noctua::noctua_options(binary="raw")
  
  dt1 <- noctua:::athena_read.athena_data.table(
    method_1,
    test_file,
    data_type)
  
  dt2 <- noctua:::athena_read.athena_vroom(
    method_2,
    test_file,
    data_type)
  
  dt1[, string := sapply(raw_string, rawToChar)]
  
  dt2 <- dt2 %>% mutate(string = sapply(raw_string, rawToChar))
  
  expect_equal(jsonlite::toJSON(dt1$json[[1]], auto_unbox = T), jsonlite::toJSON(iris[1,1:2]))
  expect_equal(jsonlite::toJSON(dt2$json[[1]], auto_unbox = T), jsonlite::toJSON(iris[1,1:2]))
  
  expect_equal(dt1$original, dt1$string)
  expect_equal(dt2$original, dt2$string)
  unlink(test_file)
})


test_that("Check in conversion is turned off",{
  size <- 1e1
  test_file <- tempfile()
  output_test_data(size, test_file)
  
  noctua::noctua_options(json="character")
  noctua::noctua_options(binary="character")
  
  dt1 <- noctua:::athena_read.athena_data.table(
    method_1,
    test_file,
    data_type)
  
  dt2 <- noctua:::athena_read.athena_vroom(
    method_2,
    test_file,
    data_type)
  
  expect_equal(dt1$json[[1]], as.character(jsonlite::toJSON(iris[1,1:2])))
  expect_equal(dt2$json[[1]], as.character(jsonlite::toJSON(iris[1,1:2])))
  
  expect_true(is.character(dt1$original))
  expect_true(is.character(dt2$original))
  unlink(test_file)
})

test_that("Custom json parser",{
  size <- 1e1
  test_file <- tempfile()
  output_test_data(size, test_file)
  
  noctua::noctua_options(json=jsonlite::fromJSON)
  noctua::noctua_options(binary="character")
  
  dt1 <- noctua:::athena_read.athena_data.table(
    method_1,
    test_file,
    data_type)
  
  dt2 <- noctua:::athena_read.athena_vroom(
    method_2,
    test_file,
    data_type)
  
  expect_equal(dt1$json[[1]], iris[1,1:2])
  expect_equal(dt2$json[[1]], iris[1,1:2])
  unlink(test_file)
})

test_that("Custom json parser",{
  size <- 1e1
  test_file <- tempfile()
  output_test_data(size, test_file)
  
  noctua::noctua_options(json=jsonlite::fromJSON)
  noctua::noctua_options(binary="character")
  
  dt1 <- noctua:::athena_read.athena_data.table(
    method_1,
    test_file,
    data_type)
  
  dt2 <- noctua:::athena_read.athena_vroom(
    method_2,
    test_file,
    data_type)
  
  expect_equal(dt1$json[[1]], iris[1,1:2])
  expect_equal(dt2$json[[1]], iris[1,1:2])
  unlink(test_file)
})

test_that("Check if variable is returns as character when failed to convert",{
  test_file <- tempfile()
  output_test_error_data(test_file, 1)
  
  noctua::noctua_options(json="auto")
  noctua::noctua_options(binary="raw")
  
  expect_warning(
    dt <- noctua:::athena_read.athena_data.table(
      method_1,
      test_file,
      data_type))
  
  expect_true(is.character(dt$raw_string))
  expect_true(is.character(dt$json))
  unlink(test_file)
})

test_that("Check accepted bigint options",{
  BigInt <- c("integer64", "integer", "numeric", "character")
  for(i in BigInt){
    expect_invisible(noctua::noctua_options(bigint=i))
  }
  expect_error(noctua::noctua_options(bigint="character2"))
})

test_that("Check accepted json options",{
  Json <- c("auto", "character")
  for(i in Json){
    expect_invisible(noctua::noctua_options(json=i))
  }
  
  expect_invisible(noctua::noctua_options(json=jsonlite::fromJSON))
  expect_error(noctua::noctua_options(json=1))
})
