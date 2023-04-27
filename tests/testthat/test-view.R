context("rstudio viewer")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

test_that("Check if Athena list object types is formatted correctly",{
  skip_if_no_env()

  con <- dbConnect(athena())
  
  output <- noctua:::AthenaListObjectTypes.default(con)
  
  expect_equal(
    output, 
    list(
      catalog = list(
        contains = list(
          schema = list(
            contains = list(
              table = list(contains = "data"),
              # view = list(contains = "data") # information_schema.tables returns all TableType as "BASE TABLE"
            )
          )
        )
      )
    )
  )
})

test_that("Check if Athena list object is formatted correctly",{
  skip_if_no_env()

  con <- dbConnect(athena())
  
  output1 <- noctua:::AthenaListObjects.AthenaConnection(con)
  output2 <- noctua:::AthenaListObjects.AthenaConnection(con, schema = "default")
  
  expect_true(inherits(output1, "data.frame"))
  expect_equal(names(output1), c("name", "type"))
  
  expect_true(inherits(output2, "data.frame"))
  expect_equal(names(output2), c("name", "type"))
})

test_that("Check computer host name output type",{
  skip_if_no_env()
  
  con <- dbConnect(athena())
  out <- noctua:::computeHostName(con)
  expect_true(is.character(out))
})

test_that("Check computer display name output type",{
  skip_if_no_env()
  
  con <- dbConnect(athena())
  
  out <- noctua:::computeDisplayName(con)
  expect_true(is.character(out))
})

test_that("Check if Athena list column formatting",{
  skip_if_no_env()

  con <- dbConnect(athena())
  
  output1 <- noctua:::AthenaListColumns.AthenaConnection(con, table = "iris", catalog = "AwsDataCatalog", schema = "default")
  
  dbDisconnect(con)
  
  expect_true(inherits(output1, "data.frame"))
  expect_equal(names(output1), c("name", "type"))
  
  expect_null(noctua:::AthenaListColumns.AthenaConnection(con, table = "iris", catalog = "AwsDataCatalog", schema = "default"))
})

test_that("Check if Athena list column formatting",{
  skip_if_no_env()
  
  con <- dbConnect(athena())
  
  output1 <- noctua:::AthenaTableTypes(con)
  output2 <- noctua:::AthenaTableTypes(con, schema = "default")
  output3 <- noctua:::AthenaTableTypes(con, schema = "default", name="iris")
  
  expect_true(inherits(output1, "character"))
  expect_true(inherits(output2, "character"))
  expect_true(inherits(output3, "character"))
})

test_that("Check if AthenaDatabase formatting is correct",{
  skip_if_no_env()
  
  con <- dbConnect(athena())
  
  output1 <- noctua:::AthenaDatabase(con, "AwsDataCatalog")

  expect_true(inherits(output1, "character"))
})

test_that("Check if AthenaDatabase formatting is correct",{
  skip_if_no_env()
  
  con <- dbConnect(athena())
  
  output1 <- noctua:::AthenaPreviewObject.AthenaConnection(con, 10, table = "iris")
  output2 <- noctua:::AthenaPreviewObject.AthenaConnection(con, 10, table = "iris", schema = "default")
  
  expect_true(inherits(output1, "data.frame"))
  expect_true(inherits(output2, "data.frame"))
})

test_that("Check if AthenaPreviewObject formatting is correct",{
  skip_if_no_env()
  
  con <- dbConnect(athena())
  
  output1 <- noctua:::AthenaPreviewObject.AthenaConnection(con, 10, table = "iris")
  output2 <- noctua:::AthenaPreviewObject.AthenaConnection(con, 10, table = "iris", schema = "default")
  
  expect_true(inherits(output1, "data.frame"))
  expect_true(inherits(output2, "data.frame"))
})

test_that("Check if AthenaConnectionIcon outputs correct path",{
  skip_if_no_env()
  
  con <- dbConnect(athena())
  
  output1 <- noctua:::AthenaConnectionIcon(con)
  
  expect_true(file.exists(output1))
})

test_that("Check if AthenaConnectionActions output format is correct",{
  skip_if_no_env()
  
  con <- dbConnect(athena())
  
  output1 <- noctua:::AthenaConnectionActions(con)
  
  expect_true(is.list(output1))
})

test_that("Check if on_connection_opened runs correctly",{
  skip_if_no_env()
  
  con <- dbConnect(athena())
  
  output1 <- on_connection_opened(con)
  
  expect_null(output1)
})

test_that("Check if on_connection_opened runs correctly",{
  x1 <- list(TableType = "dummy",
            Name = "dummy")
  x2 <- list(Type = "dummy",
             Name = "dummy")
  x3 <- list(Name = "dummy")
  
  exp1 <- "dummy"
  names(exp1) <- "dummy"
  exp2 <- ""
  names(exp2) <- "dummy"
  
  expect_equal(noctua:::TblMeta(x1), exp1)
  expect_equal(noctua:::ColMeta(x2), exp1)
  expect_equal(noctua:::TblMeta(x3), exp2)
  expect_equal(noctua:::ColMeta(x3), exp2)
})
