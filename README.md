
# noctua

[![Project Status: Active – The project has reached a stable, usable
state and is being actively
developed.](https://www.repostatus.org/badges/latest/active.svg)](https://www.repostatus.org/#active)
[![CRAN_Status_Badge](http://www.r-pkg.org/badges/version-ago/noctua)]https://CRAN.R-project.org/package=noctua)
![downloads](https://cranlogs.r-pkg.org/badges/noctua)

The goal of the `noctua` package is to provide a DBI-compliant interface
to Amazon’s Athena (<https://aws.amazon.com/athena/>) using [`paws`](https://github.com/paws-r/paws) SDK.
This allows for an efficient, easy setup connection to Athena using the
`paws` SDK as a driver.

**NOTE:** *Before using `noctua` you must have an aws account or have
access to aws account with permissions allowing you to use Athena.*

## Why is the package called noctua

[Athena/Minerva](https://en.wikipedia.org/wiki/Athena) is the Greek/Roman god of wisdom, handicraft, and warfare. One of the main symbols for Athena is the Owl. `Noctua` is the latin word for Owl.

## Installation:

To install `noctua` you can get it from CRAN with:
``` r
install.packages("noctua")
```

Or to get the development version from Github with:
```r
remotes::install_github("dyfanjones/noctua")
```


## Connection Methods

### Hard Coding

The most basic way to connect to AWS Athena is to hard-code your access key 
and secret access key. However this method is **not** recommended as your 
credentials are hard-coded.
```r
library(DBI)

con <- dbConnect(noctua::athena(),
                aws_access_key_id='YOUR_ACCESS_KEY_ID',
                aws_secret_access_key='YOUR_SECRET_ACCESS_KEY',
                s3_staging_dir='s3://path/to/query/bucket/',
                region_name='eu-west-1')
```

### AWS Profile Name

The next method is to use profile names set up by AWS CLI or created manually 
in the `~/.aws` directory. To create the profile names manually please refer 
to: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html.

##### Setting up AWS CLI

`noctua` is compatible with AWS CLI. This allows your aws credentials to
be stored and not be hard coded in your connection.

To install AWS CLI please refer to:
<https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html>,
to configure AWS CLI please refer to:
<https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html>

Once AWS CLI has been set up you will be able to connect to Athena by
only putting the `s3_staging_dir`.

Using default profile name:
``` r
library(DBI)
con <- dbConnect(noctua::athena(),
                 s3_staging_dir = 's3://path/to/query/bucket/')
```
Connecting to Athena using profile name other than `default`.
``` r
library(DBI)
con <- dbConnect(noctua::athena(),
                 profile_name = "your_profile",
                 s3_staging_dir = 's3://path/to/query/bucket/')
```

## Assuming ARN Role for connection

Another method in connecting to Athena is to use Amazon Resource Name (ARN) role.

Setting credentials in environmental variables:
```r
library(noctua)
assume_role(profile_name = "YOUR_PROFILE_NAME",
            role_arn = "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name",
            set_env = TRUE)

# Connect to Athena using temporary credentials
con <- dbConnect(athena(),
                s3_staging_dir = 's3://path/to/query/bucket/')
```
Connecting to Athena directly using ARN role:

```r
library(DBI)
 con <- dbConnect(athena(),
                  profile_name = "YOUR_PROFILE_NAME",
                  role_arn = "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name",
                  s3_staging_dir = 's3://path/to/query/bucket/')
```
To change the duration of ARN role session please change the parameter `duration_seconds`. 
By default `duration_seconds` is set to 3600 seconds (1 hour).

## Usage

### Basic Usage

Connect to athena, and send a query and return results back to R.

``` r
library(DBI)

# using default profile to connect
con <- dbConnect(noctua::athena(),
                 s3_staging_dir = 's3://path/to/query/bucket/')

res <- dbExecute(con, "SELECT * FROM one_row")
dbFetch(res)
dbClearResult(res)
```

To retrieve query in 1 step.

``` r
dbGetQuery(con, "SELECT * FROM one_row")
```

### Intermediate Usage

To create a tables in athena, `dbExecute` will send the query to athena
and wait until query has been executed. This makes it and idea method to
create tables within athena.

``` r
query <- 
  "CREATE EXTERNAL TABLE impressions (
      requestBeginTime string,
      adId string,
      impressionId string,
      referrer string,
      userAgent string,
      userCookie string,
      ip string,
      number string,
      processId string,
      browserCookie string,
      requestEndTime string,
      timers struct<modelLookup:string, requestTime:string>,
      threadId string,
      hostname string,
      sessionId string)
  PARTITIONED BY (dt string)
  ROW FORMAT  serde 'org.apache.hive.hcatalog.data.JsonSerDe'
      with serdeproperties ( 'paths'='requestBeginTime, adId, impressionId, referrer, userAgent, userCookie, ip' )
  LOCATION 's3://elasticmapreduce/samples/hive-ads/tables/impressions/' ;"
  
dbExecute(con, query)
```

noctua has 2 extra function to return extra information around Athena
tables: `dbGetParitiions` and `dbShow`

`dbGetPartitions` will return all the partitions (returns data.frame):

``` r
noctua::dbGetPartition(con, "impressions")
```

`dbShow` will return the table’s ddl, so you will able to see how the
table was constructed in Athena (returns SQL character):

``` r
noctua::dbShow(con, "impressions")
```

### Advanced Usage

``` r
library(DBI)
con <- dbConnect(noctua::athena(),
                 s3_staging_dir = 's3://path/to/query/bucket/')
```

#### Sending data to Athena

noctua has created a method to send data.frame from R to Athena.

``` r
# Check existing tables
dbListTables(con)
# Upload iris to Athena
dbWriteTable(con, "iris", iris, 
             partition=c("TIMESTAMP" = format(Sys.Date(), "%Y%m%d")),
             s3.location = "s3://mybucket/data/")

# Read in iris from Athena
dbReadTable(con, "iris")

# Check new existing tables in Athena
dbListTables(con)

# Check if iris exists in Athena
dbExistsTable(con, "iris")
```

### Tidyverse Usage

Creating a connection to Athena and query and already existing table
`iris` that was created in previous example.

``` r
library(DBI)
library(dplyr)

con <- dbConnect(noctua::athena(),
                aws_access_key_id='YOUR_ACCESS_KEY_ID',
                aws_secret_access_key='YOUR_SECRET_ACCESS_KEY',
                s3_staging_dir='s3://path/to/query/bucket/',
                region_name='eu-west-1')
tbl(con, sql("SELECT * FROM iris"))
```

    # Source:   SQL [?? x 5]
    # Database: Athena 0.1.4 [eu-west-1/default]
       sepal_length sepal_width petal_length petal_width species
              <dbl>       <dbl>        <dbl>       <dbl> <chr>  
     1          5.1         3.5          1.4         0.2 setosa 
     2          4.9         3            1.4         0.2 setosa 
     3          4.7         3.2          1.3         0.2 setosa 
     4          4.6         3.1          1.5         0.2 setosa 
     5          5           3.6          1.4         0.2 setosa 
     6          5.4         3.9          1.7         0.4 setosa 
     7          4.6         3.4          1.4         0.3 setosa 
     8          5           3.4          1.5         0.2 setosa 
     9          4.4         2.9          1.4         0.2 setosa 
    10          4.9         3.1          1.5         0.1 setosa 
    # … with more rows

dplyr provides lazy querying with allows to short hand `tbl(con,
sql("SELECT * FROM iris"))` to `tbl(con, "iris")`. For more information
please look at <https://db.rstudio.com/dplyr/>.

``` r
tbl(con, "iris")
```

    # Source:   table<iris> [?? x 5]
    # Database: Athena 0.1.4 [eu-west-1/default]
       sepal_length sepal_width petal_length petal_width species
              <dbl>       <dbl>        <dbl>       <dbl> <chr>  
     1          5.1         3.5          1.4         0.2 setosa 
     2          4.9         3            1.4         0.2 setosa 
     3          4.7         3.2          1.3         0.2 setosa 
     4          4.6         3.1          1.5         0.2 setosa 
     5          5           3.6          1.4         0.2 setosa 
     6          5.4         3.9          1.7         0.4 setosa 
     7          4.6         3.4          1.4         0.3 setosa 
     8          5           3.4          1.5         0.2 setosa 
     9          4.4         2.9          1.4         0.2 setosa 
    10          4.9         3.1          1.5         0.1 setosa 
    # … with more rows

Querying Athena with `profile_name` instead of hard coding
`aws_access_key_id` and `aws_secret_access_key`. By using `profile_name`
extra Meta Data is returned in the query to give users extra
information.

``` r
con <- dbConnect(noctua::athena(),
                profile_name = "your_profile",
                s3_staging_dir='s3://path/to/query/bucket/')
tbl(con, "iris")) %>% 
  filter(petal_length < 1.3)
```

    # Source:   lazy query [?? x 5]
    # Database: Athena 0.1.4 [your_profile@eu-west-1/default]
       sepal_length sepal_width petal_length petal_width species
              <dbl>       <dbl>        <dbl>       <dbl> <chr>  
     1          4.7         3.2          1.3         0.2 setosa 
     2          4.3         3            1.1         0.1 setosa 
     3          5.8         4            1.2         0.2 setosa 
     4          5.4         3.9          1.3         0.4 setosa 
     5          4.6         3.6          1           0.2 setosa 
     6          5           3.2          1.2         0.2 setosa 
     7          5.5         3.5          1.3         0.2 setosa 
     8          4.4         3            1.3         0.2 setosa 
     9          5           3.5          1.3         0.3 setosa 
    10          4.5         2.3          1.3         0.3 setosa 
    # … with more rows

``` r
tbl(con, "iris") %>% 
  select(contains("sepal"), contains("petal"))
```

    # Source:   lazy query [?? x 4]
    # Database: Athena 0.1.4 [your_profile@eu-west-1/default]
       sepal_length sepal_width petal_length petal_width
              <dbl>       <dbl>        <dbl>       <dbl>
     1          5.1         3.5          1.4         0.2
     2          4.9         3            1.4         0.2
     3          4.7         3.2          1.3         0.2
     4          4.6         3.1          1.5         0.2
     5          5           3.6          1.4         0.2
     6          5.4         3.9          1.7         0.4
     7          4.6         3.4          1.4         0.3
     8          5           3.4          1.5         0.2
     9          4.4         2.9          1.4         0.2
    10          4.9         3.1          1.5         0.1
    # … with more rows

Upload data using `dplyr` function `copy_to` and `compute`.

``` r
library(DBI)
library(dplyr)

con <- dbConnect(noctua::athena(),
                profile_name = "your_profile",
                s3_staging_dir='s3://path/to/query/bucket/')
```

Write data.frame to Athena table
```r
copy_to(con, mtcars,
        s3_location = "s3://mybucket/data/")
```              

Write Athena table from tbl_sql
```r
athena_mtcars <- tbl(con, "mtcars")
mtcars_filter <- athena_mtcars %>% filter(gear >=4)
```

Create athena with unique table name
```r
mtcars_filer %>% compute()
```

Create athena with specified name and s3 location
```r
mtcars_filer %>% 
  compute("mtcars_filer",
          s3_location = "s3://mybucket/mtcars_filer/")

# Disconnect from Athena
dbDisconnect(con)
```

## Work Groups

Creating work group:

``` r
library(noctua)
library(DBI)

con <- dbConnect(noctua::athena(),
                profile_name = "your_profile",
                encryption_option = "SSE_S3",
                s3_staging_dir='s3://path/to/query/bucket/')

create_work_group(con, "demo_work_group", description = "This is a demo work group",
                  tags = tag_options(key= "demo_work_group", value = "demo_01"))
```

List work groups:

``` r
list_work_groups(con)
```

    [[1]]
    [[1]]$Name
    [1] "demo_work_group"
    
    [[1]]$State
    [1] "ENABLED"
    
    [[1]]$Description
    [1] "This is a demo work group"
    
    [[1]]$CreationTime
    2019-09-06 18:51:28.902000+01:00
    
    
    [[2]]
    [[2]]$Name
    [1] "primary"
    
    [[2]]$State
    [1] "ENABLED"
    
    [[2]]$Description
    [1] ""
    
    [[2]]$CreationTime
    2019-08-22 16:14:47.902000+01:00

Update work group:

``` r
update_work_group(con, "demo_work_group", description = "This is a demo work group update")
```

Return work group meta data:

``` r
get_work_group(con, "demo_work_group")
```

    $Name
    [1] "demo_work_group"
    
    $State
    [1] "ENABLED"
    
    $Configuration
    $Configuration$ResultConfiguration
    $Configuration$ResultConfiguration$OutputLocation
    [1] "s3://path/to/query/bucket/"
    
    $Configuration$ResultConfiguration$EncryptionConfiguration
    $Configuration$ResultConfiguration$EncryptionConfiguration$EncryptionOption
    [1] "SSE_S3"
    
    
    
    $Configuration$EnforceWorkGroupConfiguration
    [1] FALSE
    
    $Configuration$PublishCloudWatchMetricsEnabled
    [1] FALSE
    
    $Configuration$BytesScannedCutoffPerQuery
    [1] 10000000
    
    $Configuration$RequesterPaysEnabled
    [1] FALSE
    
    
    $Description
    [1] "This is a demo work group update"
    
    $CreationTime
    2019-09-06 18:51:28.902000+01:00

Connect to Athena using work group:

``` r
con <- dbConnect(noctua::athena(),
                work_group = "demo_work_group")
```

Delete work group:

``` r
delete_work_group(con, "demo_work_group")
```

# Similar Projects

## Python:

  - `pyAthena` - A python wrapper of the python package `Boto3` using
    the sqlAlchemy framework:
    <https://github.com/laughingman7743/PyAthena>

## R:

  - `AWR.Athena` - A R wrapper of RJDBC for the AWS Athena’s JDBC
    drivers: <https://github.com/nfultz/AWR.Athena>
  - `RAthena` - A R wrapper of the python package `Boto3` using DBI as the framework: <https://github.com/DyfanJones/RAthena>
  - `awsathena` - rJava Interface to AWS Athena SDK <https://github.com/hrbrmstr/awsathena>
  - `metis` - Helpers for Accessing and Querying Amazon Athena using R, Including a lightweight RJDBC shim <https://github.com/hrbrmstr/metis>
  - `metisjars` - JARs for `metis` <https://github.com/hrbrmstr/metis-jars>
  - `metis.tidy` - Access and Query Amazon Athena via the Tidyverse <https://github.com/hrbrmstr/metis-tidy>

## Comparison:

`noctua` is basically the same as `RAthena` however it utilises the R AWS SDK `paws` to achieve the same goal.