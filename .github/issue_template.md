<!--
  If you are wishing to request a new feature please ignore the following (and feel free to delete the below). Issues please try to use the template as it will help me get to the problem faster. 

Please remove all the extra text.
-->
  
  ### Issue Description
  <!--Example: `dbGetQuery()` returns incorrect timestamps.-->
  
  ### Reproducible Example
  <!--
  Please include a small code example, if you can please use open source data i.e. `iris`, `mtcars` etc... To demostrate your issue.

If you struggle with markdown and formatting please use the:
  
  `reprex` package to help `install.packages("reprex")`

https://github.com/tidyverse/reprex#what-is-a-reprex

Example:
  ```r
library(paws.athena)
library(DBI)
con <- dbConnect(paws.athena::athena(),
                 profile_name = "paws.athena")

dbWriteTable(con, "mtcars", mtcars, s3.location = Sys.getenv("my_s3_bucket"))

dbGetQuery(con, "select mpg, cyl, disp, hp from mtcars")
```

**NOTE:** Please don't include your AWS credentials!
-->
<details>
<summary>Session Info</summary>

```r
devtools::session_info()
#> output
```
</details>
