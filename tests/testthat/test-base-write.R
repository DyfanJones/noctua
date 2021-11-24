context("base write raw")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("noctua_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("noctua_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("noctua_s3_tbl"): "s3://path/to/bucket/"

library(data.table)

exp_df = data.table(
  var1=LETTERS,
  var2=1:26
)

test_that("Check if raw object is written out correctly", {
  temp_file1 = tempfile()
  temp_file2 = tempfile()
  
  fwrite(exp_df, temp_file1)
  
  obj_raw = readBin(temp_file1, "raw", n=file.size(temp_file1))
  
  base_write_raw(obj_raw, temp_file2)
  
  obj_df = data.table::fread(temp_file2)
  
  expect_equal(exp_df, obj_df)
  
  unlink(temp_file1)
  unlink(temp_file2)
})
