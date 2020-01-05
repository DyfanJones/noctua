library(DBI)
con <- dbConnect(noctua::athena(),
                 aws_access_key_id = ${0:aws_access_key_id=NULL},
                 aws_secret_access_key = ${1:aws_secret_access_key=NULL},
                 aws_session_token = ${2:aws_session_token=NULL},
                 schema_name = ${3:schema_name="default"},
                 work_group = ${4:work_group=NULL},
                 profile_name = ${5:profile_name=NULL},
                 s3_staging_dir = ${6:s3_staging_dir=NULL},
                 region_name = ${7:region_name=NULL})
