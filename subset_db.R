library(tidyverse)
library(odbc)
library(DBI)
library(RPostgreSQL)
library(RPostgres)

drv <- RPostgres::Postgres()
user <- ""
password <- ""
dbname <- "test_1"
table_name <- ""
stg_table_name <- ""

con <- dbConnect(drv = drv,
                 dbname = dbname,
                 #host = "localhost",
                 #port = 5432,
                 user = user,
                 password = password)
dbListTables(con)

# create table
sql_statement <- "CREATE TABLE working_parking_events_statewide_2019 (
	long float8 NULL,
	lat float8 NULL,
	start_date_time timestamptz NULL,
	start_year int4 NULL,
	start_month int4 NULL,
	start_day int4 NULL,
	start_time varchar null,
	minutes numeric NULL,
	parking_cat int4 NULL,
	dow_index float8 NULL,
	dow_desc text NULL,
	vehicleweightclass int4 NULL,
	day varchar(32767) NULL,
	county_fips int4 NULL,
	cnt_year_mnth_key text NULL
);"

rs <- dbSendQuery(con, sql_statement)
dbClearResult(rs)

# create table
sql_statement <- "CREATE TABLE working_tripstemp (
	long float8 NULL,
	lat float8 NULL,
	start_date_time timestamp NULL,
	start_year int4 NULL,
	start_month int4 NULL,
	start_day int4 NULL,
	start_time  varchar(32767) null,
	minutes numeric NULL,
	parking_cat int4 NULL,
	dow_index float8 NULL,
	dow_desc text NULL,
	vehicleweightclass int4 NULL,
	day varchar(32767) NULL
);"
rs <- dbSendQuery(con, sql_statement)
dbClearResult(rs)
dbListTables(con)

# select 100 rows from table
sql_statement <- 'SELECT * FROM public."MD_Parking_2019" LIMIT 1000;'
rs <- dbSendQuery(con, sql_statement)
df <- dbFetch(rs)
# returns a dataframe
df

dbClearResult(rs)
dbDisconnect(con)
dbUnloadDriver(drv)

sum(duplicated(x))
  
