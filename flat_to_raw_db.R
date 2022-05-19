library(tidyverse)
library(odbc)
library(DBI)
library(RPostgreSQL)
library(RPostgres)

setwd()
files <- list.files(pattern = ".csv")

scan_data <- function(d) {
  
  # finds the 'na' values for each column
  data_errors <- tibble()
  for (x in names(d)) {
    b <- df %>% 
      filter(is.na(x))
    rbind(data_errors,b)
  }
  
  # find values where start date is after the end date
  date_errors <- d %>%
    filter(startdate > enddate)
  
  # find values where duration is less than 0
  duration_errors <- d %>%
    filter(parkingduration < 0)
  
  # find values where weight class is less than 0
  weight_errors <- d %>%
    filter(vehicleweightclass < 0)
  
  
  
  return(list(nrow(data_errors), nrow(date_errors), nrow(weight_errors)))
  
}

drv <- RPostgres::Postgres()
user <- ""
password <- ""
dbname <- ""
table_name <- ""
fields <- ""
value <- df #df to write to table

#############################################################
file <- "nmvtrips2019q1_v2.csv"
a <- read.csv(file = file, header = TRUE)
df <- a
rm(a)



scan_data(df)
# gets dimenstion of df before loading
preload_rows1 <- as.numeric(nrow(df))
preload_cols1 <- as.numeric(ncol(df))

### CREATE TABLES ###
con <- dbConnect(drv = drv,
                 dbname = dbname,
                 #host = "localhost",
                 #port = 5432,
                 user = user,
                 password = password)
value <- df #df to write to table
# write df to database
dbWriteTable(conn = con, 
             name = table_name, 
             value = value, 
             row.names = FALSE)

# remove initial df for space
rm(df)

# read in table to check against pre loaded data
dtab = dbReadTable(conn = con, "MD_Parking_2019")

# post load structure
postload_rows1 <- as.numeric(nrow(dtab))
postload_cols1 <- as.numeric(ncol(dtab))

preload_rows1 - postload_rows1
preload_cols1 - postload_cols1
# free up resources
dbDisconnect(con)
dbUnloadDriver(drv)
rm(dtab)


### UPDATE TABLE WITH ADDITIONAL UPLOADS ###
#second data file
file <- "nmvtrips2019q2_v2.csv"
a <- read.csv(file = file, header = TRUE)
df <- a
rm(a)



scan_data(df)
# gets dimenstion of df before loading
preload_rows2 <- as.numeric(nrow(df)) + preload_rows1
preload_cols2 <- as.numeric(ncol(df))

# should update table in db by adding rows of df
con <- dbConnect(drv = drv,
                   dbname = dbname,
                   #host = "localhost",
                   #port = 5432,
                   user = user,
                   password = password)
value <- df #df to write to table
dbWriteTable(con, table_name, df, append = TRUE, row.names = FALSE)
rm(df)

# query table
dtab = dbReadTable(conn = con, "MD_Parking_2019")
# post load structure
postload_rows2 <- as.numeric(nrow(dtab))
postload_cols2 <- as.numeric(ncol(dtab))

preload_rows2 - postload_rows2
preload_cols2 - postload_cols2
# free up resources
dbDisconnect(con)
dbUnloadDriver(drv)
rm(dtab)

#third data file
file <- "nmvtrips2019q3_v2.csv"
a <- read.csv(file = file, header = TRUE)
df <- a
rm(a)

scan_data(df)
# gets dimenstion of df before loading
preload_rows3 <- as.numeric(nrow(df)) + preload_rows2
preload_cols3 <- as.numeric(ncol(df))

# should update table in db by adding rows of df
con <- dbConnect(drv = drv,
                 dbname = dbname,
                 #host = "localhost",
                 #port = 5432,
                 user = user,
                 password = password)
value <- df #df to write to table
dbWriteTable(con, table_name, df, append = TRUE, row.names = FALSE)
rm(df)

# query table
dtab = dbReadTable(conn = con, "MD_Parking_2019")
# post load structure
postload_rows3 <- as.numeric(nrow(dtab))
postload_cols3 <- as.numeric(ncol(dtab))

preload_rows3 - postload_rows3
preload_cols3 - postload_cols3
# free up resources
dbDisconnect(con)
dbUnloadDriver(drv)
rm(dtab)

#fourth data file
file <- "nmvtrips2019q4_v2.csv"
a <- read.csv(file = file, header = TRUE)
df <- a
rm(a)

scan_data(df)
# gets dimenstion of df before loading
preload_rows4 <- as.numeric(nrow(df)) + preload_rows3
preload_cols4 <- as.numeric(ncol(df))

# should update table in db by adding rows of df
con <- dbConnect(drv = drv,
                 dbname = dbname,
                 #host = "localhost",
                 #port = 5432,
                 user = user,
                 password = password)
value <- df #df to write to table
dbWriteTable(con, table_name, df, append = TRUE, row.names = FALSE)
rm(df)
rm(value)

# query table
dtab = dbReadTable(conn = con, "MD_Parking_2019")
# post load structure
postload_rows4 <- as.numeric(nrow(dtab))
postload_cols4 <- as.numeric(ncol(dtab))

preload_rows4 - postload_rows4
preload_cols4 - postload_cols4
# free up resources
dbDisconnect(con)
dbUnloadDriver(drv)
rm(dtab)

dbRemoveTable(conn = con, "MD_Parking_2019")
