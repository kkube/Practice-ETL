# 1. Create desired tables
# 2. Create a function to process data
# 3. Create a function to append the deisred table with the processed df
# 4. Create a function that applies #2 & #3 to each subset of data (.csv files or from db table)

library(tidyverse)


str(x)

### TRIPSTEMP TABLE ###
# rename existing columns
names(x)[names(x)=="startloclat"] <- "lat"
names(x)[names(x)=="startloclon"] <- "lon"
names(x)[names(x)=="startdate"] <- "start_date_time"


# create new columns
start_year <- str_split(df$start_date_time, "-")
start_month <- ""
start_day <- ""
start_time <- ""
minutes <- ""
parking_cat <- ""
dow_index <- ""
dow_desc <- ""
county_fips <- ""

# adds columns to df
df <- df %>%
  add_column(start_year, .after = start_date_time) %>%
  add_column(start_month, .after = start_year) %>%
  add_column(start_day, .after = start_month) %>%
  add_column(start_time, .after = start_day) %>%
  add_column(minutes, .after = start_time) %>%
  add_column(parking_cat, .after = minutes) %>%
  add_column(dow_index, .after = parking_cat) %>%
  add_column(dow_desc, .after = dow_index) %>%
  add_column(county_fips, .after = vehicleweightclass) 

# edit columns to be calculations
df <- df %>% 
  mutate(start_year = as.integer(str_sub(df$start_date_time,1,4))) %>%
  mutate(start_month = as.numeric(str_sub(df$start_date_time,6,7))) %>%
  mutate(start_day = as.numeric(str_sub(df$start_date_time,9,10))) %>%
  mutate(minutes = as.numeric(parkingduration*60))
str(df)
