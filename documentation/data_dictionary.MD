# Data Dictionary

This project only uses a subset of the available data from the 2 sources. This applies to the data files as well as the 
columns within each file. In this data dictionary we only take columns that are used for the  project into consideration.
Please note that with the exception of the stations file all these files were uploaded to S3 and Spark in their original 
form. The number of rows correspond to their original shape.

For full details about the data please have a look at the following links:
- [yelp][yelp_link]
- [ghcn][ghcn_link]

**The Yelp Open Dataset**

- Business (160.585 rows)

This project is only interested in open businesses, and state, latitude, and longitude must not be null since these
columns are used to join with other files.

| column | data type | definition | constraint 
| :---: | :---: | :---: | :---: |   
| business_id | string |  22 char unique string 
| is_open | integer | 0 or 1 for closed or open | == 1  
| city | string | the city where the business is located | 
| state | string | the state where the business is located | Not Null 
| latitude | float | latitude of the business location | Not Null 
| longitude | float | longitude of the business location | Not Null 

- Review (8.635.403 rows)

This file contains reviews from 2004-2021, however this project is only interested in the 3 most recent years.

| column | data type | definition | constraint 
| :---: | :---: | :---: | :---: |   
| business_id | string |  22 char unique string 
| user_id | string | 22 char unique string   
| text | string | the actual review 
| date | string | the date of the review | isin(2019, 2020, 2021)

- User (2.189.457 rows)

Only users whom are in the top 25% of 'complimented' users are in scope for this project. The hypothesis behind this 
constraint is that it is likely that these reviews are more elaborate and well-written, which could make the analysis of
the text more easy for the NLP researchers. 

| column | data type | definition | constraint 
| :---: | :---: | :---: | :---: |   
| user_id | string | 22 char unique string   
| compliment_writer | integer | number of writer compliments received by the user | \>= 3 

**GHCN**

- Yearly weather data (78.898.896 rows)

Although the GHCN source contains data from all the way back to 1763 (!), for this project only the 3 most recent years
are in scope. Since each year has a separate file, this is not shown in the constraints of the data dictionary. Since 
GHCN has weather stations from all over the world, a constraint is set that the station_id has to start with 'US',
since the Yelp data only has data from the United States. Finally, there more than 30 weather elements available but this
project only focuses on the 5 core values:

- PRCP = Precipitation (tenths of mm)
- SNOW = Snowfall (mm)
- SNWD = Snow depth (mm)
- TMAX = Maximum temperature (tenths of degrees C)
- TMIN = Minimum temperature (tenths of degrees C)

Even though these are core elements, not every station on every date has a valid record of these elements and their
values.

| column | data type | definition | constraint 
| :---: | :---: | :---: | :---: |   
| station_id | string |  the id of the weather station | startswith('US')
| date | string | the date of the weather measurement by the weather station
| element | string | a weather metric | isin('PRCP', 'SNOW', 'SNWD', 'TMAX', 'TMIN')
| value | integer | value of the weather metric | 

- Stations (118.493 rows)

This file was modified in the sense that some of the original columns were already deleted before uploading to S3.
his file is difficult to import due to combination of tabs separation and a lot of different text fields with tabs.
Also, the file is very sparse which makes it more difficult to import the data correctly since it is not always clear
where one column ends and another one begins. Eventually, I have imported the data with Google sheets and file separation
detection. After the file was imported correctly I deleted the columns which were not relevant for this project.

State, latitude, and longitude must not be null since these columns are used to join with other files.

| column | data type | definition | constraint 
| :---: | :---: | :---: | :---: |   
| station_id | string |  the station of the weather measurement | startswith('US')
| state | string | the state where the weather station is located | Not Null 
| latitude | float | latitude of the weather station | Not Null 
| longitude | float | longitude of the weather station| Not Null 


[yelp_link]: https://www.yelp.com/dataset/documentation/main
[ghcn_link]: https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt