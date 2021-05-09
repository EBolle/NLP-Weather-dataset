# Data Dictionary

As mentioned in the REAMDE this project only uses a subset of the available data from the 2 sources. This applies to the
data files as well as the columns within each file. In this data dictionary we only take columns that are used for the 
project into consideration. 

For full details about all the data including all the columns please have a look at the following links:
- [yelp][yelp_link]
- [ghcn]

**The Yelp Open Dataset**

- Business

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

- Review

This file contains reviews from 2004-2021, however this project is only interested in the 3 most recent years.

| column | data type | definition | constraint 
| :---: | :---: | :---: | :---: |   
| business_id | string |  22 char unique string 
| user_id | string | 22 char unique string   
| text | string | the actual review 
| date | string | the date of the review | isin(2019, 2020, 2021)

- User

This project only keeps the users whom are in the top 25% of 'complimented' users. The hypothesis behind this constraint
is that it is likely that these reviews are more elaborate and well-written, which could make the analysis of the text
more easy for the NLP researchers. 

| column | data type | definition | constraint 
| :---: | :---: | :---: | :---: |   
| user_id | string | 22 char unique string   
| compliment_writer | integer | number of writer compliments received by the user | \>= 3 

**GHCN**







## Transformed data

- PRCP = Precipitation (tenths of mm)
- SNOW = Snowfall (mm)
- SNWD = Snow depth (mm)
- TMAX = Maximum temperature (tenths of degrees C)
- TMIN = Minimum temperature (tenths of degrees C)


[yelp_link]: https://www.yelp.com/dataset/documentation/main