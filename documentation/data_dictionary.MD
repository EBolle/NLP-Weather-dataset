# Data Dictionary

As mentioned in the REAMDE this project only uses a subset of the available data from the 2 sources. This applies to the
data files as well as the columns within each file. In this data dictionary we only take columns that are used for the 
project into consideration. 

For full details about all the data including all the columns please have a look at the following links:
- [yelp][yelp_link]
- [ghcn]

**The Yelp Open Dataset**

- Business

| column | data type | definition | constraint |
|  -|-   |    -|-    |     -|-    |    -|-     |
| business_id | string |  22 char unique string | |
| is_open | integer | 0 or 1 for closed or open | = 1 | |
| city | string | the city where the business is located | |
| state | string | the state where the business is located | Not Null |
| latitude | float | latitude of the business location | Not Null |
| longitude | float | longitude of the business location | Not Null | 

## Transformed data

- PRCP = Precipitation (tenths of mm)
- SNOW = Snowfall (mm)
- SNWD = Snow depth (mm)
- TMAX = Maximum temperature (tenths of degrees C)
- TMIN = Minimum temperature (tenths of degrees C)


[yelp_link]: https://www.yelp.com/dataset/documentation/main