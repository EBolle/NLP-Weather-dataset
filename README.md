# NLP-Weather-dataset

This project contains a data pipeline that creates a dataset which contains reviews of business in the United States, 
and the weather during the time of writing of that review. This dataset allows NLP researchers to examine whether there
is a relation between written text and the weather. 

## The data

The data originates from 2 great sources of publicly available data:

- [The Yelp Open Dataset](yelp)
- [Global Historical Climatology Network (GHCN)](ghcn)

To acknowledge the specific version of the GHCN dataset hereby the official citation:

> Menne, M.J., I. Durre, B. Korzeniewski, S. McNeal, K. Thomas, X. Yin, S. Anthony, R. Ray, 
R.S. Vose, B.E.Gleason, and T.G. Houston, 2012: Global Historical Climatology Network - 
Daily (GHCN-Daily), Version 3.26. NOAA National Climatic Data Center. 

From each source we take a subset of the available data, and to give you an idea of what to expect the number of rows /
records is included.

**The Yelp Open Dataset**
- business.json (160.585 records)
- review.json (8.635.403 records)
- user.json (2.189.457 records)

**GHCN**
- /by_year .csv datasets from 2004-2021 (628.553.790 rows)
- us_stations.txt this is a modified version of ghcnd-stations.txt (65.171 rows)

## Transformations

The final dataset looks as follows:

`table with review, date, state, lon, lat, temperature`

**The Yelp Open Dataset**

From the Yelp source we retrieve the location (latitude, longitude) of the business for which the review was written,
and the date of writing of the review. The review dates ranges from 2004-2021. To assure the quality of the review we
only include reviews of users with more than 25 useful reviews (top 20% range). 

**GHCN**

From the GHCN source we retrieve weather metrics for 65.171 weather stations throughout the United States, for each day
between 2004-2021. Since GHCN provides weather data globally, we need to filter the yearly weather data on location 
('US'). In total there are 35 metrics available, but we only look at the 6 of them, which are the most prevalent metrics.

- PRCP = Precipitation (tenths of mm)
- SNOW = Snowfall (mm)
- SNWD = Snow depth (mm)
- TMAX = Maximum temperature (tenths of degrees C)
- TMIN = Minimum temperature (tenths of degrees C)
- TOBS = Temperature at the time of observation (tenths of degrees C) 


[yelp]: https://www.yelp.com/dataset
[ghcn]: https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ncdc:C00861/html