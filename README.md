# NLP-Weather-dataset

This project contains a data pipeline that creates a dataset which contains reviews of local businesses in the United
States, and the weather during the time of writing of that review. This dataset allows NLP researchers to examine whether 
there is a relation between written text and the weather. 

## The data

The data originates from 2 great sources of publicly available data:

- [The Yelp Open Dataset](yelp)
- [Global Historical Climatology Network (GHCN)](ghcn)

To acknowledge the specific version of the GHCN dataset hereby the official citation:

> Menne, M.J., I. Durre, B. Korzeniewski, S. McNeal, K. Thomas, X. Yin, S. Anthony, R. Ray, 
R.S. Vose, B.E.Gleason, and T.G. Houston, 2012: Global Historical Climatology Network - 
Daily (GHCN-Daily), Version 3.26. NOAA National Climatic Data Center. 

From each source we take a subset of the available data, and to give you an idea of what to expect the number of records /
rows is included.

**The Yelp Open Dataset**
- business.json (160.585 records)
- review.json (8.635.403 records)
- user.json (2.189.457 records)

**GHCN**
- /by_year .csv datasets from 2004-2021 (628.553.790 rows)
- us_stations.txt this is a modified version of ghcnd-stations.txt (65.171 rows)

Uncompressed these files contain more than 30GB of data.

## Transformations

The final dataset looks as follows:

`table with review, date, state, lon, lat, temperature`

**The Yelp Open Dataset**

From the Yelp source we retrieve the location (latitude, longitude) of the business for which the review was written,
and the date of writing of the review. The review dates ranges from 2004-2021. To assure the quality of the review we
only include reviews of users with more than 25 useful reviews (top 15% range). 

**GHCN**

From the GHCN source we retrieve weather metrics for 65.171 weather stations throughout the United States, for each day
between 2004-2021. Since GHCN provides weather data globally, we need to filter the yearly weather data on location 
('US'). In total there are 35 metrics available but we only consider the 6 most prevalent metrics.

- PRCP = Precipitation (tenths of mm)
- SNOW = Snowfall (mm)
- SNWD = Snow depth (mm)
- TMAX = Maximum temperature (tenths of degrees C)
- TMIN = Minimum temperature (tenths of degrees C)
- TOBS = Temperature at the time of observation (tenths of degrees C) 

## Processing

Due to the size of the data we will be utilizing Spark on EMR (AWS). Note that if you do not want to use Spark on EMR
I have included some code snippets in /local_python which allow you to process most of the data locally, even if you have 
a computer with low memory. However, once you want to join these processed data sources it becomes very tedious to do that
locally due to the size of the data. Therefore, I would recommend to use Spark on EMR.

## Haverstine



## Dataflow

Show a very simple diagram with Yelp / GHCN -> Local -> S3 -> Spark on EMR -> S3, wrapped in Airflow.

## Instructions

To execute the pipeline yourself there are a few things you need to do.

- Download the data from the 2 sources 
- Upload the data to S3 (include AWS CLI2 code?)
- Make sure Airflow is up and running
- Make sure you have an active Spark cluster on EMR 
- Make sure you set the right credentials in Airflow

Once you are ready and the DAG is loaded correctly in Airflow, simply unpause the DAG and execute. 

`show Airflow UI / DAG flow`

[yelp]: https://www.yelp.com/dataset
[ghcn]: https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ncdc:C00861/html