# NLP-Weather-dataset

This project contains a data pipeline that creates a dataset of reviews of local businesses in the United States, and
the local weather on the day of writing of that review. We believe this data can help NLP researchers to examine whether
there is a relationship between written text and the weather.

## The data

The data originates from 2 sources of publicly available data:

- [The Yelp Open Dataset][yelp]
- [Global Historical Climatology Network (GHCN)][ghcn]

To acknowledge the specific version of the GHCN dataset hereby the official citation:

> Menne, M.J., I. Durre, B. Korzeniewski, S. McNeal, K. Thomas, X. Yin, S. Anthony, R. Ray, 
R.S. Vose, B.E.Gleason, and T.G. Houston, 2012: Global Historical Climatology Network - 
Daily (GHCN-Daily), Version 3.26. NOAA National Climatic Data Center. 

From each source we only take a subset of the available data.

**The Yelp Open Dataset**
- business.json 
- review.json 
- user.json 

**GHCN**
- /by_year .csv files from ????-2021 
- us_stations.txt which is a modified version of ghcn-stations.txt

## The data model

The data model provides insights into the steps taken to go from the raw data to the final table. You can find a 
detailed description in the [documentation folder][documentation_md].

## Dataflow

Show a very simple diagram with Yelp / GHCN -> Local -> S3 -> Spark on EMR -> S3, wrapped in Airflow.

## Instructions

To execute the pipeline yourself there are a few things you need to do.

- Download the data from the 2 sources 
- Upload the data to S3 (data_upload/local_to_s3.py might be helpful) 
- Make sure you have an active Spark cluster on EMR 
- Configure the setting.cfg file
- Execute

### Upload the data to S3

In this section I will explain how to upload the files to S3 based on the local_to_s3.py script, there are 3 steps you 
need to take:

- create 2 folders (e.g., `ghcn` and `yelp`)
- gzip each file you want to upload to S3
- create a bucket on S3
- adjust the settings.cfg file accordingly

### Spark on EMR


[yelp]: https://www.yelp.com/dataset
[ghcn]: https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ncdc:C00861/html
[documentation_md]: https://github.com/EBolle/NLP-Weather-dataset/blob/main/documentation/data_model.MD