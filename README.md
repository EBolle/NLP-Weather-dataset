# NLP-Weather-dataset

This project contains a data pipeline that creates a dataset of reviews of local businesses in the United States, and
the local weather on the day of writing of that review. This dataset can help NLP researchers to examine whether
there is a relationship between written text and the weather. A glimpse of the dataset:

<img src="https://user-images.githubusercontent.com/49920622/118020706-b29d1680-b35a-11eb-98d0-87377f6aa0c4.JPG">

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
- /by_year .csv files from 2019-2021 
- stations.txt which is a modified version of ghcnd-stations.txt

You can find more details about the data in the [data dictionary][data_dictionary].

## The data model

The data model provides insights into the steps taken to go from the raw data to the final table. You can find a 
detailed description in the [documentation folder][documentation_md].

## Dataflow

<img src="https://user-images.githubusercontent.com/49920622/117587202-1c2fe180-b11d-11eb-849d-28ec3a6274dc.png">

If you want to create this diagram yourself have a look at `local_utils/dataflow.py`.

## Instructions

To execute the pipeline there are a few things you need to do. To make life more easy I have provided 2 scripts in
the `local_utils` folder that help to upload the data to S3, and download and merge the data from S3 after the Spark 
script has finished. However, you can also upload and download all the files and scripts manually if you would like. 
Please note that if you are going to use the scripts you also have to install and activate the accompanying virtual
environment. 

1. Clone this project
2. Add 2 folders to the local cloned project: /ghcn & /yelp (optional)
3. Download the data from the 2 sources section 
4. Move the data into the /ghcn & /yelp folders (optional)
5. Make sure to add `year_` before the file names of the GHCN yearly weather data in order for the Spark script to load
the files correctly
6. Modify settings.cfg 

The `settings.cfg` file has default settings based on using the helper scripts. If you do not plan to use them make sure
the local_paths refer to the correct locations. The same goes for your s3 bucket and location, make sure the location
corresponds with the location from your credentials if you have set them. If you receive any errors with regard to the
AWS credentials please look here for more information [here][aws_cred]. You can always set the credentials in the script
if necessary. 

Finally, the parameters have sensible defaults which result in 120K rows of reviews and meta data. You can tweak these
parameters to get more or less accurate and / or relevant data. Please note that when you increase the parameters it will
take (a lot) longer to execute the script. With the default settings it takes ~20 minutes to run the Spark script on EMR.

7. Install and activate the virtual environment based on the `environment.yml` file (optional)

Make sure you are in the top-level directory of the local project folder and enter the following commands:

```bash
conda env create -f environment.yml
conda activate nlp_weather
```

If you do not use Anaconda use `venv` or `virtualenv` instead. 

8. Upload the data and the spark_app files to S3

If you want to upload the files manually make sure to use the correct folder names:

<img src="https://user-images.githubusercontent.com/49920622/118023439-d6159080-b35d-11eb-92ae-29dda91a209a.JPG">

If you use the script make sure you are in the top-level directory of the project and execute the following command:

```bash
python local_utils/local_to_s3.py
```

9. Activate a Spark cluster on EMR with access to the S3 bucket

The spark_app was successfully tested with the following setup:

- emr-6.2.0
- Spark 3.0.1
- Master: 1x m5.xlarge
- Core: 2x m5.xlarge

Make sure that the Cluster can access the S3 bucket with the data and the spark_app files. 

10. Connect to the Spark cluster with ssh, sync the spark_app folder, and submit the spark job

```bash
ssh -i <location to your key-pair file> hadoop@<master-public-dns-name>
```

Once connected to the cluster enter the following bash commands:

```bash
aws s3 sync s3://<your bucket>/spark_app .
spark-submit --master yarn --py-files haversine_distance.py main.py
```

11. Download the output .json files from S3 and merge into 1 zipped nlp-weather dataset file

Once the Spark script is finished running and the files are correctly downloaded to S3, you may execute the following
command: 

```bash
python local_utils/s3_to_local.py
```

This script created an `output` sub folder in the project folder with the zipped `nlp_weather_dataset`.

## Contact

In case of any questions or remarks please contact me via LinkedIn or open a pull request.

[yelp]: https://www.yelp.com/dataset
[ghcn]: https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ncdc:C00861/html
[data_dictionary]: https://github.com/EBolle/NLP-Weather-dataset/blob/main/documentation/data_dictionary.MD
[documentation_md]: https://github.com/EBolle/NLP-Weather-dataset/blob/main/documentation/data_model.MD
[aws_cred]: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
