# NLP-Weather-dataset

This project contains a data pipeline that creates a dataset of reviews of local businesses in the United States, and
the local weather on the day of writing of that review. This dataset can help NLP researchers to examine whether
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
- /by_year .csv files from 2019-2021 
- stations.txt which is a modified version of ghcnd-stations.txt

You can find more details about the data in the [data dictionary][data_dictionary].

## The data model

The data model provides insights into the steps taken to go from the raw data to the final table. You can find a 
detailed description in the [documentation folder][documentation_md].

## Dataflow

<img src="https://user-images.githubusercontent.com/49920622/117587202-1c2fe180-b11d-11eb-849d-28ec3a6274dc.png">

If you want to re-create this image yourself have a look at `dataflow.py`.

## Instructions

To execute the pipeline there are a few things you need to do. Note that if you want to upload and download the files
manually to S3 you can skip step 4-6 & 9. 

1. Clone this project
2. Add 2 folders to the local project: /ghcn & /yelp
3. Download the data from the 2 sources in `The Data` section into /ghcn & /yelp
4. Modify settings.cfg 
5. Install and activate the virtual environment based on the `environment.yml` file

```bash
conda env create -f environment.yml
conda activate nlp_weather
```

You can also reproduce the diagram from the `Dataflow` section with this environment, the code can be found in
`dataflow.py`.

6. Upload the data and the spark_app files to S3

Make sure you are in the top-level directory of the project and execute the following command:

```bash
python local_utils/local_to_s3.py
```

7. Activate a Spark cluster on EMR with access to the S3 bucket

The spark_app was successfully tested with the following EMR settings:

```bash
EMR cluster CLI export here
```

Make sure that the Cluster can access the S3 bucket with the data and the spark_app files. 

8. Connect to the Spark cluster with ssh, sync the spark_app folder with S3, and submit the spark job.

```bash
ssh -i <location to your .pem file> hadoop@<master-public-dns-name>
```

Once connected to the cluster enter the following bash commands:

```bash
aws s3 sync s3://<your bucket>/spark_app .
spark-submit --master yarn --conf spark.dynamicAllocation.enabled=true --py-files haversine_distance.py main.py
```

9. Download the output .json files from S3 and merge into 1 zipped nlp-weather dataset file

```bash
python local_utils/s3_to_local.py
```

## Contact

In case of any questions or remarks please contact me via LinkedIn or open a pull request.

[yelp]: https://www.yelp.com/dataset
[ghcn]: https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ncdc:C00861/html
[data_dictionary]: https://github.com/EBolle/NLP-Weather-dataset/blob/main/documentation/data_dictionary.MD
[documentation_md]: https://github.com/EBolle/NLP-Weather-dataset/blob/main/documentation/data_model.MD
