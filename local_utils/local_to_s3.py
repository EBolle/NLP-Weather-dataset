"""
This module helps to upload the files from Yelp and GHCN to S3. It is recommended to gzip the files before uploading.
You can set the folders with all your files in settings.cfg before running this script.
"""


import boto3
import configparser
from pathlib import Path

config = configparser.ConfigParser()
config.read('settings.cfg')

yelp_folder = Path(config['LOCAL_PATHS']['yelp_folder'])
ghcn_folder = Path(config['LOCAL_PATHS']['ghcn_folder'])
s3_bucket = config['AWS']['s3_bucket']

yelp_paths = [path for path in yelp_folder.iterdir()]
ghcn_paths = [path for path in ghcn_folder.iterdir()]

client = boto3.client('s3')

for path in yelp_paths:
    client.upload_file(Filename=str(path), Bucket=s3_bucket, Key=f'yelp/{path.name}')

for path in ghcn_paths:
    client.upload_file(Filename=str(path), Bucket=s3_bucket, Key=f'ghcn/{path.name}')

# Scripts should be uploaded as well, so first git clone, then run this script ..
# When all the json files are in S3, create another scripts s3_to_local to create 1 json file in Pandas :).
# Haversine is not known on the other nodes, you have to pass it to these nodes explicitly with spark submit --py-files
# https://stackoverflow.com/questions/57315030/aws-emr-modulenotfounderror-no-module-named-pyarrow
# repartition lijkt toch wel erg de moeite waard te zijn, 1 executor is nog lange tijd bezig.. :)
# check verdeling in notebook?
# [hadoop@ip-172-31-24-15 spark_app]$ spark-submit --master yarn --conf spark.dynamicAllocation.enabled=true --py-files haversine.py main.py
# Script doet er ongeveer 25 min over, maar check de output nog, aantal partitions is anders op het einde, werk aan de winkel :)
# Check output!