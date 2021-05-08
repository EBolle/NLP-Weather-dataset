"""
This module helps to upload the data from Yelp and GHCN, and the Spark script and helper modules to S3. It is
recommended to gzip the data files before uploading.
"""


import boto3
import configparser
from pathlib import Path

config = configparser.ConfigParser()
config.read('settings.cfg')

yelp_folder = Path(config['LOCAL_PATHS']['yelp_folder'])
ghcn_folder = Path(config['LOCAL_PATHS']['ghcn_folder'])
s3_bucket = config['AWS']['s3_bucket']
s3_location = config['AWS']['s3_location']

yelp_files = [path for path in yelp_folder.iterdir()]
ghcn_files = [path for path in ghcn_folder.iterdir()]
cwd = Path('.')
spark_files = [cwd / 'main.py',
               cwd / 'haversine_distance.py',
               cwd / 'schemas.py']


client = boto3.client('s3')

response = client.create_bucket(
    ACL='private',
    Bucket=s3_bucket,
    CreateBucketConfiguration={
        'LocationConstraint': s3_location
    },
)

for file in yelp_files:
    client.upload_file(Filename=str(file), Bucket=s3_bucket, Key=f'yelp/{file.name}')

for file in ghcn_files:
    client.upload_file(Filename=str(file), Bucket=s3_bucket, Key=f'ghcn/{file.name}')

for file in spark_files:
    client.upload_file(Filename=str(file), Bucket=s3_bucket, Key=f'spark/{file.name}')

# [hadoop@ip-172-31-24-15 spark_app]$ spark-submit --master yarn --conf spark.dynamicAllocation.enabled=true --py-files haversine.py main.py
# Script doet er ongeveer 25 min over, maar check de output nog, aantal partitions is anders op het einde, werk aan de winkel :)
# Check output!
