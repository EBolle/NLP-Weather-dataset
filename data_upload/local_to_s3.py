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
