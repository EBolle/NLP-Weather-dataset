"""
This module helps to upload the files from Yelp and GHCN to S3. It is recommended to gzip the files before uploading.
You can set the folders with all your files in settings.cfg before running this script.
"""


import boto3
import configparser

config = configparser.ConfigParser()
config.read('settings.cfg')

yelp_folder = config['LOCAL_PATHS']['yelp_folder']
ghcn_folder = config['LOCAL_PATHS']['ghcn_folder']
s3_bucket = config['AWS']['s3_bucket']

client = boto3.client('s3')
# Create a loop! :)
client.upload_file(Filename=yelp_folder, Bucket=s3_bucket, Key='yelp/business.json.gz')