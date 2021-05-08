"""This module retrieves the output .json files from S3, appends them, and writes the appended file to a local folder."""


from pathlib import Path
import boto3
import configparser
from logger import logger
import pandas as pd

config = configparser.ConfigParser()
config.read('settings.cfg')

output_folder = Path(config['LOCAL_PATHS']['output_folder'])
s3_bucket_name = config['AWS']['s3_bucket']


def main() -> None:
    """
    Executes 3 steps in order to upload all the files to S3 for the spark app to run.
    - Get an S3 bucket
    - Gets the metadata of the S3 bucket
    - Downloads all the .json output files and stores these locally as 1 merged zipped file
    """
    logger.info("*** s3_to_local.py script started... ***")

    s3_bucket = get_s3_bucket()
    count, total_mb_size = get_s3_metadata(s3_bucket)
    download_files(s3_bucket, count, total_mb_size)

    logger.info("*** Download from S3 to local complete, you are now ready to proceed with the NLP research... ***")


def get_s3_bucket():
    """Returns an S3 bucket, if you are not using credentials in a .aws folder set your credentials here."""
    logger.info("Getting an S3 bucket...")
    s3 = boto3.resource('s3')
    s3_bucket = s3.Bucket(s3_bucket_name)

    return s3_bucket


def get_s3_metadata(s3_bucket):
    """Counts the .json.gz objects in the S3 bucket to keep track of the download process."""
    logger.info("Getting the metadata of the S3 bucket...")
    count = 0
    total_mb_size = 0

    for object in s3_bucket.objects.filter(Prefix='output'):
        count += 1
        total_mb_size += object.size

    return count, total_mb_size


def download_files(s3_bucket, count, total_mb_size):
    """Downloads the .json output files from S3, merges each file into 1 output file, and saves it locally as .gzip."""
    logger.info("Start downloading the files from the S3 bucket...")
    df = pd.DataFrame()

    for idx, object in enumerate(s3_bucket.objects.filter(Prefix='output'), start=1):
        if object.key.endswith('json.gz'):
            if (idx % 10) == 0:
                logger.debug(f"Downloading file {idx} / {count}...")
            file_path = f"s3://{s3_bucket}/{object.key}"
            temp_df = pd.read_json(file_path, lines=True)
            df = df.append(temp_df)

    output_folder.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Saving the merged files as 1 zipped file in {output_folder}/nlp_weather_dataset.json.gz...")
    df.to_json(f"{output_folder}/nlp_weather_dataset.json.gz", orient='records', compression='gzip')
