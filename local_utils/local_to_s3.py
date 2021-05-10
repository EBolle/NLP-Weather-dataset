"""
This module helps to upload the data from Yelp and GHCN, and the Spark script and helper modules to S3. It is
recommended to gzip the data files before uploading.
"""


import boto3
import configparser
from logger import logger
from pathlib import Path

config = configparser.ConfigParser()
config.read('settings.cfg')

yelp_folder = Path(config['LOCAL_PATHS']['yelp_folder'])
ghcn_folder = Path(config['LOCAL_PATHS']['ghcn_folder'])
s3_bucket = config['AWS']['s3_bucket']
s3_location = config['AWS']['s3_location']


def main() -> None:
    """
    Executes 3 steps in order to upload all the files to S3 for the spark app to run.
    - Get an S3 client
    - Create a new S3 bucket
    - Upload the data and spark app scripts, modules, and settings to the new S3 bucket
    """
    logger.info("*** local_to_s3.py script started... ***")

    s3_client = get_s3_client()
    create_bucket_on_s3(s3_client)
    upload_files(s3_client)

    logger.info("*** Upload to S3 complete, you are now ready to proceed with the execution of the Spark app... ***")


def get_s3_client():
    """Returns an S3 client, if you are not using credentials in a .aws folder set your credentials here."""
    logger.info("Getting an S3 client...")
    s3_client = boto3.client('s3')

    return s3_client


def create_bucket_on_s3(s3_client) -> None:
    """Creates a bucket on S3 based on the s3_bucket and s3_location variables in settings.cfg"""
    logger.info(f"Creating a new bucket on S3 called {s3_bucket} located at {s3_location}...")

    response = s3_client.create_bucket(
        ACL='private',
        Bucket=s3_bucket,
        CreateBucketConfiguration={
            'LocationConstraint': s3_location
        },
    )

    response_public = s3_client.put_public_access_block(
        Bucket=s3_bucket,
        PublicAccessBlockConfiguration={
            'BlockPublicAcls': True,
            'IgnorePublicAcls': True,
            'BlockPublicPolicy': True,
            'RestrictPublicBuckets': True
        },
    )


def upload_files(s3_client) -> None:
    """Uploads the ylep, ghcn, and spark app files to the S3 bucket."""
    logger.info(f"Starting to upload the files to {s3_bucket}, dependent on your connection this can take a while.")

    yelp_files = [path for path in yelp_folder.iterdir()]
    ghcn_files = [path for path in ghcn_folder.iterdir()]
    spark_app_files = [path for path in Path('.') / 'spark_app']

    for list in [yelp_files, ghcn_files, spark_app_files]:
        if not list:
            raise Warning(f"The {list} directory is empty, please check the directory path and the actual data.")

    for idx, file in enumerate(yelp_files, start=1):
        file_size = round(file.stat().st_size * 1e6)
        logger.debug(f"Uploading yelp file {file.name} ({idx}/{len(yelp_files)}), this file is {file_size} MB...")
        s3_client.upload_file(Filename=str(file), Bucket=s3_bucket, Key=f'yelp/{file.name}')

    for idx, file in enumerate(ghcn_files, start=1):
        file_size = round(file.stat().st_size * 1e6)
        logger.debug(f"Uploading ghcn file {file.name} ({idx}/{len(ghcn_files)}), this file is {file_size} MB...")
        s3_client.upload_file(Filename=str(file), Bucket=s3_bucket, Key=f'ghcn/{file.name}')

    for idx, file in enumerate(spark_app_files, start=1):
        logger.debug(f"Uploading the spark_app files, this should not take long...")
        s3_client.upload_file(Filename=str(file), Bucket=s3_bucket, Key=f'spark_app/{file.name}')


if __name__ == '__main__':
    main()
