"""This module retrieves the output .json files from S3, appends them, and writes the appended file to a local folder."""


from pathlib import Path
import boto3
import configparser
import pandas as pd

config = configparser.ConfigParser()
config.read('settings.cfg')

output_folder = Path(config['LOCAL_PATHS']['output_folder'])
s3_bucket = config['AWS']['s3_bucket']


s3 = boto3.resource('s3')
nlp_bucket = s3.Bucket(s3_bucket)

df = pd.DataFrame()

for object in nlp_bucket.objects.filter(Prefix='output'):
    if object.key.endswith('json.gz'):
        file_path = f"s3://{s3_bucket}/{object.key}"
        temp_df = pd.read_json(file_path, lines=True)
        df = df.append(temp_df)

output_folder.mkdir(parents=True, exist_ok=True)
df.to_json(f"{output_folder}/nlp_weather_dataset.json.gz", orient='records', compression='gzip')
