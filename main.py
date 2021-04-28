"""Main script which executes the full ETL pipeline."""

import configparser
from haverstine_distance import udf_get_distance
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (col, year, month, dayofmonth, hour, weekofyear, dayofweek,
                                   broadcast, monotonically_increasing_id)
from pyspark.sql.types import StructType, TimestampType
from schemas import log_schema, song_schema

config = configparser.ConfigParser()
config.read('settings.cfg')

reviews_data_path = config['PATHS']['reviews_data_path']
max_distance = int(config['PARAMETERS']['max_distance'])


def main():
    """
    Main execution method which sequentially calls the methods to execute the full Sparify ETL pipeline:
    - Get or create a Spark session
    - Extract the raw .json song data from S3
    - Transform the song data into 2 folders - songs and artists - and Load these tables back into S3 in parquet format
    - Extract the raw .json log data from S3
    - Transform the log data into 3 folders - time, users, and songplays - and Load these tables back into S3 in parquet format
    - stop the spark session
    """
    spark = create_spark_session()

    songs_df = process_song_data(spark, song_data_path, output_data_path, song_schema)
    process_log_data(spark, song_data_path, output_data_path, log_schema, songs_df)

    spark.stop()


def create_spark_session():
    """Gets or creates an activate Spark session."""
    spark = (SparkSession
        .builder
        .appName("Sparkify ETL")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
        .getOrCreate()
    )

    return spark