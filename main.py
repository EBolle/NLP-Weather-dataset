"""Main script which executes the full ETL pipeline."""

import configparser
from haverstine_distance import udf_get_distance
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (col, year, month, dayofmonth, hour, weekofyear, dayofweek,
                                   broadcast, monotonically_increasing_id)
from pyspark.sql.types import DoubleType
from schemas import yearly_weather_schema

config = configparser.ConfigParser()
config.read('settings.cfg')

reviews_data_path = config['PATHS']['reviews_data_path']
max_distance = int(config['PARAMETERS']['max_distance'])


def main():
    """
    """
    spark = create_spark_session()

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


def create_all_distances_df(spark, business_path, us_stations_path):
    """
    Combines the business.json and us_stations.txt files to create a dataframe with the weather station with a
    certain range from the business. (max_distance parameter).
    """
    business = (spark
        .read
        .json(business_path)
        .filter(col('is_open') == 1)
        .filter(col('state').isNotNull())
        .filter(col('latitude').isNotNull() & col('longitude').isNotNull())
        .select(col('business_id'),
                col('city'),
                col('state'),
                col('latitude'),
                col('longitude'))
    )

    us_stations = (spark
        .read
        .csv(us_stations_path, sep='\t', header=True)
        .filter(col('station_id').startswith('US'))
        .filter(col('state').isNotNull())
        .filter(col('latitude').isNotNull() & col('longitude').isNotNull())
        .withColumn('station_latitude', col('latitude').cast(DoubleType()))
        .withColumn('station_longitude', col('longitude').cast(DoubleType()))
        .select(col('station_id').alias('us_station_id'),
                col('station_latitude'),
                col('station_longitude'),
                col('state').alias('station_state'))
    )

    business_station_join_condition = [business.state == us_stations.station_state]

    all_distances = (business
        .join(broadcast(us_stations), on=business_station_join_condition, how='inner')
        .withColumn('distance', udf_get_distance(col('longitude'),
                                                 col('latitude'),
                                                 col('station_longitude'),
                                                 col('station_latitude')).cast(DoubleType()))
        .filter(col('distance') < max_distance)
        .select(col('business_id'),
                col('city'),
                col('state'),
                col('latitude'),
                col('longitude'),
                col('us_station_id'),
                col('station_latitude'),
                col('station_longitude'),
                col('distance'))
)
