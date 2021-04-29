"""Main script which executes the full ETL pipeline."""

import configparser
from haverstine_distance import udf_get_distance
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (col, broadcast, to_date)
from pyspark.sql.types import DoubleType, StructType
from schemas import yearly_weather_schema

config = configparser.ConfigParser()
config.read('settings.cfg')

business_path = config['PATHS']['business']
us_stations_path = config['PATHS']['us_stations']
max_distance = int(config['PARAMETERS']['max_distance'])

review_path = config['PATHS']['review']
user_path = config['PATHS']['user']

yearly_weather_path = config['PATHS']['yearly_weather']


def main():
    """
    """
    spark = create_spark_session()

    distances = create_distances(spark, business_path, us_stations_path, max_distance)
    review = create_review(spark, review_path, user_path)
    yearly_weather = create_yearly_weather(spark, yearly_weather_path, yearly_weather_schema)


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


def create_distances(spark, business_path: str, us_stations_path: str, max_distance: int) -> DataFrame:
    """
    Combines the business.json and us_stations.txt file to create a dataframe with the closest weather stations
    to the local business based on the `max_distance` parameter.
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

    distances = (business
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

    return distances


def create_review(spark, review_path: str, user_path: str) -> DataFrame:
    """
    Subsets the raw review data based on 2 filters:
    - the business must be open
    - only review from credible users are considered (> 25 useful reviews, top 15%)

    :param spark:
    :param review_path:
    :param user_path:
    :return:
    """
    user = (spark
        .read
        .json(user_path)
        .filter(col('useful') > 25)
        .select(col('user_id'))
    )

    review_raw = (spark
        .read
        .json(review_path)
        .withColumn('review_date', to_date(col('date')))
        .select(col('business_id'),
                col('user_id'),
                col('text'),
                col('review_date'))
    )

    user_review_join_condition = [user.user_id == review_raw.user_id]

    review = (review_raw
        .join(broadcast(user), on=user_review_join_condition, how='inner')
        .select(col('city'),
                col('state'),
                col('latitude'),
                col('longitude'),
                col('text'),
                col('review_date'))
    )

    return review


def create_yearly_weather(spark, yearly_weather_path: str, yearly_weather_schema: StructType) -> DataFrame:
    """

    :param spark:
    :param yearly_weather_path:
    :param yearly_weather_schema:
    :return:
    """
    pass


if __name__ == '__main__':
    main()