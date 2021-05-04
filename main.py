"""Main script which executes the full ETL pipeline."""


import configparser
from haverstine_distance import udf_get_distance
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (avg, col, concat_ws, broadcast, first, substring, to_date, year)
from pyspark.sql.types import DoubleType, IntegerType, StructType
from schemas import yearly_weather_schema

config = configparser.ConfigParser()
config.read('settings.cfg')

business_path = config['PATHS']['business']
us_stations_path = config['PATHS']['us_stations']
max_distance = int(config['PARAMETERS']['max_distance'])

review_path = config['PATHS']['review']
user_path = config['PATHS']['user']

yearly_weather_path = config['PATHS']['yearly_weather']
elements_to_keep = config['PARAMETERS']['weather_elements']

output_path = config['PATHS']['output']


def main():
    """
    """
    spark = create_spark_session()

    distances = create_distances(spark, business_path, us_stations_path, max_distance)
    review = create_review(spark, review_path, user_path)
    yearly_weather = create_yearly_weather(spark, yearly_weather_path, yearly_weather_schema, elements_to_keep)
    final_table = create_final_table()

    write_final_table(final_table)

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

    Arguments:

    Returns:

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

    Arguments:

    Returns:

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
        .filter(year(col('review_date')) == 2020)
        .select(col('business_id'),
                col('user_id'),
                col('text'),
                col('review_date'))
    )

    user_review_join_condition = [user.user_id == review_raw.user_id]

    review = (review_raw
        .join(broadcast(user), on=user_review_join_condition, how='inner')
        .select(col('business_id'),
                col('text'),
                col('review_date'))
    )

    return review


def create_yearly_weather(spark,
                          yearly_weather_path: str,
                          yearly_weather_schema: StructType,
                          elements_to_keep: list) -> DataFrame:
    """
    Reads in 18 years of daily weather reports throughout the world. After filtering on US stations only, and keeping
    only the most prevalent key weather metrics, the dataframe needs to be pivotted so it can be easily joined with
    the review and distances dataframes.

    Arguments:

    Returns:
    """
    yearly_weather = (spark
        .read
        .csv(yearly_weather_path, header=False, schema=yearly_weather_schema)
        .filter(col('element').isin(elements_to_keep))
        .filter(col('station_id').startswith('US'))
        .withColumn('year', substring(col('date'), 1, 4))
        .withColumn('month', substring(col('date'), 5, 2))
        .withColumn('day', substring(col('date'), 7, 2))
        .withColumn('weather_date', to_date(concat_ws('-', col('year'), col('month'), col('day'))))
        .select(col('station_id'),
                col('weather_date'),
                col('element'),
                col('value').cast(IntegerType()))
    )

    yearly_weather_pivot = (yearly_weather
        .groupby('station_id', 'weather_date')
        .pivot('element')
        .agg(first('value'))
    )

    return yearly_weather_pivot


def create_final_table(distances: DataFrame, review: DataFrame, yearly_weather_pivot: DataFrame) -> DataFrame:
    """
    Combines the 3 dataframes from step 1-3 to create the final table.

    Arguments:

    Returns:

    """
    distance_review_join_condition = [distances.business_id == review.business_id]

    distances_review = (distances
                        .join(review, on=distance_review_join_condition, how='inner')
                        .select(col('city'),
                                col('state'),
                                col('text'),
                                col('review_date'),
                                col('us_station_id'))
                        )

    distances_review_weather_join_condition = [distances_review.review_date == yearly_weather_pivot.weather_date,
                                               distances_review.us_station_id == yearly_weather_pivot.station_id]

    final_table = (distances_review
                   .join(yearly_weather_pivot, on=distances_review_weather_join_condition, how='inner')
                   .groupby('city', 'state', 'review_date', 'text')
                   .agg(avg('PRCP'),
                        avg('SNOW'),
                        avg('SNWD'),
                        avg('TMAX'),
                        avg('TMIN'))
                   )

    return final_table


def write_final_table(final_table: DataFrame):
    """Writes the final table back to S3, you can modify the exact location in settings.cfg"""
    (final_table
     .write
     .option('compression', 'gzip')
     .mode('overwrite')
     .json(output_path)
     )


if __name__ == '__main__':
    main()
