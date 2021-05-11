"""Main script which executes the full ETL pipeline."""


import configparser
from haversine_distance import udf_get_distance
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import avg, col, concat_ws, broadcast, first, substring, to_date, year
from pyspark.sql.types import DoubleType, IntegerType
from schemas import yearly_weather_schema

config = configparser.ConfigParser()
config.read('settings.cfg')

compliment_writer = int(config['PARAMETERS']['compliment_writer'])
max_distance = int(config['PARAMETERS']['max_distance'])
s3_bucket = config['AWS']['s3_bucket']


def main():
    """
    """
    spark = create_spark_session()

    distances = create_distances(spark)
    review = create_review(spark)
    yearly_weather = create_yearly_weather(spark)
    final_table = create_final_table(distances, review, yearly_weather)

    write_final_table(final_table)

    spark.stop()


def create_spark_session():
    """Gets or creates an activate Spark session."""
    spark = (SparkSession
        .builder
        .appName("NLP-Weather-dataset ETL")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
        .getOrCreate()
    )

    return spark


def create_distances(spark) -> DataFrame:
    """
    Combines the business.json and us_stations.txt file to create a dataframe with the closest weather stations
    to the local business based on the `max_distance` parameter.

    Arguments:

    Returns:

    """
    business_path = f"s3://{s3_bucket}/yelp/business.json.gz"

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

    us_stations_path = f"s3://{s3_bucket}/ghcn/us_stations.txt.gz"

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
        .repartition(200, 'business_id')
    )

    return distances


def create_review(spark) -> DataFrame:
    """
    Subsets the raw review data based on 2 filters:
    - the business must be open
    - only review from credible users are considered (> 25 useful reviews, top 15%)

    Arguments:

    Returns:

    """
    user_path = f"s3://{s3_bucket}/yelp/user.json.gz"

    user = (spark
        .read
        .json(user_path)
        .filter(col('compliment_writer') > compliment_writer)
        .select(col('user_id'))
    )

    review_path = f"s3://{s3_bucket}/yelp/review.json.gz"

    review_raw = (spark
        .read
        .json(review_path)
        .withColumn('review_date', to_date(col('date')))
        .filter(year(col('review_date')).isin(2019, 2020, 2021))
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
        .repartition(200, 'business_id')
    )

    return review


def create_yearly_weather(spark) -> DataFrame:
    """
    Reads in 18 years of daily weather reports throughout the world. After filtering on US stations only, and keeping
    only the most prevalent key weather metrics, the dataframe needs to be pivotted so it can be easily joined with
    the review and distances dataframes.

    Arguments:

    Returns:
    """
    yearly_weather_path = f"s3://{s3_bucket}/ghcn/year_2020.csv.gz"
    elements_to_keep = ['PRCP', 'SNOW', 'SNWD', 'TMAX', 'TMIN']

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
        .repartition(200, 'station_id', 'weather_date')
    )

    yearly_weather_pivot = (yearly_weather
        .groupby('station_id', 'weather_date')
        .pivot('element')
        .agg(first('value'))
        .dropna(subset=['first(PRCP)', 'first(TMAX)', 'first(TMIN)'])
        .repartition(200, 'station_id', 'weather_date')
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
        .repartition(200, 'us_station_id', 'review_date')
    )

    distances_review_weather_join_condition = [distances_review.review_date == yearly_weather_pivot.weather_date,
                                               distances_review.us_station_id == yearly_weather_pivot.station_id]

    final_table = (distances_review
       .join(yearly_weather_pivot, on=distances_review_weather_join_condition, how='inner')
       .groupby('city', 'state', 'review_date', 'text')
       .agg(avg('PRCP').alias('prcp'),
            avg('SNOW').alias('snow'),
            avg('SNWD').alias('snwd'),
            avg('TMAX').alias('tmax'),
            avg('TMIN').alias('tmin'))
    )

    return final_table


def write_final_table(final_table: DataFrame):
    """Writes the final table back to S3, you can modify the exact location in settings.cfg"""
    output_path = f"s3://{s3_bucket}/output/"

    final_table_write = (final_table
         .write
         .option('compression', 'gzip')
         .mode('overwrite')
         .json(output_path)
     )


if __name__ == '__main__':
    main()
