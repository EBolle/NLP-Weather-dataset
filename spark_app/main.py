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
    Main execution method which sequentially calls the methods to execute the pipeline:
    - Get or create a Spark session
    - Create the distances dataframe
    - Create the review dataframe
    - Create the pivoted yearly weather dataframe
    - Combine these 3 dataframes into the final dataframe
    - Write the final dataframe to S3
    - stop the spark session
    """
    spark = create_spark_session()

    distances = create_distances(spark)
    review = create_review(spark)
    yearly_weather = create_yearly_weather(spark)
    final_dataframe = create_final_dataframe(distances, review, yearly_weather)

    write_final_dataframe(final_dataframe)

    spark.stop()


def create_spark_session():
    """Gets or creates an activate Spark session."""
    spark = (SparkSession
        .builder
        .appName("NLP-Weather-dataset ETL")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.shuffle.file.buffer", "1m")
        .config("spark.io.compression.lz4.blockSize", "512")
        .config("spark.shuffle.registration.timeout", 12000)
        .config("spark.shuffle.registration.maxAttempts", 5)
        .getOrCreate()
    )

    return spark


def create_distances(spark) -> DataFrame:
    """
    Combines the business and stations file to create a dataframe with the closest weather stations
    to the local business based on the `max_distance` parameter.
    """
    business_path = f"s3://{s3_bucket}/yelp/yelp_academic_dataset_business.json.gz"

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

    us_stations_path = f"s3://{s3_bucket}/ghcn/stations.txt.gz"

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
    Combines the user and review files to create a new review dataframe with only open businesses and only users in the
    top 25% of complimented writers. This last constraint can be controlled with the `compliment_writer` parameter.
    """
    user_path = f"s3://{s3_bucket}/yelp/yelp_academic_dataset_user.json.gz"

    user = (spark
        .read
        .json(user_path)
        .filter(col('compliment_writer') > compliment_writer)
        .select(col('user_id'))
    )

    review_path = f"s3://{s3_bucket}/yelp/yelp_academic_dataset_review.json.gz"

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
    Reads in 3 years of daily weather reports throughout the world. After filtering on US stations only, and keeping
    only the most prevalent key weather metrics, the dataframe needs to be pivotted so it can be easily joined with
    the review and distances dataframes.
    """
    yearly_weather_path = f"s3://{s3_bucket}/ghcn/year_*"
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
        .dropna(subset=['PRCP', 'TMAX', 'TMIN'])
        .repartition(200, 'station_id', 'weather_date')
    )

    return yearly_weather_pivot


def create_final_dataframe(distances: DataFrame, review: DataFrame, yearly_weather_pivot: DataFrame) -> DataFrame:
    """
    Combines the 3 dataframes from the previous steps to create the final dataframe. This dataframe contains the not
    only the actual review and the weather metrics, but also the date, state, and city so one can easily further extend
    this final dataframe.
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
        .repartition(200)
    )

    return final_table


def write_final_dataframe(final_table: DataFrame):
    """Writes the final table back to S3."""
    output_path = f"s3://{s3_bucket}/output/"

    final_table_write = (final_table
         .write
         .option('compression', 'gzip')
         .mode('overwrite')
         .json(output_path)
     )


if __name__ == '__main__':
    main()
