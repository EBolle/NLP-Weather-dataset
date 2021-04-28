"""This module contains the schema of the song and log data on S3."""


from pyspark.sql.types import StructType, StringType


yearly_weather_schema = (StructType([
    StructField('station_id', StringType(), True),
    StructField('date', StringType(), True),
    StructField('element', StringType(), True),
    StructField('value', StringType(), True),
    StructField('measurement', StringType(), True),
    StructField('quality', StringType(), True),
    StructField('source', StringType(), True),
    StructField('obs_time', StringType(), True),
]))
