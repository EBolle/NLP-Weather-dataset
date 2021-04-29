# Data model

To go from the raw data to the final table 4 steps have to be taken. Each step involves 1 or more files of the Yelp
and GHCN sources. The steps are described in detail in the same way as they are processed in the data pipeline. This 
should give you a clear idea of all the data transformations.

1. Combine the Yelp business file with the GHCN weather stations. 
2. Prepare the Yelp review file with the help of the Yelp user file 
3. Prepare the yearly GHCN weather data
4. Bring it all together

## 1. Combine the Yelp business file with the GHCN weather stations into the distances dataframe

The goal of this step is to retrieve weather stations in close proximity to the local businesses. Therefore we combine
the business and weather station files, and calculate the haversine distances for each business and weather station
within the same state. The relevant weather stations are filtered based on the `max_distance` parameter from `settings.cfg`.

Since `state`, `latitude`, and `longitude` are all essential for this step there are filters in place for both files in 
the Spark code. 

<insert dbdiagram>.

### Haversine distance

Since we are calculating the distance between 2 geographical coordinates on a sphere (the Earth) we choose to use the
haversine distance. All credits should go to [this medium post](haversine), I merely applied the logic. 

### Challenges

Although this part seems straightforward, it actually comes with a computational challenge. Since we join these 2 tables
on `state`, the resulting dataframe becomes large.

n_rows after filtering and cleaning:
- business.json: 123.248
- us_weather_stations.txt: 65.170

n_rows after joining on state: 228.499.768

The computational challenge lies in the fact that for > 228M rows we need to calculate the haversine distance, with a
user-defined function (udf). UDFs are notorious bottlenecks in Spark code but we cannot circumvent this function without
sacrificing too much data quality. On the Spark cluster it took ~x minutes to create this dataframe.

## 2. Prepare the Yelp review file with the help of the Yelp user file 







[haversine]: https://medium.com/@nikolasbielski/using-a-custom-udf-in-pyspark-to-compute-haversine-distances-d877b77b4b18