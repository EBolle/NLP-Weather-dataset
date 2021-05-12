# Data model

To get to the nlp-weather dataset which NLP researchers can use for actual analysis, quite a few steps have to be taken. This page explains
each step in detail. The guideline for this page is the `main.py` Spark script.

1. Create a distances dataframe
2. Create the review dataframe
3. Create the yearly weather dataframe
4. Bring it all together

It must be said that there a 2 strong assumptions in this project that you must take into account.

- The reviews are written by users, not by businesses. We do not have the actual longitude and latitude coordinates of 
the place where the review was written, but we assume the review was written in relatively close proximity to the business
location.
- Although we know the date of the review and the weather, we do not have the timestamps. The weather is therefore an
average of the weather of that day, not on the exact moment of writing.

## 1. Create a distances dataframe

For each business location we need to know which weather stations are in closest proximity. The proximity is calculated
with the help of the [haversine distance][haversine_wiki], and controlled by the the `max_distance` parameter in
`settings.cfg`. There are 2 steps involved to create the distances dataframe

- Join the Yelp business file and the GHCN weather stations file 
- Calculate the haversine distance for each combination of business and weather station

Although both files are relatively small (160.585 rows and 118.493 rows respectively), a full join would result in a huge
dataframe which would seriously slow down the performance. Therefore, and mostly for logical reasons, the 2 files are 
joined based on state. This join condition allows for relevant and fewer distance calculations. Ideally, you would also
take weather stations into account that are just over the border in another state, but this is out of scope for this 
project.

Since both files are small the join itself is computationally not that expensive since we can broadcast the stations 
file to each node. The haversine distance did present a challenge since it is a user-defined function (UDF). This is a
common bottleneck in Spark performance but to my knowledge there is not a way to work around this, except to keep the
number of calculations to a minimum. Please note that I have not developed the UDF myself, all the credits should go to
[this medium post][haversine_medium], I have merely applied the logic.

## 2. Create the review dataframe

This step is straightforward. To keep only the relevant reviews for the NLP research a join is performed with the user
file to keep only the most complimented users in scope. This increases the data quality while also making sure that only
relevant data is processed. This same logic is applied on the years of reviews that are taken into consideration
(2019-2021, instead of all the reviews from 2004 onwards). 

## 3. Create the yearly weather dataframe

Although there are no joins in this step, up to this point it is computationally the most expensive step. The cleaning 
and transformation steps of the weather data are straightforward, until the 'pivot' of the dataframe. 

Basically the original data is in long format, while we want the data in wide format. The reason for this is that we 
want 1 row per review with the (aggregated) weather data, since most data-science algorithms (including NLP) operate 
on this type of data format (1 row is 1 observation, 1 column is the dependent variable, the others are independent).

Since the data is imported into partitions in an unordered fashion, this step creates *a lot* of shuffle operations.
The combination of the columns `station_id` and `weather_date` needs to be searched, aggregated and send to new partitions
to do the first() calculation for each element per combination. A solution to increase the speed of this step is to
partition the data by `station_id` and `weather_date` before doing the `pivot`. However, since this dataframe is only 
used once, it does not speed up the calculations. Another solution would be to store the data in .parquet form on S3,
already partitioned by `station_id` and `weather_date` and then imported. However, the overhead of creating these
partitions caused by the massive I/O operations is not worth it.

### 4. Bring it all together

The last step but definitely not least. There are 3 expensive operations in this final step:

- join the distances dataframe with the review dataframe into a new distances_review dataframe
- join the newly created distances_review dataframe with the pivoted weather dataframe into the final table
- aggregate the final table based on `('city', 'state', 'review_date', 'text')` and calculate the average value for
each of these elements

All together it takes ~20 minutes to process these steps with the EMR setup as mentioned in the README. Although there are a 
few optimization improvements I have made with Spark configuration settings and filters, the real improvements (e.g., 
partitioning, storing and importing the data in the right partitions) are not worth it given that this is not a 
recurring pipeline and relatively small. 

Regardless, the end result is an interesting dataset on which numerous (NLP) analysis can be performed, e.g.:

- Sentiment analysis: does the weather influence the mood of the user that is writing the review? 
- Text analysis: can you determine in which state someone lives dependent on their review?
- Text analysis: are their lexical differences between cities in 1 state?

The columns `('city', 'state', 'review_date')` were left in the dataset on purpose, so it would be easy for researchers
to further extend the dataset with other (meta) data. 

[haversine_wiki]: https://en.wikipedia.org/wiki/Haversine_formula
[haversine_medium]: https://medium.com/@nikolasbielski/using-a-custom-udf-in-pyspark-to-compute-haversine-distances-d877b77b4b18