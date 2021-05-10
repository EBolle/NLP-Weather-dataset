# Data model

To get to the nlp-weather dataset which NLP researchers can use, quite a few steps have to be taken. This page explains
each step in detail. The guideline for this page is the `main.py` spark script.

1. Create a distances dataframe
2. Create the review dataframe
3. Create the yearly weather dataframe
4. Bring it all together

It must be said that there a 2 strong assumptions in this project that you must take into account.

- The reviews are written by users, not by businesses. We do not have the actual longitude and latitude coordinates of 
the place where the review was written, but we assume the review was written in relatively close proximity to the business
location
- Although we know the date of the review and the weather, we do not have the timestamps. The weather is therefore an
average of the weather of that day, not on the exact moment of writing

## 1. Create a distances dataframe

For each business location we need to know which weather stations are in closest proximity. The proximity is calculated
with the help of the [haversine distance][haversine_wiki], and controlled by the the `max_distance` parameter in
`settings.cfg`. There are 2 steps involved to create the distances dataframe

- Join the Yelp business file and the GHCN weather stations file 
- Calculate the haversine distance for each combination of business and weather station

Although both files are relatively small (160.585 rows and 118.493 rows respectively), a full join would result in a huge
dataframe which would seriously slow down the performance. Therefore, and mostly for logical reasons, the 2 files are 
joined based on state. This join condition allows for relevant and far less distance calculations. Ideally, you would also
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
relevant data is processed. This same logic is applied on the years of reviews that are taken into consideration (2019
-2021, instead of all the reviews from 2004 onwards). 

## 3. Create the yearly weather dataframe










## 3. Prepare the yearly GHCN weather data

The goal of this step is to retrieve daily weather data between 2004-2021 in a format so it can be easily used to join
on the review and the distance data. After joining it should be easy to aggregate the key weather metrics in order to
create the final table. To prepare the GHCN daily weather data no joins were needed, but this step is computationally
the most difficult one.

### Challenges

The first challenge is to get the data in the correct shape, a few things needed to be done:

- only retrieve weather data from the US
- make sure `state`, `latitude`, and `longitude` are not null
- wrangle the date string (e.g., '20200401') to the same actual data format as the review file (2020-04-01)
- keep only the top weather 6 weather metrics to prevent extreme sparsity in the final table and unnecessary computations

The second challenge is to get the data from a long to a wide format. In the final step we want to have all the weather
information on 1 row for 1 review on 1 specific date. Therefore we need to pivot the table. Although code wise this is 
easy, it is actually a very expensive transformation. Since this is also the step were most data is processed 
(+20GB uncompressed) it takes quite a while to get all the data in the correct shape. On the Spark cluster it took ~y
minutes.

When these steps are completed, this is the transformation.

<show table old format>
<show table new format>

## 4. Bring it all together

Finally, we can join the output from the first 3 steps to create the final table. Basically it is nothing more than a join
and an aggregation.

<show end result>.


[haversine_wiki]: https://en.wikipedia.org/wiki/Haversine_formula
[haversine_medium]: https://medium.com/@nikolasbielski/using-a-custom-udf-in-pyspark-to-compute-haversine-distances-d877b77b4b18