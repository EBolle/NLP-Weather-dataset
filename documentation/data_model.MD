# Data model

To go from the raw data to the final table 4 steps have to be taken. Each step involves 1 or more files of the Yelp
and GHCN sources. The steps are described in detail in the same way as they are processed in the data pipeline. This 
should give you a clear idea of all the data transformations.

1. Combine the Yelp business file with the GHCN weather stations. 
2. Combine x, y, z
3. Combine a, b, c
4. Bring it all together

## 1. Combine the Yelp business file with the GHCN weather stations into the distances dataframe

The goal of this step is to retrieve weather stations in close proximity to the local businesses. Therefore we combine
the business and weather station files, and calculate the haversine distances for each business and weather station
within the same state. The relevant weather stations are filtered based on the `max_distance` parameter from `settings.cfg`.

Since `state`, `latitude`, and `longitude` are all essential for this step there are filters in place for both files in 
the spark code. 

<insert dbdiagram>.

### Haversine distance






