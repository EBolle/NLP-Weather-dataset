"""
The logic in this module to compute the Haversine distance must be fully credited to
https://medium.com/@nikolasbielski/using-a-custom-udf-in-pyspark-to-compute-haversine-distances-d877b77b4b18
"""


from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import udf


def get_distance(longit_a, latit_a, longit_b, latit_b):
    # Transform to radians
    longit_a, latit_a, longit_b, latit_b = map(radians, [longit_a, latit_a, longit_b, latit_b])
    dist_longit = longit_b - longit_a
    dist_latit = latit_b - latit_a

    # Calculate area
    area = sin(dist_latit / 2) ** 2 + cos(latit_a) * cos(latit_b) * sin(dist_longit / 2) ** 2

    # Calculate the central angle
    central_angle = 2 * asin(sqrt(area))
    radius = 6371

    # Calculate Distance
    distance = central_angle * radius

    return abs(round(distance, 2))


udf_get_distance = udf(get_distance)