import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark
findspark.init()
findspark.find()

import pyspark
import sys
import datetime
import math

from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark.context import SparkContext
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import *

    
spark = SparkSession.builder \
                    .appName("Recommendations Mart") \
                    .master("yarn") \
                    .config("spark.executor.memory", "2g") \
                    .config("spark.executor.cores", "2") \
                    .config("spark.driver.cores", "2") \
                    .config("spark.dynamicAllocation.enabled", "true") \
                    .config("spark.ui.port", "4051") \
                    .getOrCreate()

date = sys.argv[1] # '2022-05-31'
depth_days = sys.argv[2] # 35
events_path = sys.argv[3] # "/user/tolique7/data/geo/events/" 
geo_cities_path = sys.argv[4] # "/user/tolique7/geo.csv"
output_path = sys.argv[5] # "/user/tolique7/data/analytics/"

# function for calculation the distance between two geographical points
def calculate_the_distance(lat_1, lat_2, lng_1, lng_2):
    lat_1 = (math.pi / 180) * lat_1
    lat_2 = (math.pi / 180) * lat_2
    lng_1 = (math.pi / 180) * lng_1
    lng_2 = (math.pi / 180) * lng_2
    result = 2 * 6371 * math.asin(math.sqrt(math.pow(math.sin((lat_2 - lat_1) / 2), 2) + math.cos(lat_1) * math.cos(lat_2) * math.pow(math.sin((lng_2 - lng_1) / 2),2))) 
    return result

# defining a user defined function for calculating the distance 
udf_calculate_the_distance = F.udf(calculate_the_distance)

def input_paths(date, depth_days, events_path):
    d_t = datetime.datetime.strptime(date, "%Y-%m-%d")
    result = [f"{events_path}/date={(d_t-datetime.timedelta(days=day)).strftime('%Y-%m-%d')}" for day in range(int(depth_days))]
    return result

# reading the geo.csv data to get info about Australian cities
geo_cities_au = spark.read.csv(geo_cities_path, sep = ";", header = True) \
        .withColumn("lat", F.col("lat").cast(DoubleType())) \
        .withColumn("lng", F.col("lng").cast(DoubleType())) \
        .withColumnRenamed("lat", "lat_c") \
        .withColumnRenamed("lng", "lng_c")

def main():
    #reading the data on paths
    paths = input_paths(date, depth_days, events_path)
    events = spark.read.option("basePath", events_path).parquet(*paths)

    # getting the dataset with all the subscribers
    event_subscriptions = (
        events.filter(F.col("event_type") == "subscription")
        .where(
            F.col("event.subscription_channel").isNotNull()
            & F.col("event.user").isNotNull()
        )
        .select(
            F.col("event.subscription_channel").alias("channel_id"),
            F.col("event.user").alias("user_id")
        )
        .distinct()
    )

    # find users subscribed to the same channel
    cols = ["user_left", "user_right"]
    subscriptions = (
        event_subscriptions.withColumnRenamed("user_id", "user_left")
        .join(
            event_subscriptions.withColumnRenamed("user_id", "user_right"),
            on="channel_id",
            how="inner",
        )
        .drop("channel_id")
        .filter(F.col("user_left") != F.col("user_right"))
        .withColumn("hash", F.hash(F.concat(F.col("user_left"), F.col("user_right"))))
    )

    # senders and recipients of the messages
    def senders_and_recipients(message_from, message_to):
        return (
            events.filter("event_type == 'message'")
            .where(
                F.col("event.message_from").isNotNull()
                & F.col("event.message_to").isNotNull()
            )
            .select(
                F.col(message_from).alias("user_left"),
                F.col(message_to).alias("user_right"),
                F.col("lat").alias("lat_from"),
                F.col("lon").alias("lon_from"),
            )
            .distinct()
        )
    
    #finding senders
    senders = senders_and_recipients ("event.message_from", "event.message_to")

    #finding recipients
    recipients = senders_and_recipients ("event.message_to", "event.message_from")

    # find messages/subscriptions in unif time
    def events_unix (event_type):
        result = (
            events.filter(F.col("event_type") == event_type)
            .where(
                F.col("lat").isNotNull() | (F.col("lon").isNotNull()) | (
                    F.unix_timestamp(
                        F.col("event.datetime"), "yyyy-MM-dd HH:mm:ss"
                    ).isNotNull()
                )
            )
            .select(
                F.col("event.message_from").alias("user_right"),
                F.col("lat").alias("lat"),
                F.col("lon").alias("lon"),
                F.unix_timestamp(F.col("event.datetime"), "yyyy-MM-dd HH:mm:ss").alias("time")
            )
            .distinct()
        )
        return result 

    # finding messages
    event_messages_in_unix_time = events_unix("message")

    #finding subscriptions
    event_subscriptions_in_unix_time = events_unix("subscription")
    
    w = Window.partitionBy("user_right")
    
    #latest messages / subscription
    def event_results(events_filtered_unix):
        result = (
            events_filtered_unix.withColumn("maxdatetime", F.max("time").over(w))
            .where(F.col("time") == F.col("maxdatetime"))
            .select("user_right", "lat", "lon", "time")
        )
        return result
    
    # latest messages
    event_messages = event_results(event_messages_in_unix_time)

    #latest subscriptions
    event_subscriptions = event_results(event_subscriptions_in_unix_time)
    
    # unite latest messages and subscriptions together
    event_coordinates = event_messages.union(event_subscriptions).distinct()
    
    # finding users who were in contact with each other
    users_intersection = (
        senders.union(recipients)
        .withColumn("arr", F.array_sort(F.array(*cols)))
        .drop_duplicates(["arr"])
        .withColumn("hash", F.hash(F.concat(F.col("user_left"), F.col("user_right"))))
        .filter(F.col("user_left") != F.col("user_right"))
        .select("user_left", "user_right", "lat_from", "lon_from", "hash")
    )

    # finding users who were not in contact with each other and subscribed to one channel 
    subscriptions_without_intersection = (
        subscriptions.join(
            users_intersection.withColumnRenamed(
                "user_right", "user_right_temp"
            ).withColumnRenamed("user_left", "user_left_temp"),
            on=["hash"],
            how="left",
        )
        .where(F.col("user_right_temp").isNull())
        .where(F.col("user_left") != 0)
        .filter(F.col("user_left") != F.col("user_right"))
        .select("user_left", "user_right", "lat_from", "lon_from")
    )
    
    # finding the latest coordinates of the users
    event_subscription_coordinates = (
        subscriptions_without_intersection.join(
            event_coordinates.withColumnRenamed(
                "user_id", "user_left"
            ).withColumnRenamed("lon", "lon_left")
            .withColumnRenamed("lat", "lat_left"),
            on=["user_right"],
            how="inner",
        ).join(
            event_coordinates.withColumnRenamed(
                "user_id", "user_right"
            ).withColumnRenamed("lon", "lon_right")
            .withColumnRenamed("lat", "lat_right"),
            on=["user_right"],
            how="inner",
        )
    )
    
    # users who did not have contacts whithin a year
    distance = (
        event_subscription_coordinates.withColumn(
            "distance",
            udf_calculate_the_distance(
                F.col("lat_left"),
                F.col("lat_right"),
                F.col("lon_left"),
                F.col("lon_right"),
            ).cast(DoubleType()),
        )
        .where(F.col("distance") <= 1.0)
        .withColumnRenamed("lat_left", "lat")
        .withColumnRenamed("lon_left", "lon")
        .drop("lat_from", "lon_from", "distance")
        #.select("user_right", "user_left", "lat", "lon", "time", "lat_right", "lon_right", "time")
    )
    
    # find a city
    users_city = (
        distance.crossJoin(geo_cities_au.hint("broadcast"))
        .withColumn(
            "distance",
            udf_calculate_the_distance(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lng_c")).cast(
                DoubleType()
            ),
        )
        .withColumn(
            "row",
            F.row_number().over(
                Window.partitionBy("user_left", "user_right").orderBy(
                    F.col("distance").asc()
                )
            ),
        )
        .filter(F.col("row") == 1)
        .drop("row", "lon", "lat", "city_lon", "city_lat", "distance", "channel_id")
        .withColumnRenamed("city", "zone_id")
        .distinct()
    )
    
    # recomendations according рекоммендации по подпискам пользователей
    recommendations = (
        users_city.withColumn("processed_dttm", F.current_date())
        .withColumn(
            "local_datetime",
            F.from_utc_timestamp(F.col("processed_dttm"), F.col("timezone")),
        )
        .withColumn("local_time", F.date_format(F.col("local_datetime"), "HH:mm:ss"))
        .select("user_left", "user_right", "processed_dttm", "zone_id", "local_time")
    )
    
    # writing data to parquest format
    recommendations.write.mode("overwrite").parquet(
        f"{output_path}/mart/recommendations/"
    )          


if __name__ == "__main__":

    main()