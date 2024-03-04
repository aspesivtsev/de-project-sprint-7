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
from pyspark import SparkConf
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import *


spark = SparkSession.builder \
    .appName("Geo Locations Mart") \
    .master("yarn") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.cores", "2") \
    .config("spark.ui.port", "4051") \
    .getOrCreate()

date = sys.argv[1] # '2022-05-31'
depth_days = sys.argv[2] # 35
events_path = sys.argv[3] # "/user/tolique7/data/geo/events/" 
geo_cities_path = sys.argv[4] # "/user/tolique7/geo.csv"
output_path = sys.argv[5] # "/user/tolique7/data/analytics/"

# function for calculation the distance between two geographical points
def calculate_the_distance(lat_1, lat_2, lng_1, lng_2):
    lat_1 = math.radians(lat_1) #(math.pi / 180) * lat_1
    lat_2 = math.radians(lat_2) #(math.pi / 180) * lat_2
    lng_1 = math.radians(lng_1) #(math.pi / 180) * lng_1
    lng_2 = math.radians(lng_2) #(math.pi / 180) * lng_2
    result = 2 * 6371 * math.asin(math.sqrt(math.pow(math.sin((lat_2 - lat_1) / 2), 2) + math.cos(lat_1) * math.cos(lat_2) * math.pow(math.sin((lng_2 - lng_1) / 2),2))) 
    return result  


# defining a user defined function for calculating the distance 
udf_calculate_the_distance = F.udf(calculate_the_distance)

# list of paths
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
   
    # function to separate dataset according to type of the event (message / reaction / subscription)
    def event_types(event_type):    
        result = (
            events.where(F.col("event_type") == event_type)
            .where(F.col("lat").isNotNull() & (F.col("lon").isNotNull()))
            .select(
                F.col("event.message_from").alias("user_id"),
                F.col("event.message_id").alias("message_id"),
                "lat",
                "lon",
                F.to_date(F.coalesce(F.col("event.datetime"), F.col("event.message_ts")),
                ).alias("date")
            )
        )
        return result 

    # get the dataset with all messages
    event_messages = event_types("message")

    # get the dataset with all reactions
    event_reactions = event_types("reaction") 

    # # get the dataset with all subscription
    event_subscriptions = event_types("subscription")
    
    # adding names of the cities to all previous data sets
    cols = ["user_id", "message_id", "lat", "lon", "date", "city", "lat_c", "lng_c", "timezone"]
    all_messages = event_messages.crossJoin(geo_cities_au.hint("broadcast")).select(cols)
    all_reactions = event_reactions.crossJoin(geo_cities_au.hint("broadcast")).select(cols)
    all_subscriptions = event_subscriptions.crossJoin(geo_cities_au.hint("broadcast")).select(cols)    
    
    # adding column with distance (according to users' coordinates) to the previous datasets
    messages_distances = all_messages.withColumn(
        "distance",
        udf_calculate_the_distance(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lng_c")).cast(DoubleType())
    )

    reactions_distances = all_reactions.withColumn(
        "distance",
        udf_calculate_the_distance(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lng_c")).cast(DoubleType())
    )

    subscriptions_distances = all_subscriptions.withColumn(
        "distance",
        udf_calculate_the_distance(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lng_c")).cast(DoubleType())
    )
    

    # function which finds the coordinates of the city, from which the particular user sent the lates message/reaction/subscription
    def latest_event (event_dataset):
        result = ( 
            event_dataset.withColumn("row", F.row_number().over(Window.partitionBy("user_id", "date", "message_id").orderBy(F.col("distance").asc())),)
            .filter(F.col("row") == 1)
            .select("user_id", "message_id", "date", "city")
            .withColumnRenamed("city", "zone_id")
            )
        return result 

    # getting coordinates of the city from which the user sent the latest message
    latest_message_dataset = latest_event(messages_distances)

    # getting coordinates of the city from which the user sent the latest reaction
    latest_reaction_dataset = latest_event(reactions_distances)

    # getting coordinates of the city from which the user sent the latest message subscription
    latest_subscription_dataset = latest_event(subscriptions_distances)    
    
    # counting events on the city in scope of weeks and months
    def count_events (dataset, week_action, month_action):
        result = (
            dataset.withColumn("month", F.month(F.col("date")))
            .withColumn("week", F.weekofyear(F.to_date(F.to_timestamp(F.col("date")), "yyyy-MM-dd")))
            .withColumn(week_action,(F.count("message_id").over(Window.partitionBy("zone_id", "week"))),)
            .withColumn(month_action, (F.count("message_id").over(Window.partitionBy("zone_id", "month"))),)
            .select("zone_id", "week", "month", week_action, month_action)
            .distinct()
            )
        return result 
    
    #count quantity of the messages
    count_messages = count_events(latest_message_dataset, "week_message", "month_message")

    #count quantity of the reactions
    count_reactions = count_events(latest_reaction_dataset, "week_reaction", "month_reaction")

    #count quantity of the subscriptions
    count_subscriptions = count_events(latest_subscription_dataset, "week_subscription", "month_subscription")

    #count quantity of the registrations
    count_registrations = (
        latest_message_dataset.withColumn("month", F.month(F.col("date")))
        .withColumn("week", F.weekofyear(F.to_date(F.to_timestamp(F.col("date")), "yyyy-MM-dd")))
        .withColumn("row", (F.row_number().over(Window.partitionBy("user_id").orderBy(F.col("date").asc()))),)
        .filter(F.col("row") == 1)
        .withColumn("week_user", (F.count("row").over(Window.partitionBy("zone_id", "week"))))
        .withColumn("month_user", (F.count("row").over(Window.partitionBy("zone_id", "month"))))
        .select("zone_id", "week", "month", "week_user", "month_user")
        .distinct()
    )

    # binding all the data to the final mart
    geo_mart = (
        count_messages.join(count_registrations, ["zone_id", "week", "month"], how="full")
        .join(count_reactions, ["zone_id", "week", "month"], how="full")
        .join(count_subscriptions, ["zone_id", "week", "month"], how="full")
    )

    # Fill in zeroes instead of nulls
    geo_mart = geo_mart.fillna(0)  
    
    # writing data
    geo_mart.write.mode("overwrite").parquet(f"{output_path}/mart/geo")



if __name__ == "__main__":
    main()