import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark
findspark.init()
findspark.find()

import math
import datetime
import pyspark
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.context import SparkContext
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import *


# spark settings
spark = SparkSession.builder \
    .appName("Users Mart") \
    .master("yarn") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.cores", "2") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
    .config("spark.ui.port", "4051") \
    .getOrCreate()

date = sys.argv[1] # '2022-05-31'
depth_days = sys.argv[2] # 35
events_path = sys.argv[3] # "/user/tolique7/data/geo/events/" 
geo_cities_path = sys.argv[4] # "/user/tolique7/geo.csv"
output_path = sys.argv[5] # "/user/tolique7/data/analytics/"

# sprk-submit for testing
# /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/users_mart.py 2022-05-31 10 hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/tolique7/data/geo/events/ hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/tolique7/geo.csv hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/tolique7/data/analytics/


# reading the geo.csv data to get info about Australian cities
geo_cities_au = spark.read.csv(geo_cities_path, sep = ";", header = True) \
        .withColumn("lat", F.col("lat").cast(DoubleType())) \
        .withColumn("lng", F.col("lng").cast(DoubleType())) \
        .withColumnRenamed("lat", "lat_c") \
        .withColumnRenamed("lng", "lng_c")

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


def main():
    spark = (
        SparkSession.builder.master("local")
        .appName("Users Mart")
        .getOrCreate()
    )

    paths_for_processing = input_paths(date, depth_days, events_path)

    all_events = spark.read.option("basePath", events_path).parquet(*paths_for_processing)

    # getting the dataset with all the event messages
    all_event_messages = (
        all_events.where(F.col("event_type") == "message")
        .withColumn(
            "date",
            F.date_trunc(
                "day",
                F.coalesce(F.col("event.datetime"), F.col("event.message_ts")),
            ),
        )
        .selectExpr(
            "event.message_from as user_id",
            "event.message_id",
            "date",
            "event.datetime",
            "lat",
            "lon",
        )
    )


    # dataset with all messages, send date, city and a time zone
    messages_and_cities = (
        all_event_messages.crossJoin(geo_cities_au)
        .withColumn(
            "distance",
            udf_calculate_the_distance(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lng_c")).cast(
            "float"
            ),
        )
        .withColumn(
            "distance_rank",
            F.row_number().over(
                Window().partitionBy(["user_id", "message_id"]).orderBy("distance")
            ),
        )
        .where("distance_rank == 1")
        .select ("user_id", "message_id", "date", "datetime", "city", "timezone")
    )
    

    # getting the current address according to the last message sent
    current_city_by_last_message = (
        all_event_messages.withColumn(
            "datetime_rank",
            F.row_number().over(
                Window().partitionBy(["user_id"]).orderBy(F.desc("datetime"))
            ),
        )
        .where("datetime_rank == 1")
        .orderBy("user_id")
        .crossJoin(geo_cities_au)
        .withColumn(
            "distance",
            udf_calculate_the_distance(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lng_c")).cast("float"),
        )
        .withColumn(
            "distance_rank",
            F.row_number().over(
                Window().partitionBy(["user_id"]).orderBy(F.asc("distance"))
            ),
        )
        .where("distance_rank == 1")
        .select("user_id", F.col("city").alias("act_city"), "date", "timezone")
    ) 


    # temporary table with the list of different cities from which messages were sent (for every user separatelly) 
    cities_list_of_users = (
        messages_and_cities.withColumn(
            "max_date", F.max("date").over(Window().partitionBy("user_id"))
        )
        .withColumn(
            "city_lag",
            F.lead("city", 1, "empty").over(
                Window().partitionBy("user_id").orderBy(F.col("date").desc())
            ),
        )
        .filter(F.col("city") != F.col("city_lag"))
        .select("user_id", "message_id", "date", "datetime", "city", "timezone", "max_date", "city_lag") 
    )


    # getting city address from which user was sending the messages within 27 days period
    home_city = (
        cities_list_of_users.withColumnRenamed("city", "home_city")
        .withColumn(
            "date_lag",
            F.coalesce(
                F.lag("date").over(
                    Window().partitionBy("user_id").orderBy(F.col("date").desc())
                ),
                F.col("max_date"),
            ),
        )
        .withColumn("date_diff", F.datediff(F.col("date_lag"), F.col("date")))
        .where(F.col("date_diff") > 27)
        .withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("user_id").orderBy(F.col("date").desc())
            ),
        )
        .where(F.col("rank") == 1)
        .select("user_id", "home_city")
    )


    # quantity of changing the city by a user
    travel_list = cities_list_of_users.groupBy("user_id").agg(
        F.count("*").alias("travel_count"),
        F.collect_list("city").alias("travel_array")
    )


    # calculating the local time
    local_time = current_city_by_last_message.withColumn(
        "localtime", F.from_utc_timestamp(F.col("date"), F.col("timezone"))
    ).select("user_id", "localtime")


    # binding all the data to the final mart
    final_mart = (
        current_city_by_last_message
        .join(home_city, "user_id", "left")
        .join(travel_list, "user_id", "left")
        .join(local_time, "user_id", "left")
        .select(
            "user_id",
            "act_city",
            "home_city",
            "travel_count",
            "travel_array",
            "localtime",
        )
    )

    # writing the result to parquet
    final_mart.write.mode("overwrite").parquet(
        f"{output_path}/mart/users/_{date}_{depth_days}"
    )


if __name__ == "__main__":
    main()