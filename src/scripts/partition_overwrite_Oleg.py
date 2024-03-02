import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F


def main():
    date = sys.argv[1]
    base_input_path = sys.argv[2]
    base_output_path = sys.argv[3]

    conf = SparkConf().setAppName(f"EventsPartitioningJob-{date}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    events = sql.read.json(f"{base_input_path}/date={date}")

    writer = partition_writer(events)

    writer.save(f'{base_output_path}/date={date}')


def partition_writer(events):
    return events \
        .write \
        .mode('overwrite') \
        .partitionBy('event_type') \
        .format('parquet')


if __name__ == "__main__":
    main()



# /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/partition_overwrite.py 2022-05-04  /user/vasilyevol/data/events /user/vasilyevol/analytics/user_interests_d5_TEST