import sys
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

#from hadoop_settings import *
#import hadoop_settings as hs
#hs.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

def main():
        input_path=sys.argv[1]
        output_path=sys.argv[2]

        conf = SparkConf().setAppName(f"Initial Data Loading")
        sc = SparkContext(conf=conf)
        sql = SQLContext(sc)

# reading 10% of all source geo data events
        events = sql.read.parquet(f"{input_path}").sample(0.1)

# writing data to new folder 
        events\
        .write\
        .mode('overwrite')\
        .partitionBy(['date', 'event_type'])\
        .format('parquet')\
        .save(f"{output_path}")


if __name__ == "__main__":
        main()
        
#! /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/initial_data_loader.py /user/master/data/geo/events/ /user/tolique7/data/geo/events/


# hdfs dfs -ls /user/tolique7/data/geo/events/
#! hdfs dfs -rm -r /user/tolique7/data/geo/events/*   #remove all files in a hdfs folder
        