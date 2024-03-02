import os

os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/usr/local/lib/python3.8"

import findspark
findspark.init()
findspark.find()



ssh -i /Users/anatoly/yandex.practicum/ssh_key -ND 8157 yc-user@51.250.70.57


/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/nst_olgt.py 2022-05-31 10 /user/tolique7/data/geo/events/ /user/tolique7/geo.csv /user/tolique7/data/analytics/


# sprk-submit for testing
# /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/users_mart.py 2022-05-31 10 hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/tolique7/data/geo/events/ hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/tolique7/geo.csv hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/tolique7/data/analytics/



#cleanup


!hdfs dfs -copyFromLocal /lessons/geo.csv /user/tolique7/geo.csv


# hdfs dfs -ls /user/tolique7/data/geo/events/
#! hdfs dfs -rm -r /user/tolique7/data/geo/events/*   #remove all files in a hdfs SOURCE folder
#! hdfs dfs -rm -r /user/tolique7/data/analytics/mart/users/



 hdfs dfs -rm /user/tolique7/geo.csv
hdfs dfs -cat /user/tolique7/geo.csv

!ls -la
!pwd
!hdfs dfs -ls /user/tolique7
!hdfs dfs -rm /user/tolique7/geo.csv
!hdfs dfs -copyFromLocal geo.csv /user/tolique7/
!hadoop fs -put geo.csv /user/tolique7/
!hdfs dfs -ls /user/tolique7/
!hdfs -get /user/tolique7/geo.csv /lessons/
!hdfs dfs -cat /user/tolique7/geo.csv
cat /lessons/geo.csv
!ls -la
!chmod 777 /lessons/geo.csv


hdfs dfs -put /lessons/geo.csv /user/tolique7/geo.csv