# Проектная работа по организации Data Lake #7
Соцсеть, для которой построили Data Lake, развивается — пробный запуск в Австралии оказался удачным. Команда готовится к разработке обновлений. Значит, структуру хранилища предстоит переделать, а данные — дополнить. Этим и займёмся в проекте.

## Описание задачи
Коллеги из другого проекта по просьбе нашей команды начали вычислять координаты событий (сообщений, подписок, реакций, регистраций), которые совершили пользователи соцсети. Значения координат будут появляться в таблице событий. Пока определяется геопозиция только исходящих сообщений, но уже сейчас можно начать разрабатывать новый функционал. 

В продукт планируют внедрить систему рекомендации друзей. Приложение будет предлагать пользователю написать человеку, если пользователь и адресат:
* состоят в одном канале,
* раньше никогда не переписывались,
* находятся не дальше 1 км друг от друга.

При этом команда хочет лучше изучить аудиторию соцсети, чтобы в будущем запустить монетизацию. Для этого было решено провести геоаналитику:
* Выяснить, где находится большинство пользователей по количеству сообщений, лайков и подписок из одной точки.
* Посмотреть, в какой точке Австралии регистрируется больше всего новых пользователей.
* Определить, как часто пользователи путешествуют и какие города выбирают.

Благодаря такой аналитике в соцсеть можно будет вставить рекламу: приложение сможет учитывать местонахождение пользователя и предлагать тому подходящие услуги компаний-партнёров. 


## Структура репозитория

## Витрины данных
### Витрина пользователей - users_mart.py

Расположение: /user/tolique7/data/analytics/mart/users/

* **user_id** - идентификатор пользователя
* **act_city** - актуальный адрес. Это город, из которого было отправлено последнее сообщение
* **home_city** - домашний адрес. Это последний город, в котором пользователь был дольше 27 дней
* **travel_count** — количество посещённых городов. Если пользователь побывал в каком-то городе повторно, то это считается за отдельное посещение.
* **travel_array** — список городов в порядке посещения.
* **local_time** — местное время. Время последнего события пользователя, о котором у нас есть данные с учётом таймзоны геопозициии этого события.

### Витрина зон - geo_locations_mart.py

Расположение: /user/tolique7/data/analytics/mart/geo/

* **month** — месяц расчёта
* **week** — неделя расчёта
* **zone_id** — идентификатор зоны (города)
* **week_message** — количество сообщений за неделю
* **week_reaction** — количество реакций за неделю
* **week_subscription** — количество подписок за неделю
* **week_user** — количество регистраций за неделю
* **month_message** — количество сообщений за месяц
* **month_reaction** — количество реакций за месяц
* **month_subscription** — количество подписок за месяц
* **month_user** — количество регистраций за месяц

### Витрина рекомендаций друзей - recommendations_mart.py

Расположение: /user/tolique7/data/analytics/mart/recommedations/

* **user_left** — первый пользователь;
* **user_right** — второй пользователь;
* **processed_dttm** — дата расчёта витрины;
* **zone_id** — идентификатор зоны (города);
* **local_time** — локальное время.

## Используемые технологии и инструменты
DataLake, Python, PySpark, HDFS, Airflow, Hadoop, Jupyter Notebook

## Технические заметки
### Файл с координатами
Файл с координатами городов Австралии, которые аналитики собрали в одну таблицу — geo.csv
Изначально файл расположен по адресу <https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv>
Но потом Олег предоставил правильный файл с колонкой timezones.
Именно он был загружен в папку /lessons через Jupyter Notebook, а потом скопирован в HDFS: 
```
!hdfs dfs -copyFromLocal /lessons/geo.csv /user/tolique7/geo.csv
```
Примечание: Можно воспользоваться интерфейсом HDFS Namenode (ссылка дается в телеграм-боте)
Hadoop > Utilities > Browse the file system и прям с локального компьютера загрузить необходимый файл в HDFS.

### Структура файлов проекта
Внутри `src` расположены две папки:
```
/src/dags
/src/scripts
```

### Параметры подключения в Airflow
```
Airflow > Admin > Connections
Connection Id: spark_yarn
Connection Type: Spark
Host: yarn
```

### Шпаргалка и записи для справки

**pyspark.sql.functions.col("name")** - возвращает колонку с указанным именем

**pyspark.sql.functions.udf(func)** - объявление user defined function

**pyspark.sql.functions.date_trunc(format, timestamp)** -  

**pyspark.sql.functions.coalesce(col)** - например cDf.select('*', coalesce(cDf["a"], lit(0.0))).show() показывать 0.0 вместо null

**pyspark.sql.functions.count(col)** - агрегатная функция считает количество по группе

**pyspark.sql.functions.filter()** - фильтрация массива по функции которая возвращает True

**pyspark.sql.functions.collect_list()** - возвращает список объектов

**pyspark.sql.functions.from_utc_timestamp(timestamp, timezone)** - возвращает метку времени в формате UTC в часовом поясе timezone

**pyspark.sql.functions.to_timestamp(col, format)** - функция, которая преобразует строку в метку времени. Принимает два аргумента: строку для преобразования и формат строки. 

**pyspark.sql.functions.current_date()** - возвращает текущую дату

**pyspark.sql.functions.hash(cols)** - рассчет хэша

**pyspark.sql.functions.unix_timestamp(timestamp, format)** - перевести строку в нужном формате в unix-время

**pyspark.sql.functions.concat(cols)** - слить несколько колонок в одну (без разделителей)

**pyspark.sql.functions.array_join()** - слить несколько колонок в одну с разделителем

**pyspark.sql.DataFrame.withColumn("col_name")** - вернуть датафрейм с добавленной колонкой

**pyspark.sql.DataFrame.withColumnRenamed("old_col_name", "new_col_name")** - вернуть датафрейм с переименнованной колонкой 

**pyspark.sql.DataFrame.alias("")** - создание псевдонима

**pyspark.sql.DataFrame.drop(cols)** - вернуть новый датафрейм без указанной колонки

**pyspark.sql.DataFrame.selectExpr(expr)** - позволяет использовать sql выражения

**pyspark.sql.DataFrame.fillna()** или **na.fill()** - заменяет одни значения на другие
У нас использовалось fillna(0) что меняет все значения null на 0.

**pyspark.sql.DataFrame.groupBy** - группировка 
```
df = spark.createDataFrame([(2, "Alice"), (2, "Bob"), (2, "Bob"), (5, "Bob")], schema=["age", "name"])
df.groupBy("name").agg({"age": "sum"}).sort("name").show()
```

**pyspark.sql.Column.cast()** — это функция из класса Column, которая используется для преобразования типа столбца в другой тип данных.

**Чтение csv**
```
spark = spark.read.csv(geo_cities_path, sep = ";", header = True) \
        .withColumn("lat", F.col("lat").cast(DoubleType())) \
        .withColumnRenamed("lat", "lat_c")
```



**Запись а parquet**
```
recommendations.write.mode("overwrite").parquet(f"{output_path}/mart/recommendations/") 
```

**sprk-submit for testing:**
```
/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/users_mart.py 2022-05-31 30 /user/tolique7/data/geo/events/ /user/tolique7/geo.csv /user/tolique7/data/analytics/
```

**Параметры вызовов из командной строки**
```
date = sys.argv[1] # '2022-05-31'
depth_days = sys.argv[2] # 35
events_path = sys.argv[3] # "/user/tolique7/data/geo/events/" 
geo_cities_path = sys.argv[4] # "/user/tolique7/geo.csv"
output_path = sys.argv[5] # "/user/tolique7/data/analytics/"
```

**Настройки Spark'а, которые корректно отрабатывали на инфраструктуре:**
```
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
```