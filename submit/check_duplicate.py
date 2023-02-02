import findspark
findspark.init()
findspark.add_packages("org.mongodb.spark:mongo-spark-connector-10.0.5")

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F

spark = SparkSession \
  .builder \
  .appName("lemuck") \
  .master("local[*]") \
  .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.5')\
  .config("spark.executor.memory", "4g").config('spark.driver.memory', '2g').config('spark.driver.cores', '4')\
  .config("spark.memory.offHeap.enabled","true") \
  .config("spark.memory.offHeap.size","10g")\
  .enableHiveSupport().getOrCreate()

userEventDf = spark.read\
    .format('mongodb')\
    .option("connection.uri", "mongodb+srv://phinguyen:QtURxdQf3xeu73jE@taptap-datastream-test.6te3s.mongodb.net") \
    .option("database", "universal_db")\
    .option("collection", "user_event")\
    .option("spark.mongodb.read.partitioner.options.partition.size", 64) \
    .option("spark.mongodb.read.readPreference.name", "primaryPreferred")\
    .load()#.limit(100000)

val = userEventDf\
  .filter(userEventDf.eventType=="Coupon")\
  .select("brandCode","storeCode","params")\
  .groupBy("params.offerId")\
  .agg(F.collect_set('brandCode').alias('brandCode'))\
  .selectExpr('*', 'size(brandCode) as numCount')\
  .filter(col("numCount") > 1)

val.show()

val.write\
  .format('mongodb')\
  .option("connection.uri", "mongodb+srv://phiroud:1@tracking-service.cp1dn0g.mongodb.net") \
  .option("database", "write_db")\
  .option("collection", "duplicate_coupon_campain")\
  .mode("append")\
  .save()