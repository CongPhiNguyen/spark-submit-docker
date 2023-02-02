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

spark.sparkContext.setLogLevel("WARN")

def upsertDataFrame(oldDataFrame, newDataFrame): 
    newDataFrameTransform = newDataFrame.withColumnRenamed("d_id", "new_d_id").withColumnRenamed("id", "new_id")
    joinEd = oldDataFrame\
        .join(newDataFrameTransform , oldDataFrame.id == newDataFrameTransform.new_id, "full")\
        .withColumn("d_id_new", when(col("new_d_id").isNull(), col("d_id")).otherwise(col("new_d_id")))\
        .select("id", col("d_id_new").alias("d_id"))
    return joinEd

testDFOld = spark.createDataFrame([(1,"111"), (2,"222"), (3,"333"), (4,"444"), (5,"555")], ["id", "d_id"])
testDFNew = spark.createDataFrame([(1,"AAA"), (2,"BBB")], ["id", "d_id"])

result = upsertDataFrame(testDFOld, testDFNew)
testDFOld.show()
testDFNew.show()
result.show()