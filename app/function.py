from pyspark.sql import SparkSession
from pyspark.sql.functions import col,date_format

import mysql.connector
from mysql.connector import errorcode

def init_spark():
  sql = SparkSession \
    .builder\
    .appName("titanic-app")\
    .config("spark.jars", "/opt/spark-apps/mysql-connector-java_8.0.28-1ubuntu20.04_all.deb")\
    .getOrCreate()
  sc = sql.sparkContext
  return sql


spark = init_spark()
df = spark.read.csv("/opt/spark-data/titanic.csv",header=True,sep=",")
print(df.collect())