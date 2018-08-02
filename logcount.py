import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


if __name__ == "__main__":
    mySchema = StructType()\
        .add("datetime", StringType())\
        .add("source", StringType())\
        .add("type", StringType())\
        .add("log", StringType())
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 pyspark-shell'
    spark = SparkSession\
        .builder\
        .appName("PythonStreamingConsumerKafkaLogCount")\
        .getOrCreate()
    spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "mslave2.admintome.lab:31000")\
        .option("subscribe", "www_logs")\
        .load()\
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")\
        .select(from_json(col("value"), mySchema).getField("log").alias("log"))\
        .select(from_json(col("log"), mySchema).alias("tmp")).select("tmp.*")\
        .writeStream\
        .format("console")\
        .start()\
        .awaitTermination()
