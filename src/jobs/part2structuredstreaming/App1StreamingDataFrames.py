from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.jobs.common.package import stocksSchema
from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Streaming Data Frames") \
    .master("local[2]") \
    .getOrCreate()

config = config_loader("/Users/sagarl/projects/spark-streaming-pyspark/src/config.json")
dataPath = config["dataPath"]


def readFromSocket():
    lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12345) \
        .load()

    shortLines = lines.where(length(col("value")) <= 5)

    print(shortLines.isStreaming)

    shortLines.writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()


def readFromFiles():
    stocksDF = spark.readStream \
        .format("csv") \
        .option("header", "false") \
        .option("dateFormat", "MMM d yyyy") \
        .schema(stocksSchema) \
        .load(f"{dataPath}/stocks")

    stocksDF.writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()


def demoTriggers():
    lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12345) \
        .load()

    # run the query every 2 seconds
    # lines.writeStream \
    #     .format("console") \
    #     .outputMode("append") \
    #     .trigger(processingTime="2 seconds") \
    #     .start() \
    #     .awaitTermination()

    # run once
    # lines.writeStream \
    #     .format("console") \
    #     .outputMode("append") \
    #     .trigger(once=True) \
    #     .start() \
    #     .awaitTermination()

    # experimental, every 2 seconds create a batch with whatever is present
    lines.writeStream \
        .format("console") \
        .outputMode("append") \
        .trigger(continuous="2 seconds") \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    # readFromSocket()
    # readFromFiles()
    demoTriggers()
