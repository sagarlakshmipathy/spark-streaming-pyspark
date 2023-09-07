from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import *

from src.utils import config_loader

config = config_loader("/Users/sagarl/projects/rockthejvm-pyspark/spark-streaming-pyspark/src/config.json")
dataPath = config["dataPath"]

spark = SparkSession.builder \
    .appName("Late data with watermarks") \
    .master("local[2]") \
    .getOrCreate()


# def debugQuery(query: StreamingQuery):
#     import time
#     import threading
#     def _debug_thread():
#         for i in range(1, 101):
#             time.sleep(1)
#
#             # Check the type of the query.lastProgress object before accessing the eventTime attribute.
#             if isinstance(query.lastProgress, dict):
#                 query_event_time = "[]"
#             else:
#                 query_event_time = query.lastProgress.eventTime.toString()
#
#             print(f"{i}: {query_event_time}")
#
#         thread = threading.Thread(target=_debug_thread)
#         thread.start()

# format 40000,blue
def watermarks():
    dataDF = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12345) \
        .load()

    processedDataDF = dataDF.select(col("value").cast(StringType()).alias("line")) \
        .withColumn("tokens", split(col("line"), ",")) \
        .withColumn("created", from_unixtime(col("tokens")[0]).cast(TimestampType())) \
        .withColumn("color", col("tokens")[1])

    watermarkedDF = processedDataDF \
        .withWatermark("created", "2 seconds") \
        .groupBy(window(col("created"), "2 seconds"), col("color")) \
        .count() \
        .select(col("window.*"), col("color"), col("count"))

    query = watermarkedDF.writeStream \
        .format("console") \
        .outputMode("append") \
        .trigger(processingTime="2 seconds") \
        .start()

    # debugQuery(query)
    query.awaitTermination()


if __name__ == '__main__':
    watermarks()
