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


def debug_query(query: StreamingQuery):
    import time
    import threading

    def _debug_thread():
        for i in range(1, 101):
            time.sleep(1)
            query_event_time = "[]" if query.lastProgress is None else str(query.lastProgress["eventTime"])
            print(f"{i}: {query_event_time}")

    thread = threading.Thread(target=_debug_thread)
    thread.start()


# format 40000,blue
def watermarks():
    data_df = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12347) \
        .load()

    processed_data_df = data_df.select(col("value").cast(StringType()).alias("line")) \
        .withColumn("tokens", split(col("line"), ",")) \
        .withColumn("created", from_unixtime(col("tokens")[0]).cast(TimestampType())) \
        .withColumn("color", col("tokens")[1])

    watermarked_df = processed_data_df \
        .withWatermark("created", "2 seconds") \
        .groupBy(window(col("created"), "2 seconds"), col("color")) \
        .count() \
        .select(col("window.*"), col("color"), col("count"))

    query = watermarked_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .trigger(processingTime="2 seconds") \
        .start()

    debug_query(query)
    query.awaitTermination()


if __name__ == '__main__':
    watermarks()
