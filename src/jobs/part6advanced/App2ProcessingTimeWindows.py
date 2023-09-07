from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Processing Time Windows") \
    .master("local[2]") \
    .getOrCreate()

config = config_loader("/spark-streaming-pyspark/src/config.json")
dataPath = config["dataPath"]


def aggregate_by_processing_time():
    df = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12345) \
        .load()

    lines_char_count_by_window_df = df \
        .select(col("value"), current_timestamp().alias("processing_time")) \
        .groupBy(window(col("processing_time"), "10 seconds").alias("window")) \
        .agg(sum(length(col("value"))).alias("char_count")) \
        .select(
            col("window.start").alias("start"),
            col("window.end").alias("end"),
            col("char_count")) \
        .orderBy(col("start"))

    return lines_char_count_by_window_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("truncate", False) \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    aggregate_by_processing_time()
