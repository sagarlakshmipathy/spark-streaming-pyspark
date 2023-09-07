from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Streaming Aggregations") \
    .master("local[2]") \
    .getOrCreate()

config = config_loader("/Users/sagarl/projects/spark-streaming-pyspark/src/config.json")
dataPath = config["dataPath"]


def streaming_count():
    lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12345) \
        .load()

    line_count = lines.select(count(col("*")).alias("lineCount"))

    # append output mode can only be used with watermarks
    line_count.writeStream \
        .format("console") \
        .outputMode("complete") \
        .start() \
        .awaitTermination()


def numerical_aggregations(agg_function):
    lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12345) \
        .load()

    numbers = lines.select(col("value").cast(IntegerType()).alias("number"))
    aggregation_df = numbers.select(agg_function(col("number")).alias("agg_so_far"))

    aggregation_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .start() \
        .awaitTermination()


def group_names():
    lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12345) \
        .load()

    names = lines \
        .select(col("value").alias("name")) \
        .groupBy(col("name")) \
        .count()

    names.writeStream \
        .format("console") \
        .outputMode("complete") \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    # streamingCount()
    # numericalAggregations(sum)
    group_names()
