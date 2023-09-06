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


def streamingCount():
    lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12345) \
        .load()

    lineCount = lines.select(count(col("*")).alias("lineCount"))

    # append output mode can only be used with watermarks
    lineCount.writeStream \
        .format("console") \
        .outputMode("complete") \
        .start() \
        .awaitTermination()


def numericalAggregations(aggFunction):
    lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12345) \
        .load()

    numbers = lines.select(col("value").cast(IntegerType()).alias("number"))
    aggreationDF = numbers.select(aggFunction(col("number")).alias("agg_so_far"))

    aggreationDF.writeStream \
        .format("console") \
        .outputMode("complete") \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    # streamingCount()
    numericalAggregations(sum)
