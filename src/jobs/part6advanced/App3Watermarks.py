from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from src.utils import config_loader

config = config_loader("/Users/sagarl/projects/rockthejvm-pyspark/spark-streaming-pyspark/src/config.json")
dataPath = config["dataPath"]

spark = SparkSession.builder \
    .appName("Late data with watermarks") \
    .master("local[2]") \
    .getOrCreate()


# format 40000,blue
def watermarks():
    dataDF = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12346) \
        .load()

    processedDataDF = dataDF.select(col("value").cast(StringType()).alias("line")) \
        .withColumn("tokens", split(col("line"), ",")) \
        .withColumn("created", from_unixtime(col("tokens")[0]).cast(TimestampType())) \
        .withColumn("color", col("tokens")[1])


    watermarkedDF: DataFrame = processedDataDF \
        .withWatermark("created", "2 seconds") \
        .groupBy(window(col("created"), "2 seconds"), col("color")) \
        .count() \
        .select(col("window.*"), col("color"), col("count"))

    watermarkedDF.writeStream \
        .format("console") \
        .outputMode("append") \
        .trigger(processingTime="2 seconds") \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    watermarks()