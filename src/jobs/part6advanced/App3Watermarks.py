from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.utils import config_loader

config = config_loader("/spark-streaming-pyspark/src/config.json")
dataPath = config["dataPath"]

spark = SparkSession.builder \
    .appName("Late data with watermarks") \
    .master("local[2]") \
    .getOrCreate()


# format 3000, blue
def testWatermarks():
    dataDF = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12345) \
        .load()

    stringDataDF = dataDF.select(col("value").cast(StringType()).alias("line"))

    def process_row(row):
        tokens = row.split(",")
        timestamp = unix_timestamp(tokens[0])
        data = tokens[1]
        return timestamp, data

    processedDF = stringDataDF.rdd.map(process_row()).toDF(["created", "color"])

    watermarkDF = processedDF \
        .withWatermark("created", "2 seconds") \
        .groupBy(window(col("created"), "2 seconds"), col("color")) \
        .count() \
        .select(col("window.*"), col("color"), col("count"))

    return watermarkDF


if __name__ == '__main__':
    pass