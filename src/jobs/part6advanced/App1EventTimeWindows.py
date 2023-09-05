from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

from src.utils import config_loader

config = config_loader("/spark-streaming-pyspark/src/config.json")
dataPath = config["dataPath"]

spark = SparkSession.builder \
    .appName("Event Time Windows") \
    .master("local[2]") \
    .getOrCreate()

spark.conf.set("spark.sql.session.timeZone", "UTC+02:00")

onlinePurchaseSchema = StructType(
    [
        StructField("id", StringType()),
        StructField("time", TimestampType()),
        StructField("item", StringType()),
        StructField("quantity", IntegerType())
    ]
)


def readPurchaseFromSocket():
    df = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12345) \
        .load()

    return df \
        .select(from_json(col("value"), onlinePurchaseSchema).alias("purchase")) \
        .select(col("purchase.*"))


def readFromFile():
    df = spark.readStream \
        .format("json") \
        .schema(onlinePurchaseSchema) \
        .load(f"{dataPath}/purchases")

    return df


def writePurchasesToConsole():
    purchasesDF = readPurchaseFromSocket()

    return purchasesDF.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", False) \
        .start() \
        .awaitTermination()


def aggregatePurchasesBySlidingWindow():
    purchasesDF = readPurchaseFromSocket()

    purchasesBySlidingWindow = purchasesDF \
        .groupBy(window(col("time"), "1 day", "1 hour").alias("time")) \
        .agg(sum(col("quantity")).alias("total_quantity")) \
        .orderBy(col("time.start").asc_nulls_last())

    return purchasesBySlidingWindow.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("truncate", False) \
        .start() \
        .awaitTermination()


def salesByDay():
    purchasesDF = readFromFile()

    salesByDay = purchasesDF \
        .groupBy(window(col("time"), "1 day").alias("day"), col("item")) \
        .agg(sum(col("quantity")).alias("quantity_sold")) \
        .orderBy(col("day.start"), col("quantity_sold").desc_nulls_last())

    return salesByDay.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("truncate", False) \
        .start() \
        .awaitTermination()


def salesBy24Hours():
    purchasesDF = readFromFile()

    salesBy24HoursWindow = purchasesDF \
        .groupBy(
            window(col("time"), "1 day", "1 hour").alias("time"),
            col("item")) \
        .agg(sum(col("quantity")).alias("total_quantity")) \
        .orderBy(col("time.start"), col("total_quantity").desc_nulls_last())

    return salesBy24HoursWindow.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("truncate", False) \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    salesBy24Hours()
