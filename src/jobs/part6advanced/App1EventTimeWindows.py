from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

from src.utils import config_loader

config = config_loader("/Users/sagarl/projects/rockthejvm-pyspark/spark-streaming-pyspark/src/config.json")
data_path = config["dataPath"]

spark = SparkSession.builder \
    .appName("Event Time Windows") \
    .master("local[2]") \
    .getOrCreate()

spark.conf.set("spark.sql.session.timeZone", "UTC+02:00")

online_purchase_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("time", TimestampType()),
        StructField("item", StringType()),
        StructField("quantity", IntegerType())
    ]
)


def read_purchase_from_socket():
    df = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12345) \
        .load()

    return df \
        .select(from_json(col("value"), online_purchase_schema).alias("purchase")) \
        .select(col("purchase.*"))


def read_from_file():
    df = spark.readStream \
        .format("json") \
        .schema(online_purchase_schema) \
        .load(f"{data_path}/purchases")

    return df


def write_purchases_to_console():
    purchases_df = read_purchase_from_socket()

    purchases_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", False) \
        .start() \
        .awaitTermination()


def aggregate_purchases_by_sliding_window():
    purchases_df = read_purchase_from_socket()

    purchases_by_sliding_window = purchases_df \
        .groupBy(window(col("time"), "1 day", "1 hour").alias("time")) \
        .agg(sum(col("quantity")).alias("total_quantity")) \
        .orderBy(col("time.start").asc_nulls_last())

    purchases_by_sliding_window.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("truncate", False) \
        .start() \
        .awaitTermination()


def sales_by_day():
    purchases_df = read_from_file()

    sales_by_day_df = purchases_df \
        .groupBy(window(col("time"), "1 day").alias("day"), col("item")) \
        .agg(sum(col("quantity")).alias("quantity_sold")) \
        .orderBy(col("day.start"), col("quantity_sold").desc_nulls_last())

    sales_by_day_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("truncate", False) \
        .start() \
        .awaitTermination()


def sales_by_24_hours():
    purchases_df = read_from_file()

    sales_by_24_hours_window = purchases_df \
        .groupBy(
            window(col("time"), "1 day", "1 hour").alias("time"),
            col("item")) \
        .agg(sum(col("quantity")).alias("total_quantity")) \
        .orderBy(col("time.start"), col("total_quantity").desc_nulls_last())

    sales_by_24_hours_window.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("truncate", False) \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    sales_by_24_hours()
