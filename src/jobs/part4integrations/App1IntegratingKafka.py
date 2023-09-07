from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.jobs.common.package import cars_schema
from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Integrating Kafka") \
    .master("local[2]") \
    .config("spark.jars", "/Users/sagarl/dependencies/pyspark/spark-sql-kafka-0-10_2.12-3.3.1.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
    .getOrCreate()


config = config_loader("/Users/sagarl/projects/spark-streaming-pyspark/src/config.json")
data_path = config["dataPath"]
checkpoint_path = config["checkpointPath"]


def read_from_kafka():
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "rockthejvm") \
        .load()

    kafka_df \
        .select(col("topic"), col("value").cast(StringType()).alias("actual_value")) \
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()


def write_to_kafka():
    cars_df = spark.readStream \
        .format("json") \
        .schema(cars_schema) \
        .load(f"{data_path}/cars")

    cars_kafka_df = cars_df.select(
        upper(col("Name")).alias("key"),
        col("Name").alias("value")
        )

    cars_kafka_df.writeStream \
        .format("kafka") \
        .outputMode("append") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "rockthejvm") \
        .option("checkpointLocation", checkpoint_path) \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    # read_from_kafka()
    write_to_kafka()
