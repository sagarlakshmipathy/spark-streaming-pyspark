from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.jobs.common.package import cars_schema
from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Integrating Kafka") \
    .master("local[2]") \
    .config("spark.jars",
                    "/Users/sagarl/projects/spark-essentials-pyspark/dependencies/spark-sql-kafka-0-10_2.12-3.3.1.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
    .getOrCreate()


config = config_loader("/Users/sagarl/projects/spark-streaming-pyspark/src/config.json")
dataPath = config["dataPath"]
checkpointPath = config["checkpointPath"]


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
        .load(f"{dataPath}/cars")

    cars_kafka_df = cars_df.select(
        upper(col("Name")).alias("key"),
        col("Name").alias("value")
        )

    cars_kafka_df.writeStream \
        .format("kafka") \
        .outputMode("append") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "rockthejvm") \
        .option("checkpointLocation", checkpointPath) \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    # readFromKafka()
    write_to_kafka()
