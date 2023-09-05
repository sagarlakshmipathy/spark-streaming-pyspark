from pyspark.sql import SparkSession
from pyspark.sql.functions import *

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

def readFromKafka():
    kafkaDF = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "rockthejvm") \
        .load()

    kafkaDF \
        .select(col("topic"), col("value").cast(StringType()).alias("actual_value")) \
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    readFromKafka()
